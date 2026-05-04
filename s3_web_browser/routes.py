import boto3
import botocore
from flask import Flask, Response, flash, redirect, render_template, request, url_for

from s3_web_browser.s3 import list_objects, parse_responses


def delete_object_keys(
    s3_client: botocore.client.BaseClient,
    bucket_name: str,
    object_keys: list[str],
) -> tuple[int, list[dict[str, object]]]:
    deleted_count = 0
    errors: list[dict[str, object]] = []

    for index in range(0, len(object_keys), 1000):
        objects_to_delete = [{"Key": object_key} for object_key in object_keys[index : index + 1000]]
        delete_response = s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": objects_to_delete},
        )
        deleted_count += len(delete_response.get("Deleted", []))
        errors.extend(delete_response.get("Errors", []))

    return deleted_count, errors


def collect_folder_keys(
    s3_client: botocore.client.BaseClient,
    bucket_name: str,
    prefixes: list[str],
) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    object_keys: list[str] = []

    for prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            object_keys.extend(obj["Key"] for obj in page.get("Contents", []))

    return object_keys


def register_routes(app: Flask) -> None:  # noqa:C901
    def configured_bucket() -> str | None:
        bucket_name = app.config.get("AWS_BUCKET")
        if not bucket_name:
            return None

        return str(bucket_name).strip() or None

    @app.route("/", methods=["GET"])
    def index() -> str | Response:
        bucket_name = configured_bucket()
        if bucket_name:
            return redirect(url_for("view_bucket", bucket_name=bucket_name))

        s3 = boto3.resource("s3", **app.config["AWS_KWARGS"])
        all_buckets = s3.buckets.all()
        return render_template("index.html", buckets=all_buckets)

    @app.route("/buckets")
    def buckets() -> str | Response:
        bucket_name = configured_bucket()
        if bucket_name:
            return redirect(url_for("view_bucket", bucket_name=bucket_name))

        s3 = boto3.resource("s3", **app.config["AWS_KWARGS"])
        all_buckets = s3.buckets.all()
        return render_template("index.html", buckets=all_buckets)

    @app.route("/search/buckets/<bucket_name>", defaults={"path": ""})
    @app.route("/search/buckets/<bucket_name>/<path:path>")
    def search_bucket(bucket_name: str, path: str) -> str:
        page = request.args.get("page", 1, type=int)
        items_per_page = app.config["PAGE_ITEMS"]
        s3_client = boto3.client("s3", **app.config["AWS_KWARGS"])
        paginator = s3_client.get_paginator("list_objects_v2")
        all_entries = []
        all_prefixes = []

        try:
            # Collect all objects and folders
            for page_iterator in paginator.paginate(Bucket=bucket_name, Prefix=path):
                if "Contents" in page_iterator:
                    all_entries = [
                        {"Key": item["Key"], "Size": item["Size"], "LastModified": item["LastModified"]}
                        for item in page_iterator["Contents"]
                        if not item["Key"].endswith("/")
                    ]

            for page_iterator in paginator.paginate(Bucket=bucket_name, Prefix=path, Delimiter="/"):
                if "CommonPrefixes" in page_iterator:
                    all_prefixes.extend(page_iterator["CommonPrefixes"])

            # Create response structure
            response = {"Contents": all_entries, "CommonPrefixes": all_prefixes}

            search_param = request.args.get("search", "")
            contents = parse_responses([response], search_param)

            # Calculate pagination
            total_items = len(contents)
            total_pages = (total_items + items_per_page - 1) // items_per_page
            start_idx = (page - 1) * items_per_page
            end_idx = start_idx + items_per_page
            paginated_contents = contents[start_idx:end_idx]

            return render_template(
                "bucket_contents.html",
                contents=paginated_contents,
                bucket_name=bucket_name,
                path=path,
                search_param=search_param,
                current_page=page,
                total_pages=total_pages,
            )

        except botocore.exceptions.ClientError as e:
            match e.response["Error"]["Code"]:
                case "AccessDenied":
                    return render_template(
                        "error.html",
                        error="You do not have permission to access this bucket.",
                    )
                case "NoSuchBucket":
                    return render_template("error.html", error="The specified bucket does not exist.")
                case _:
                    return render_template("error.html", error=f"An unknown error occurred: {e}")

    @app.route("/buckets/<bucket_name>", defaults={"path": ""})
    @app.route("/buckets/<bucket_name>/<path:path>")
    def view_bucket(bucket_name: str, path: str) -> str:
        page = request.args.get("page", 1, type=int)
        items_per_page = 500

        s3_client = boto3.client("s3", **app.config["AWS_KWARGS"])

        # Get total objects count for current prefix level only
        paginator = s3_client.get_paginator("list_objects_v2")
        total_objects = 0
        for page_iterator in paginator.paginate(Bucket=bucket_name, Prefix=path, Delimiter="/"):
            # Count folders (CommonPrefixes)
            if "CommonPrefixes" in page_iterator:
                total_objects += len(page_iterator["CommonPrefixes"])
            # Count files (Contents) but exclude folder markers
            if "Contents" in page_iterator:
                total_objects += sum(1 for obj in page_iterator["Contents"] if not obj["Key"].endswith("/"))

        total_pages = (total_objects + items_per_page - 1) // items_per_page

        try:
            # Calculate continuation token for the requested page
            continuation_token = None
            if page > 1:
                temp_response = None
                for _ in range(page - 1):
                    temp_response = list_objects(
                        s3_client, bucket_name, path, app.config["PAGE_ITEMS"], "/", continuation_token
                    )
                    if not temp_response.get("IsTruncated"):
                        break
                    continuation_token = temp_response.get("NextContinuationToken")

            # Get the current page contents
            response = list_objects(s3_client, bucket_name, path, app.config["PAGE_ITEMS"], "/", continuation_token)
            contents = parse_responses([response], request.args.get("search", ""))

            return render_template(
                "bucket_contents.html",
                contents=contents,
                bucket_name=bucket_name,
                path=path,
                search_param=request.args.get("search", ""),
                current_page=page,
                total_pages=total_pages,
            )
        except botocore.exceptions.ClientError as e:
            match e.response["Error"]["Code"]:
                case "AccessDenied":
                    return render_template(
                        "error.html",
                        error="You do not have permission to access this bucket.",
                    )
                case "NoSuchBucket":
                    return render_template("error.html", error="The specified bucket does not exist.")
                case _:
                    return render_template("error.html", error=f"An unknown error occurred: {e}")

    @app.route("/download/buckets/<bucket_name>/<path:path>")
    def download_file(bucket_name: str, path: str) -> Response:
        s3_client = boto3.client("s3", **app.config["AWS_KWARGS"])
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": path},
            ExpiresIn=3600,
        )  # URL expires in 1 hour
        return redirect(url)

    @app.route("/buckets/<bucket_name>/delete-file", methods=["POST"])
    def delete_file(bucket_name: str) -> Response:
        object_key = request.form.get("key", "").strip()
        current_path = request.form.get("current_path", "")

        if not object_key:
            flash("File key is missing.", "error")
            return redirect(url_for("view_bucket", bucket_name=bucket_name, path=current_path))

        s3_client = boto3.client("s3", **app.config["AWS_KWARGS"])
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=object_key)
            flash(f"File deleted: {object_key}", "success")
            return redirect(url_for("view_bucket", bucket_name=bucket_name, path=current_path))
        except botocore.exceptions.ClientError as e:
            match e.response["Error"]["Code"]:
                case "AccessDenied":
                    flash("You do not have permission to delete this file.", "error")
                case "NoSuchBucket":
                    flash("The specified bucket does not exist.", "error")
                case _:
                    flash(f"An unknown error occurred: {e}", "error")

            return redirect(url_for("view_bucket", bucket_name=bucket_name, path=current_path))

    @app.route("/buckets/<bucket_name>/delete-selected", methods=["POST"])
    def delete_selected(bucket_name: str) -> Response:
        file_keys = [key.strip() for key in request.form.getlist("file_keys") if key.strip()]
        folder_prefixes = [prefix.strip() for prefix in request.form.getlist("folder_prefixes") if prefix.strip()]
        current_path = request.form.get("current_path", "")

        if not file_keys and not folder_prefixes:
            flash("Select at least one item to delete.", "info")
            return redirect(url_for("view_bucket", bucket_name=bucket_name, path=current_path))

        s3_client = boto3.client("s3", **app.config["AWS_KWARGS"])

        try:
            folder_keys = collect_folder_keys(s3_client, bucket_name, folder_prefixes)
            object_keys = list(dict.fromkeys([*file_keys, *folder_keys]))

            if not object_keys:
                flash("No objects found for the selected item(s).", "info")
                return redirect(url_for("view_bucket", bucket_name=bucket_name, path=current_path))

            deleted_count, errors = delete_object_keys(s3_client, bucket_name, object_keys)

            if errors:
                flash(
                    f"Delete completed with {len(errors)} error(s); deleted {deleted_count} object(s).",
                    "error",
                )
            else:
                selected_count = len(file_keys) + len(folder_prefixes)
                flash(f"Deleted {selected_count} selected item(s) ({deleted_count} object(s)).", "success")

            return redirect(url_for("view_bucket", bucket_name=bucket_name, path=current_path))
        except botocore.exceptions.ClientError as e:
            match e.response["Error"]["Code"]:
                case "AccessDenied":
                    flash("You do not have permission to delete the selected item(s).", "error")
                case "NoSuchBucket":
                    flash("The specified bucket does not exist.", "error")
                case _:
                    flash(f"An unknown error occurred: {e}", "error")

            return redirect(url_for("view_bucket", bucket_name=bucket_name, path=current_path))

    @app.route("/buckets/<bucket_name>/delete-folder", methods=["POST"])
    def delete_folder(bucket_name: str) -> Response:
        prefix = request.form.get("prefix", "").strip()
        current_path = request.form.get("current_path", "")

        if not prefix:
            flash("Folder prefix is missing.", "error")
            return redirect(url_for("view_bucket", bucket_name=bucket_name, path=current_path))

        s3_client = boto3.client("s3", **app.config["AWS_KWARGS"])

        try:
            object_keys = collect_folder_keys(s3_client, bucket_name, [prefix])
            deleted_count, errors = delete_object_keys(s3_client, bucket_name, object_keys)

            if errors:
                flash(
                    f"Folder delete completed with {len(errors)} error(s); deleted {deleted_count} object(s).",
                    "error",
                )
            elif deleted_count == 0:
                flash(f"No objects found under folder: {prefix}", "info")
            else:
                flash(f"Folder deleted: {prefix} ({deleted_count} object(s))", "success")

            return redirect(url_for("view_bucket", bucket_name=bucket_name, path=current_path))
        except botocore.exceptions.ClientError as e:
            match e.response["Error"]["Code"]:
                case "AccessDenied":
                    flash("You do not have permission to delete this folder.", "error")
                case "NoSuchBucket":
                    flash("The specified bucket does not exist.", "error")
                case _:
                    flash(f"An unknown error occurred: {e}", "error")

            return redirect(url_for("view_bucket", bucket_name=bucket_name, path=current_path))
