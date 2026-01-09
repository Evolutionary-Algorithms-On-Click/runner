import os
from minio import Minio
from minio.error import S3Error
from app.config import MINIO_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

def download_file(run_id, file_name, extension):
    """Downloads a file from MinIO storage."""
    BUCKET_NAME = "code"
    try:
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        object_name = f"{run_id}/{file_name}.{extension}"
        download_path = f"code/{run_id}/{file_name}.{extension}"
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        minio_client.fget_object(BUCKET_NAME, object_name, download_path)
        print(f"Successfully downloaded {object_name} to {download_path}")
        return os.path.abspath(download_path)
    except S3Error as exc:
        print(f"Failed to download {object_name}: {exc}")
        raise exc
    except Exception as e:
        print(f"An unexpected error occurred during download: {e}")
        raise e


def upload_file(run_id, file_path):
    """Uploads a file to MinIO storage."""
    BUCKET_NAME = "code"
    try:
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        # Extract filename from the full path.
        file_name_with_ext = os.path.basename(file_path)
        object_name = f"{run_id}/{file_name_with_ext}"
        minio_client.fput_object(BUCKET_NAME, object_name, file_path)
        print(f"Successfully uploaded {file_path} as {object_name}")
    except S3Error as exc:
        print(f"Failed to upload {file_path}: {exc}")
        raise exc
    except Exception as e:
        print(f"An unexpected error occurred during upload: {e}")
        raise e
