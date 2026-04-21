# Storage repository for MinIO object storage

from __future__ import annotations

import io
import base64
import logging
from datetime import timedelta
from typing import Optional

from minio import Minio
from minio.error import S3Error

from app.core.config import get_settings
from app.core.exceptions import StorageError

logger = logging.getLogger(__name__)


class StorageRepository:
    """Data access layer for MinIO object storage operations."""

    def __init__(self) -> None:
        settings = get_settings()
        self._client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        self._bucket_images = settings.MINIO_BUCKET_IMAGES
        self._bucket_models = settings.MINIO_BUCKET_MODELS
        self._public_endpoint = settings.MINIO_PUBLIC_ENDPOINT

    # ── Initialize buckets ───────────────────────────────────
    def ensure_buckets(self) -> None:
        for bucket in (self._bucket_images, self._bucket_models):
            try:
                if not self._client.bucket_exists(bucket):
                    self._client.make_bucket(bucket)
                    logger.info("Created bucket: %s", bucket)
            except S3Error as exc:
                raise StorageError(f"Failed to create bucket '{bucket}': {exc}") from exc

    # ── Upload image (flat / data lake) ────────────────────────
    def upload_image(
        self,
        filename: str,
        image_base64: str,
        content_type: str = "image/png",
    ) -> str:
        """
        Upload base64-encoded fingerprint image to MinIO (flat, data lake).
        Images are stored directly in the bucket root: fingerprint-images/{filename}
        Returns the object name.
        """
        try:
            image_bytes = base64.b64decode(image_base64)
            data = io.BytesIO(image_bytes)
            self._client.put_object(
                bucket_name=self._bucket_images,
                object_name=filename,
                data=data,
                length=len(image_bytes),
                content_type=content_type,
            )
            logger.info("Uploaded image: %s/%s", self._bucket_images, filename)
            return filename
        except Exception as exc:
            raise StorageError(f"Image upload failed: {exc}") from exc

    def _get_presign_client(self) -> Minio:
        if self._public_endpoint:
            return Minio(
                endpoint=self._public_endpoint,
                access_key=get_settings().MINIO_ACCESS_KEY,
                secret_key=get_settings().MINIO_SECRET_KEY,
                secure=get_settings().MINIO_SECURE,
            )
        return self._client

    # ── Presigned URL ────────────────────────────────────────
    def get_presigned_url(
        self,
        object_name: str,
        bucket: Optional[str] = None,
        expires: timedelta = timedelta(hours=1),
    ) -> str:
        """Generate presigned URL for worker to download image."""
        try:
            url = self._get_presign_client().presigned_get_object(
                bucket_name=bucket or self._bucket_images,
                object_name=object_name,
                expires=expires,
            )
            return url
        except S3Error as exc:
            raise StorageError(f"Failed to create presigned URL: {exc}") from exc

    def get_presigned_put_url(
        self,
        object_name: str,
        bucket: Optional[str] = None,
        expires: timedelta = timedelta(hours=1),
    ) -> str:
        """Generate a presigned PUT URL for worker-side image upload."""
        try:
            return self._get_presign_client().presigned_put_object(
                bucket_name=bucket or self._bucket_images,
                object_name=object_name,
                expires=expires,
            )
        except S3Error as exc:
            raise StorageError(f"Failed to create presigned upload URL: {exc}") from exc

    # ── Model management ─────────────────────────────────────
    # Path convention: {type}/{name}_v{version}/model.onnx
    #   e.g. embedding/embedding_v1/model.onnx
    #        matching/matching_v1/model.onnx
    #        pad/pad_v1/model.onnx

    def get_model_url(
        self,
        s3_path: str,
        expires: timedelta = timedelta(hours=2),
    ) -> str:
        """Generate presigned download URL for a model file by its s3_path."""
        return self.get_presigned_url(
            object_name=s3_path,
            bucket=self._bucket_models,
            expires=expires,
        )

    def list_models(self, model_type: Optional[str] = None) -> list[str]:
        """
        List available models on MinIO.
        If model_type is provided (e.g. 'embedding'), only list models under that prefix.
        """
        try:
            prefix = f"{model_type}/" if model_type else None
            objects = self._client.list_objects(
                self._bucket_models, prefix=prefix, recursive=True
            )
            return [obj.object_name for obj in objects]
        except S3Error as exc:
            raise StorageError(f"Failed to list models: {exc}") from exc


# ── Singleton ────────────────────────────────────────────────
_repo: Optional[StorageRepository] = None


def get_storage_repo() -> StorageRepository:
    global _repo
    if _repo is None:
        _repo = StorageRepository()
    return _repo
