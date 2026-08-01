"""S3Service: thin wrapper over a boto3 S3 client (MinIO-compatible — see
`app.config.Settings.s3_endpoint`).

No AWS/network calls of its own — the constructor takes an already-built
boto3 `client("s3", endpoint_url=..., ...)` (or a hand-rolled fake in
tests), so the whole thing is unit-testable without a live bucket store
(see tests/test_s3_service.py).
"""

from __future__ import annotations

from typing import Any, List

from botocore.exceptions import ClientError

# Error codes boto3 raises when the bucket already exists and is already
# owned by the caller — treated as success since create_bucket is meant to
# be idempotent (matching K8sService.apply_* 409-tolerant semantics).
_ALREADY_OWNED_CODES = {"BucketAlreadyOwnedByYou"}

# Error codes boto3 raises when the bucket does not exist — treated as
# success since delete_bucket is meant to be idempotent (per-pipeline
# teardown, Sub-project B).
_NO_SUCH_BUCKET_CODES = {"NoSuchBucket", "404"}


class S3Service:
    """Bucket lifecycle wrapper. `client` is an injected boto3 S3 client
    instance (or hand-rolled fake in tests) — see class docstring."""

    def __init__(self, client: Any) -> None:
        self.client = client

    # ----------------------------------------------------------------
    # create_bucket — idempotent: already-owned-by-you counts as success
    # ----------------------------------------------------------------

    def create_bucket(self, name: str) -> bool:
        try:
            self.client.create_bucket(Bucket=name)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in _ALREADY_OWNED_CODES:
                raise
        return True

    # ----------------------------------------------------------------
    # list_buckets
    # ----------------------------------------------------------------

    def list_buckets(self) -> List[str]:
        response = self.client.list_buckets()
        return [bucket["Name"] for bucket in response.get("Buckets", [])]

    # ----------------------------------------------------------------
    # empty_bucket — delete every object (paginated), so the bucket can
    # then be deleted (S3/MinIO refuse to delete a non-empty bucket).
    # ----------------------------------------------------------------

    def empty_bucket(self, name: str) -> None:
        continuation_token = None
        while True:
            kwargs = {"Bucket": name}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = self.client.list_objects_v2(**kwargs)
            contents = response.get("Contents", [])
            if contents:
                self.client.delete_objects(
                    Bucket=name,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in contents]},
                )
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")

    # ----------------------------------------------------------------
    # delete_bucket — empty then remove the bucket itself. Idempotent:
    # a missing bucket is a no-op (per-pipeline teardown, Sub-project B).
    # ----------------------------------------------------------------

    def delete_bucket(self, name: str) -> None:
        try:
            self.empty_bucket(name)
            self.client.delete_bucket(Bucket=name)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in _NO_SUCH_BUCKET_CODES:
                raise
