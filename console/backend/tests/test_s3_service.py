"""S3Service: thin wrapper over a boto3 S3 client (MinIO-compatible). No
live S3/MinIO calls — every test injects a hand-rolled fake client recording
the calls it received, so the wrapper's argument-shaping and
idempotent-create behaviour is exercised without a real bucket store.
"""

from __future__ import annotations

from botocore.exceptions import ClientError

from app.services.s3_service import S3Service


# --------------------------------------------------------------------------
# Fake boto3 S3 client
# --------------------------------------------------------------------------

class FakeS3:
    def __init__(self, already_owned: bool = False):
        self.already_owned = already_owned
        self.made = []
        self.deleted_objects_calls = []
        self.buckets = ["src-existing"]
        self.objects = {}  # bucket -> list of keys
        self.list_objects_calls = []

    def create_bucket(self, Bucket):
        if self.already_owned:
            raise ClientError(
                {"Error": {"Code": "BucketAlreadyOwnedByYou", "Message": "mine"}},
                "CreateBucket",
            )
        self.made.append(Bucket)
        self.buckets.append(Bucket)
        return {"Location": f"/{Bucket}"}

    def list_buckets(self):
        return {"Buckets": [{"Name": name} for name in self.buckets]}

    def list_objects_v2(self, Bucket, ContinuationToken=None):
        self.list_objects_calls.append(Bucket)
        keys = self.objects.get(Bucket, [])
        return {"Contents": [{"Key": k} for k in keys]} if keys else {}

    def delete_objects(self, Bucket, Delete):
        self.deleted_objects_calls.append((Bucket, Delete))
        return {"Deleted": Delete["Objects"]}


# --------------------------------------------------------------------------
# create_bucket
# --------------------------------------------------------------------------

def test_create_bucket():
    s = S3Service(FakeS3())
    assert s.create_bucket("src-x") is True


def test_create_bucket_calls_client_with_name():
    fake = FakeS3()
    s = S3Service(fake)
    s.create_bucket("src-mssql1")
    assert fake.made == ["src-mssql1"]


def test_create_bucket_already_owned_is_idempotent_true():
    fake = FakeS3(already_owned=True)
    s = S3Service(fake)
    assert s.create_bucket("src-existing") is True


# --------------------------------------------------------------------------
# list_buckets
# --------------------------------------------------------------------------

def test_list_buckets_returns_names():
    fake = FakeS3()
    fake.buckets = ["src-a", "src-b"]
    s = S3Service(fake)
    assert s.list_buckets() == ["src-a", "src-b"]


def test_list_buckets_empty():
    fake = FakeS3()
    fake.buckets = []
    s = S3Service(fake)
    assert s.list_buckets() == []


# --------------------------------------------------------------------------
# empty_bucket
# --------------------------------------------------------------------------

def test_empty_bucket_deletes_all_objects():
    fake = FakeS3()
    fake.objects["src-x"] = ["warehouse/a.parquet", "warehouse/b.parquet"]
    s = S3Service(fake)
    s.empty_bucket("src-x")

    assert len(fake.deleted_objects_calls) == 1
    bucket, delete_body = fake.deleted_objects_calls[0]
    assert bucket == "src-x"
    assert delete_body == {
        "Objects": [
            {"Key": "warehouse/a.parquet"},
            {"Key": "warehouse/b.parquet"},
        ]
    }


def test_empty_bucket_noop_when_already_empty():
    fake = FakeS3()
    s = S3Service(fake)
    s.empty_bucket("src-x")

    assert fake.deleted_objects_calls == []
    assert len(fake.list_objects_calls) == 1
