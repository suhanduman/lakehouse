"""S3Service: thin wrapper over a boto3 S3 client (MinIO-compatible). No
live S3/MinIO calls — every test injects a hand-rolled fake client recording
the calls it received, so the wrapper's argument-shaping and
idempotent-create behaviour is exercised without a real bucket store.
"""

from __future__ import annotations

import pytest
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
        self.log = []  # shared ordered log: ("empty", name) / ("delbucket", name)

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
        if not keys:
            return {"Contents": [], "IsTruncated": False}
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def delete_objects(self, Bucket, Delete):
        self.deleted_objects_calls.append((Bucket, Delete))
        self.log.append(("empty", Bucket))
        return {"Deleted": Delete["Objects"]}

    def delete_bucket(self, Bucket):
        self.log.append(("delbucket", Bucket))


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


# --------------------------------------------------------------------------
# delete_bucket
# --------------------------------------------------------------------------

def test_delete_bucket_empties_then_removes():
    fake = FakeS3()
    svc = S3Service(fake)
    fake.objects["b1"] = ["k"]
    svc.delete_bucket("b1")
    assert fake.log == [("empty", "b1"), ("delbucket", "b1")]


def test_delete_bucket_missing_is_noop():
    class Missing:
        def list_objects_v2(self, **k):
            return {"Contents": [], "IsTruncated": False}

        def delete_bucket(self, Bucket):
            raise ClientError({"Error": {"Code": "NoSuchBucket"}}, "DeleteBucket")

    S3Service(Missing()).delete_bucket("gone")  # must not raise


# --------------------------------------------------------------------------
# object_count — paginated, bounded best-effort count. Pipeline Map + catalog
# enrichment (2026-08-02 spike), Task 1.
# --------------------------------------------------------------------------

def test_object_count_sums_pages():
    fake = FakeS3()
    fake.objects["b1"] = ["k1", "k2", "k3"]
    assert S3Service(fake).object_count("b1") == {"count": 3, "capped": False}


def test_object_count_missing_bucket_is_zero():
    class Missing:
        def list_objects_v2(self, **k):
            raise ClientError({"Error": {"Code": "NoSuchBucket"}}, "ListObjectsV2")

    assert S3Service(Missing()).object_count("gone") == {"count": 0, "capped": False}


def test_object_count_stops_at_cap():
    class ManyPages:
        """Two pages of 3 objects each — cap of 4 should stop mid-second-page
        and report capped=True with count clamped to the cap."""

        def __init__(self):
            self.calls = 0

        def list_objects_v2(self, Bucket, ContinuationToken=None):
            self.calls += 1
            if self.calls == 1:
                return {"KeyCount": 3, "IsTruncated": True, "NextContinuationToken": "tok"}
            return {"KeyCount": 3, "IsTruncated": False}

    assert S3Service(ManyPages()).object_count("big", cap=4) == {"count": 4, "capped": True}


def test_object_count_reraises_other_client_errors():
    class Broken:
        def list_objects_v2(self, **k):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectsV2")

    with pytest.raises(ClientError):
        S3Service(Broken()).object_count("nope")
