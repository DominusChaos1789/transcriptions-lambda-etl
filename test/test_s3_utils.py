import src.s3_utils as s3_utils

BUCKET = "augusta-nexa-dev-providers-landing"
PREFIX = "external/datanexa/transacciones/empatia/transcripciones/BDO"


def test_list_json_keys_filters_to_json_only(aws, seeded_source_files):
    s3 = aws["s3"]
    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/notes.txt", Body=b"not json")

    keys = s3_utils.list_json_keys(s3, BUCKET, PREFIX)

    assert sorted(keys) == sorted(seeded_source_files)
    assert all(k.endswith(".json") for k in keys)


def test_read_json_roundtrip(aws):
    s3 = aws["s3"]
    s3_utils.write_json(s3, BUCKET, "some/key.json", {"a": 1, "b": "x"})

    result = s3_utils.read_json(s3, BUCKET, "some/key.json")

    assert result == {"a": 1, "b": "x"}


def test_delete_objects_removes_all_given_keys(aws, seeded_source_files):
    s3 = aws["s3"]

    deleted = s3_utils.delete_objects(s3, BUCKET, seeded_source_files)

    assert sorted(deleted) == sorted(seeded_source_files)
    remaining = s3_utils.list_json_keys(s3, BUCKET, PREFIX)
    assert remaining == []
