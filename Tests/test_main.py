import pytest
import main



#TODO: write tests for some of the functions in main.py


def test_parse_s3_uri():
    s3_uri = "s3a://my-bucket/path/to/object.json"
    bucket, key = main.parse_s3_uri(s3_uri)
    assert bucket == "my-bucket"
    assert key == "path/to/object.json"
    with pytest.raises(ValueError):
        main.parse_s3_uri("invalid-uri")



