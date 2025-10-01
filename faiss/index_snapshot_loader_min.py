# FAISS index snapshot loader (container entrypoint)
import os
import json
import faiss
import boto3
import sys


def load_index_from_s3(s3uri, local_path='/tmp/index.faiss'):
    """
    Download a FAISS index file from S3 to local storage.

    Args:
        s3uri (str): S3 URI of the FAISS index file.
        local_path (str): Local path to save the downloaded index.

    Returns:
        str: Path to the downloaded local index file.
    """
    s3 = boto3.client('s3')
    parts = s3uri.replace('s3://', '').split('/', 1)
    bucket = parts[0]
    key = parts[1]

    obj = s3.get_object(Bucket=bucket, Key=key)
    with open(local_path, 'wb') as f:
        f.write(obj['Body'].read())

    return local_path


def main():
    """
    Entrypoint to load the FAISS index from S3 and read it using faiss.
    """
    s3uri = os.environ.get('INDEX_S3_URI')
    if not s3uri:
        print(json.dumps({'status': 'error', 'message': 'INDEX_S3_URI environment variable not set'}))
        sys.exit(1)

    idxpath = load_index_from_s3(s3uri)
    idx = faiss.read_index(idxpath)
    print(json.dumps({'status': 'loaded', 'index_path': idxpath}))


if __name__ == '__main__':
    main()
