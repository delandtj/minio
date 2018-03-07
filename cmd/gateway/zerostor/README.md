# Minio Gateway for 0-stor

Minio gateway for [0-stor](https://github.com/zero-os/0-stor) using [0-db](https://github.com/zero-os/0-db)
as storage server.

## Running

TODO

## Metadata

This gateway use local filesystem as metadata, the directory of the metadata
location is specified in `MINIO_ZEROSTOR_META_DIR` environment variable.

0-stor provide metadata using `ETCD` and `Badger`, but this gateway doesn't use them because they
don't have directory-like behavior.

TODO: replication/backup of the metadata

## Bucket Management

Because 0-stor doesn't have `bucket` concept, all bucket management are handled by 
the gateway, including the bucket policy.
