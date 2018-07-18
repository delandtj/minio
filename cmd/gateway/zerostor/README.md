# Minio Gateway for 0-stor

Minio gateway for [0-stor](https://github.com/zero-os/0-stor) using [0-db](https://github.com/zero-os/0-db)
as storage server.

## Metadata

This gateway has two kind of metadata : mongodb and local filesystem.

When using local filesystem as metadata, the directory of the metadata
location is specified in `MINIO_ZEROSTOR_META_DIR` environment variable.

0-stor provide metadata using `ETCD` and `Badger`, but this gateway doesn't use them because they
don't have directory-like behavior.

Metadata will be encrypted with AES encryption if user provide private key through `MINIO_ZEROSTOR_META_PRIVKEY` environment variable.
What specific AES algorithm is used, depends on the given key size:
- 16 bytes: AES_128
- 24 bytes: AES_192
- 32 bytes: AES_256


## Bucket Management

Because 0-stor doesn't have `bucket` concept, all bucket management are handled by 
the gateway, including the bucket policy.

## Config Reload

This gateway configuration could be reloaded by sending `SIGHUP` to the process.

## Running

### Build

`make`

### Check options

`minio gateway zerostor -h`

### 0-stor Gateway Configuration

Other than 0-stor client config, all configuration are done through environment variables.
We use environment variables to make it consistent with other Minio gateways.

Standard minio environment variables:
- `MINIO_ACCESS_KEY` : Your minio Access key 
- `MINIO_SECRET_KEY` : Your minio Secret key 

0-stor gateway environment variables:
- `MINIO_ZEROSTOR_CONFIG_FILE` :  Zerostor config file(default : $MINIO_CONFIG_DIR/zerostor.yaml). Default minio config dir is $HOME/.minio
- `MINIO_ZEROSTOR_META_DIR`     Zerostor metadata directory(default : $MINIO_CONFIG_DIR/zerostor_meta)
- `MINIO_ZEROSTOR_META_PRIVKEY` Zerostor metadata private key(default : ""). Metadata won't be encrypted if the key is not provided
- `MINIO_ZEROSTOR_DEBUG`        Zerostor debug flag. Set to "1" to enable debugging (default : 0)

Put your 0-stor config file as specified by `MINIO_ZEROSTOR_CONFIG_FILE`.

### Run 0-db server


```
./bin/zdb --mode direct --port 12345
```

### example configuration

```yaml
namespace: thedisk
password: kunci
datastor: # required
  # the address(es) of a zstordb cluster (required0)
  shards: # required
    - 127.0.0.1:12345
  pipeline:
    block_size: 4096
    compression: # optional, snappy by default
      type: snappy # snappy is the default, other options: lz4, gzip
      mode: default # default is the default, other options: best_speed, best_compression
    encryption: # optional, disabled by default
      type: aes # aes is the default and only standard option
      private_key: ab345678901234567890123456789012
```
Above configuration will use local filesystem as metadata.
To use `mongodb` as metadata, append these config to the end of the above config.
```yaml
minio:
  zerostor_meta:
    type: mongo
    mongo:
      url: localhost:27017
      database: zstor_meta
  multipart_meta:
    type: mongo
    mongo:
      url: localhost:27017
      database: multipart_meta
```
Please adjust the `url` and `database` to your environment.

### Run the gateway

`./minio gateway zerostor`

## Simple test using minio client


### install minio client

Follow this guide https://docs.minio.io/docs/minio-client-complete-guide for the guide

### Configure minio client to use our server

When we run the server, the server is going to print these lines
```
....
Command-line Access: https://docs.minio.io/docs/minio-client-quickstart-guide
   $ mc config host add myzerostor http://192.168.194.249:9000 XXX YYYYYY
....
```
See your screen, and type the `mc config ...` command printed there.

### list all buckets/namespace
```
$ mc ls myzerostor
[1970-01-01 07:00:00 WIB]     0B newbucket/
```

### upload file
```
$ mc cp minio myzerostor/newbucket
minio:                                    24.95 MB / 24.95 MB ┃▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓┃ 100.00% 29.54 MB/s 0s
```

### check file exist
```
$ mc ls myzerostor/newbucket
[2018-02-20 11:02:01 WIB]  25MiB minio
```
### download the file
```
mc cat myzerostor/newbucket/minio > /tmp/bin-minio
```

Check the downloaded file is not corrupt
```
cmp minio /tmp/bin-minio
```


