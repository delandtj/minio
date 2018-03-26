# Implementation status

## API 

[C] Code
[SMT] manual test
[AT] Automatic test

| API                     | C   | SMT | AT  | Notes                                                                                                               |
| ----------------------- | --- | --- | --- | ------------------------------------------------------------------------------------------------------------------- |
| Create Bucket           | v   | v   | v   | Without Location                                                                                                    |
| Get Bucket Info         | v   | v   | v   |
| List of Buckets         | v   | v   | v   |
| List Objects in Bucket  | v   | v   | v   |
| Delete Bucket           | v   | v   | v   |
| Set Bucket Policy       | v   | v   | v   |
| Get Bucket Policy       | v   | v   | v   |
| Delete Bucket Policy    | v   | v   | v   |
| ----------------------- | --- | --- | --- | -------------------------------------------------------------------------                                           |
| Get Object              | v   | v   | v   | Without etag (https://github.com/zero-os/0-stor/issues/503).                                                        |
| Get Object Info         | v   | v   | v   | Without etag, content-type, content-encoding, user defined metadata (https://github.com/zero-os/0-stor/issues/503). |
| Put Object              | v   | v   | v   | Without etag & user defined metadata (https://github.com/zero-os/0-stor/issues/503)..                               |
| Delete Object           | v   | v   | v   |
| Copy object             | v   | v   | v   |
| -----------             | -   | --  | -   | ----------------------------------                                                                                  |
| NewMultipartUpload      | v   | v   | v   |
| PutObjectPart           | v   | v   | v   |
| CompleteMultipart       | v   | v   | v   |
| AbortMultipart          | v   | v   | v   |
| ------------------      | -   | -   | -   | ----------------------------------                                                                                  |
| Heal Bucket             | v   | v   | -   | always return true                                                                                                  |
| Heal Object             | v   |     |     |
| List Object Heal        | -   | -   |     | Not supported by 0-stor, other minio gateways also doesn't implement                                                |
| List Bucket Heal        | -   | -   |     | Not supported by 0-stor, other minio gateways also doesn't implement                                                |