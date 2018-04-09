# Implementation status

## API 

[C] Code
[SMT] manual test
[AT] Automatic test

| API                     | C   | SMT | AT  | Notes                                                                                          |
| ----------------------- | --- | --- | --- | ---------------------------------------------------------------------------------------------- |
| Create Bucket           | v   | v   | v   | Without Location                                                                               |
| Get Bucket Info         | v   | v   | v   |
| List of Buckets         | v   | v   | v   |
| List Objects in Bucket  | v   | v   | v   |
| Delete Bucket           | v   | v   | v   |
| Set Bucket Policy       | v   | v   | v   |
| Get Bucket Policy       | v   | v   | v   |
| Delete Bucket Policy    | v   | v   | v   |
| ----------------------- | --- | --- | --- | -------------------------------------------------------------------------                      |
| Get Object              | v   | v   | v   |                                                                                                |
| Get Object Info         | v   | v   | v   |                                                                                                |
| Put Object              | v   | v   | v   |                                                                                                |
| Delete Object           | v   | v   | v   |
| Copy object             | v   | v   | v   |
| -----------             | -   | --  | -   | ----------------------------------                                                             |
| NewMultipartUpload      | v   | v   | v   |
| PutObjectPart           | v   | v   | v   |
| CopyObjectPart          | v   |     | v   |
| CompleteMultipart       | v   | v   | v   |
| AbortMultipart          | v   | v   | v   |
| ListObjectParts         | v   |     | v   |
| ListMultipartUpload     | v   |     | v   | only respect `bucket` argument, because of lack of docs & example in production ready gateway. |
| ----------              | --- | -   | --  | ----------------------------------------                                                       |
| StorageInfo             | v   | v   |     | If there is no total size, set to 1TB                                                          |