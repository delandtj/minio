package meta

import (
	"net/http"
	"time"

	"github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
	"github.com/zero-os/0-stor/client/metastor/db"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

var (
	contentTypeKey     = http.CanonicalHeaderKey("content-type")
	contentEncodingKey = http.CanonicalHeaderKey("content-encoding")
	// ETagKey defines key of `etag` value in the metadata
	ETagKey = http.CanonicalHeaderKey("etag")
)

// Storage defines interface for 0-stor gateway metadata
type Storage interface {
	// 0-stor metastor DB interface
	db.DB

	// GetDecodeMeta gets and decode meta
	GetDecodeMeta(key []byte) (*metatypes.Metadata, error)

	// ListObjects list objects for the given parameters.
	// See https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
	// for the details
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error)

	Encode(md metatypes.Metadata) ([]byte, error)
}

// BucketManager defines interface that manages
// the gateway's buckets
type BucketManager interface {
	// Create creates a bucket
	Create(bucket string) error

	// Get gets a bucket
	Get(bucket string) (*Bucket, error)

	// GetAllBuckets returns all buckets
	GetAllBuckets() ([]Bucket, error)

	// Del deletes a bucket
	Del(bucket string) error

	// SetPolicy set a bucket policy
	SetPolicy(bucket string, pol policy.BucketPolicy) error
}

// Bucket defines a bucket
type Bucket struct {
	Name    string
	Created time.Time
	Policy  policy.BucketPolicy
}

// CreateObjectInfo creates minio ObjectInfo from 0-stor metadata
func CreateObjectInfo(bucket, object string, md *metatypes.Metadata) minio.ObjectInfo {
	info := minio.ObjectInfo{
		Bucket:          bucket,
		Name:            object,
		Size:            md.Size,
		ModTime:         zstorEpochToTimestamp(md.LastWriteEpoch),
		ETag:            getUserMetadataValue(ETagKey, md.UserDefined),
		ContentType:     getUserMetadataValue(contentTypeKey, md.UserDefined),
		ContentEncoding: getUserMetadataValue(contentEncodingKey, md.UserDefined),
	}

	delete(md.UserDefined, ETagKey) // delete etag from user defined because this field was from this gateway

	info.UserDefined = md.UserDefined

	return info
}

func getUserMetadataValue(key string, userMeta map[string]string) string {
	v, _ := userMeta[key]
	return v
}

// convert zerostor epoch time to Go timestamp
func zstorEpochToTimestamp(epoch int64) time.Time {
	return time.Unix(epoch/1e9, epoch%1e9)
}
