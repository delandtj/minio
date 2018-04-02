package meta

import (
	"time"

	"github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
	"github.com/zero-os/0-stor/client/metastor/db"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
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
}

// BucketManager defines interface that manages
// the gateway's buckets
type BucketManager interface {
	// Create creates a bucket
	Create(bucket string) error

	// Get gets a bucket, return false if not exist
	Get(bucket string) (*Bucket, bool)

	// GetAllBuckets returns all buckets
	GetAllBuckets() []Bucket

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
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		Size:    md.Size, // TODO : returns the actual size
		ModTime: zstorEpochToTimestamp(md.LastWriteEpoch),
		// TODO:
		// ETag:"",
		// ContentType:
		// UserDefined
	}
}

// convert zerostor epoch time to Go timestamp
func zstorEpochToTimestamp(epoch int64) time.Time {
	return time.Unix(epoch/1e9, epoch%1e9)
}
