package zerostor

import (
	"fmt"
	"testing"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/errors"
	"github.com/zero-os/0-stor/client/datastor"
	"github.com/zero-os/0-stor/client/metastor"
)

func TestZstorToObjectError(t *testing.T) {
	var (
		nonZstorErr = fmt.Errorf("Non zerostor error")
	)
	const (
		bucketName = "bucket"
		objectName = "object"
	)

	testCases := []struct {
		actualErr   error
		expectedErr error
		bucket      string
		object      string
	}{
		{nil, nil, "", ""},
		{
			errors.Trace(nonZstorErr),
			nonZstorErr,
			"", "",
		},
		{
			errors.Trace(errBucketNotFound),
			minio.BucketNotFound{Bucket: bucketName},
			bucketName,
			"",
		},
		{
			errors.Trace(errBucketExists),
			minio.BucketExists{Bucket: bucketName},
			bucketName,
			"",
		},
		{
			errors.Trace(metastor.ErrNotFound),
			minio.ObjectNotFound{Bucket: bucketName, Object: objectName},
			bucketName,
			objectName,
		},
		{
			errors.Trace(datastor.ErrMissingKey),
			minio.ObjectNotFound{Bucket: bucketName, Object: objectName},
			bucketName,
			objectName,
		},
		{
			errors.Trace(datastor.ErrMissingData),
			minio.ObjectNotFound{Bucket: bucketName, Object: objectName},
			bucketName,
			objectName,
		},
		{
			errors.Trace(datastor.ErrKeyNotFound),
			minio.ObjectNotFound{Bucket: bucketName, Object: objectName},
			bucketName,
			objectName,
		},
	}

	for i, tc := range testCases {
		err := zstorToObjectErr(tc.actualErr, tc.bucket, tc.object)
		if err == nil {
			if tc.expectedErr != nil {
				t.Errorf("Test %d: Expected nil, got %v", i, err)
			}
		} else if err.Error() != tc.expectedErr.Error() {
			t.Errorf("Test %d: Expected error %v, got %v", i, tc.expectedErr, err)
		}
	}
}

func TestZstorMetaToObjectInfo(t *testing.T) {

}
