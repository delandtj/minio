package zerostor

import (
	"bytes"
	"crypto/rand"
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

func TestGatewayRoundTrip(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "buket"
		object    = "object"
		dataLen   = 4096
	)
	zo, cleanup, err := newZstorGateway(namespace, bucket)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	// initialize data fixture
	data := make([]byte, dataLen)
	rand.Read(data)

	// upload object
	_, err = zo.(*zerostorObjects).putObject(bucket, object, bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("failed to put object = %v", err)
	}

	// get & check object
	checkObject(t, zo, bucket, object, data)

	// delete object
	err = zo.DeleteObject(bucket, object)
	if err != nil {
		t.Fatalf("failed to delete object:%v", err)
	}

	// make sure the object is not exist anymore
	err = zo.GetObject(bucket, object, 0, 0, bytes.NewBuffer(nil), "")
	if err == nil {
		t.Fatalf("unexpected error=nil when deleting non existed object")
	}
}

func checkObject(t *testing.T, gateway minio.ObjectLayer, bucket, object string, expected []byte) {
	buf := bytes.NewBuffer(nil)

	err := gateway.GetObject(bucket, object, 0, int64(len(expected)), buf, "")
	if err != nil {
		t.Fatalf("failed to GetObject: %v", err)
	}
	if len(expected) != buf.Len() {
		t.Fatalf("GetObject give invalida data length: %v, expected: %v", buf.Len(), len(expected))
	}
	if !bytes.Equal(expected, buf.Bytes()) {
		t.Fatalf("GetObject produce unexpected result")
	}

}

func newZstorGateway(namespace, bucket string) (minio.ObjectLayer, func(), error) {
	zstor, cleanupZstor, err := newTestZerostor(namespace, bucket)
	if err != nil {
		return nil, nil, err
	}
	return newGatewayLayerWithZerostor(zstor), cleanupZstor, nil
}
