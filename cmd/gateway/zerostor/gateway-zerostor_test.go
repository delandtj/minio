package zerostor

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/minio/minio-go/pkg/policy"
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

func TestGatewayObjectRoundTrip(t *testing.T) {
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

	// copy object - get & check
	{
		destBucket := "destBucket"
		zo.(*zerostorObjects).bktMgr.createBucket(destBucket)
		_, err := zo.CopyObject(bucket, object, destBucket, object, minio.ObjectInfo{})
		if err != nil {
			t.Fatalf("CopyObject failed: %v", err)
		}

		checkObject(t, zo, destBucket, object, data)

	}

	// delete object
	err = zo.DeleteObject(bucket, object)
	if err != nil {
		t.Fatalf("failed to delete object:%v", err)
	}

	// make sure the object is not exist anymore
	err = zo.GetObject(bucket, object, 0, 0, bytes.NewBuffer(nil), "")
	if err == nil {
		t.Fatalf("unexpected error=nil when getting non existed object")
	}
}

// Test Deleting non existant object.
// it shouldn't return error
func TestDeleteNotExistObject(t *testing.T) {
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

	// make sure the object not exist
	err = zo.GetObject(bucket, object, 0, 0, bytes.NewBuffer(nil), "")
	if err == nil {
		t.Fatalf("unexpected error=nil when getting non existed object")
	}

	// delete object
	err = zo.DeleteObject(bucket, object)
	if err != nil {
		t.Fatalf("deleting non existant object should not return error, got: %v", err)
	}
}

func TestGatewayBucketRoundTrip(t *testing.T) {
	const (
		namespace = "ns"
	)
	var (
		buckets = []string{"bkt_1", "bkt_2", "bkt_3"}
	)

	zo, cleanup, err := newZstorGateway(namespace, buckets[0])
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	// create buckets
	for _, bkt := range buckets[1:] {
		err = zo.MakeBucketWithLocation(bkt, "")
		if err != nil {
			t.Fatalf("create bucket `%v`: %v", bkt, err)
		}
	}

	// list buckets
	{
		bucketsInfo, err := zo.ListBuckets()
		if err != nil {
			t.Fatalf("list buckets: %v", err)
		}
		var listed []string
		for _, bi := range bucketsInfo {
			listed = append(listed, bi.Name)
		}
		if err := compareStringArrs(buckets, listed); err != nil {
			t.Fatalf("invalid ListBuckets result: %v", err)
		}
	}

	// get bucket info
	{
		info, err := zo.GetBucketInfo(buckets[0])
		if err != nil {
			t.Fatalf("GetBucket: %v", err)
		}
		if info.Name != buckets[0] {
			t.Fatalf("invalid bucket name : %v, expected: %v", info.Name, buckets[0])
		}
	}

	// delete bucket
	if err := zo.DeleteBucket(buckets[0]); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}

	// make sure bucket is not exist anymore
	if _, err := zo.GetBucketInfo(buckets[0]); err == nil {
		t.Fatalf("expected to get error")
	}
}

func TestGatewayBucketPolicy(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "bucket"
	)

	zo, cleanup, err := newZstorGateway(namespace, bucket)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	// by default, BucketPolicy==None
	{
		pol, err := zo.GetBucketPolicy(bucket)
		if err != nil {
			t.Fatalf("failed to GetBucketPolicy:%v", err)
		}
		policies := policy.GetPolicies(pol.Statements, bucket)
		if len(policies) != 0 {
			t.Fatalf("invalid len policies, expected: 0, got: %v", len(policies))
		}
	}
	{
		// set bucket policy & then check bucket policy
		var pol policy.BucketAccessPolicy
		pol.Statements = policy.SetPolicy(pol.Statements, policy.BucketPolicyReadOnly, bucket, "")
		err = zo.SetBucketPolicy(bucket, pol)
		if err != nil {
			t.Fatalf("failed to SetBucketPolicy")
		}

		// get bucket policy
		pol, err := zo.GetBucketPolicy(bucket)
		if err != nil {
			t.Fatalf("failed to GetBucketPolicy:%v", err)
		}
		policies := policy.GetPolicies(pol.Statements, bucket)
		if len(policies) != 1 {
			t.Fatalf("invalid len policies, expected: 1, got: %v", len(policies))
		}
		bucketPolicy, ok := policies[bucket+supportedBucketPolicyPrefix]
		if !ok {
			t.Fatalf("policiy not found")
		}
		if bucketPolicy != policy.BucketPolicyReadOnly {
			t.Fatalf("unexpected bucket policy. got: %v, expected: %v", bucketPolicy, policy.BucketPolicyReadOnly)
		}
	}
	{
		//delete bucket and make sure the policy back to none
		err = zo.DeleteBucketPolicy(bucket)
		if err != nil {
			t.Fatalf("failed to DeleteBucketPolicy: %v", err)
		}

		pol, err := zo.GetBucketPolicy(bucket)
		if err != nil {
			t.Fatalf("failed to GetBucketPolicy:%v", err)
		}
		policies := policy.GetPolicies(pol.Statements, bucket)
		if len(policies) != 0 {
			t.Fatalf("invalid len policies, expected: 0, got: %v", len(policies))
		}

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
	gl, err := newGatewayLayerWithZerostor(zstor, zstor.filemeta.rootDir)

	return gl, cleanupZstor, err
}
