package zerostor

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sort"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// initialize data fixture
	var (
		etag            = minio.GenETag()
		contentType     = "application/json"
		contentEncoding = "gzip"
		data            = make([]byte, dataLen)
		val1            = "val1"
		key1            = "Key1"
	)
	userMeta := map[string]string{
		"Content-Type":     contentType,
		"Content-Encoding": contentEncoding,
		key1:               val1,
	}
	rand.Read(data)

	// upload object
	_, err = zo.(*zerostorObjects).putObject(bucket, object, bytes.NewReader(data), userMeta, etag)
	if err != nil {
		t.Fatalf("failed to put object = %v", err)
	}

	// check object info
	info, err := zo.GetObjectInfo(ctx, bucket, object)
	if err != nil {
		t.Fatalf("failed to get object info: %v", err)
	}
	if info.ETag != etag {
		t.Fatalf("invalid etag value: %v, expected: %v", info.ETag, etag)
	}
	if info.ContentType != contentType {
		t.Fatalf("invalid content type: %v, expected: %v", info.ContentType, contentType)
	}
	if info.ContentEncoding != contentEncoding {
		t.Fatalf("invalid content encoding: %v, expected: %v", info.ContentEncoding, contentEncoding)
	}
	if len(info.UserDefined) != len(userMeta)-1 {
		// we decrement by `1` here because we internally added etag to the user meta
		t.Fatalf("invalid number of user defined metadata: %v, expected: %v", len(info.UserDefined), len(userMeta)-1)
	}

	if info.UserDefined[key1] != val1 {
		t.Fatalf("invalid meta value of %v: %v, expected: %v", key1, info.UserDefined[key1], val1)
	}

	// get & check object data
	checkObject(ctx, t, zo, bucket, object, data)

	// copy object - get & check
	{
		destBucket := "destBucket"
		zo.(*zerostorObjects)._zstor.bktMgr.Create(destBucket)
		_, err = zo.CopyObject(ctx, bucket, object, destBucket, object, minio.ObjectInfo{})
		if err != nil {
			t.Fatalf("CopyObject failed: %v", err)
		}

		checkObject(ctx, t, zo, destBucket, object, data)

	}

	// delete object
	err = zo.DeleteObject(ctx, bucket, object)
	if err != nil {
		t.Fatalf("failed to delete object:%v", err)
	}

	// make sure the object is not exist anymore
	err = zo.GetObject(ctx, bucket, object, 0, 0, bytes.NewBuffer(nil), "")
	if err == nil {
		t.Fatalf("unexpected error=nil when getting non existed object")
	}
}

func TestGatewayListObject(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "buket"
		dataLen   = 4096
		numObject = 10
	)
	zo, cleanup, err := newZstorGateway(namespace, bucket)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		data        = make([]byte, dataLen)
		objectNames []string
	)
	// initialize data fixture
	rand.Read(data)

	// list objects, must be empty
	listResults, err := zo.ListObjects(ctx, bucket, "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(listResults.Objects) != 0 {
		t.Fatalf("objects should be empty, got: %v objects", len(listResults.Objects))
	}

	// upload objects
	for i := 0; i < numObject; i++ {
		object := fmt.Sprintf("object_%v", i)
		_, err = zo.(*zerostorObjects).putObject(bucket, object, bytes.NewReader(data), nil, "")
		if err != nil {
			t.Fatalf("failed to put object = %v", err)
		}
		objectNames = append(objectNames, object)
	}

	// list objects, must have all uploaded objects
	listResults, err = zo.ListObjects(ctx, bucket, "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(listResults.Objects) != numObject {
		t.Fatalf("invalid objects leng: %v, expected: %v", len(listResults.Objects), numObject)
	}

}

// Test Deleting non existent object.
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// make sure the object not exist
	err = zo.GetObject(ctx, bucket, object, 0, 0, bytes.NewBuffer(nil), "")
	if err == nil {
		t.Fatalf("unexpected error=nil when getting non existed object")
	}

	// delete object
	err = zo.DeleteObject(ctx, bucket, object)
	if err != nil {
		t.Fatalf("deleting non existent object should not return error, got: %v", err)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create buckets
	for _, bkt := range buckets[1:] {
		err = zo.MakeBucketWithLocation(ctx, bkt, "")
		if err != nil {
			t.Fatalf("create bucket `%v`: %v", bkt, err)
		}
	}

	// list buckets
	{
		bucketsInfo, err := zo.ListBuckets(ctx)
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
		info, err := zo.GetBucketInfo(ctx, buckets[0])
		if err != nil {
			t.Fatalf("GetBucket: %v", err)
		}
		if info.Name != buckets[0] {
			t.Fatalf("invalid bucket name : %v, expected: %v", info.Name, buckets[0])
		}
	}

	// delete bucket
	if err := zo.DeleteBucket(ctx, buckets[0]); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}

	// make sure bucket is not exist anymore
	if _, err := zo.GetBucketInfo(ctx, buckets[0]); err == nil {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// by default, BucketPolicy==None
	{
		var pol policy.BucketAccessPolicy
		pol, err = zo.GetBucketPolicy(ctx, bucket)
		if err != nil {
			t.Fatalf("failed to GetBucketPolicy:%v", err)
		}
		policies := policy.GetPolicies(pol.Statements, bucket, "")
		if len(policies) != 0 {
			t.Fatalf("invalid len policies, expected: 0, got: %v", len(policies))
		}
	}
	{
		// set bucket policy & then check bucket policy
		var pol policy.BucketAccessPolicy
		pol.Statements = policy.SetPolicy(pol.Statements, policy.BucketPolicyReadOnly, bucket, "")
		err = zo.SetBucketPolicy(ctx, bucket, pol)
		if err != nil {
			t.Fatalf("failed to SetBucketPolicy")
		}

		// get bucket policy
		pol, err = zo.GetBucketPolicy(ctx, bucket)
		if err != nil {
			t.Fatalf("failed to GetBucketPolicy:%v", err)
		}
		policies := policy.GetPolicies(pol.Statements, bucket, "")
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
		err = zo.DeleteBucketPolicy(ctx, bucket)
		if err != nil {
			t.Fatalf("failed to DeleteBucketPolicy: %v", err)
		}

		pol, err := zo.GetBucketPolicy(ctx, bucket)
		if err != nil {
			t.Fatalf("failed to GetBucketPolicy:%v", err)
		}
		policies := policy.GetPolicies(pol.Statements, bucket, "")
		if len(policies) != 0 {
			t.Fatalf("invalid len policies, expected: 0, got: %v", len(policies))
		}

	}
}

func TestMultipartUploadComplete(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "bucket"
		object    = "object"
		dataLen   = 1000
		numPart   = 100
	)

	zo, cleanup, err := newZstorGateway(namespace, bucket)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// init the data we want to upload
	var (
		data    = make([]byte, dataLen)
		partLen = dataLen / numPart
	)
	rand.Read(data)

	// Create Upload
	uploadID, err := zo.NewMultipartUpload(ctx, bucket, object, nil)
	if err != nil {
		t.Fatalf("NewMultipartUpload failed: %v", err)
	}

	// Upload each part
	var uploadParts []minio.PartInfo
	for i := 0; i < numPart; i++ {
		var part minio.PartInfo
		rd := bytes.NewReader(data[i*partLen : (i*partLen)+partLen])
		part, err = zo.(*zerostorObjects).putObjectPart(ctx, bucket, object, uploadID, "", i, rd)
		if err != nil {
			t.Fatalf("failed to PutObjectPart %v, err: %v", i, err)
		}
		uploadParts = append(uploadParts, part)
	}

	// Complete the upload
	var completeParts []minio.CompletePart
	for _, part := range uploadParts {
		completeParts = append(completeParts, minio.CompletePart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}
	_, err = zo.CompleteMultipartUpload(ctx, bucket, object, uploadID, completeParts)
	if err != nil {
		t.Fatalf("failed to CompleteMultipartUpload:%v", err)
	}

	// check the uploaded object
	checkObject(ctx, t, zo, bucket, object, data)

	// ListMultipartUploads must return empty after the upload being completed
	listUpload, err := zo.ListMultipartUploads(ctx, bucket, "", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListMultipartUploads failed: %v", err)
	}

	// check uploadID of the listed multipart uploads
	if len(listUpload.Uploads) != 0 {
		t.Fatalf("invalid num uploads after complete: %v, expected: 0", len(listUpload.Uploads))
	}

}

func TestMultipartUploadListAbort(t *testing.T) {
	const (
		namespace  = "ns"
		bucket     = "bucket"
		dataLen    = 1000
		numPart    = 100
		numUploads = 10
	)

	zo, cleanup, err := newZstorGateway(namespace, bucket)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// init the data we want to upload
	var (
		datas            = make(map[string][]byte)
		partLen          = dataLen / numPart
		uploadIDs        = make(map[string]struct{})
		uploadedPartsMap = make(map[string][]minio.PartInfo)
	)

	// do upload
	for i := 0; i < numUploads; i++ {
		var (
			data     = make([]byte, dataLen)
			object   = fmt.Sprintf("object_%v", i)
			uploadID string
			part     minio.PartInfo
		)
		rand.Read(data)

		// Create Upload
		uploadID, err = zo.NewMultipartUpload(ctx, bucket, object, nil)
		if err != nil {
			t.Fatalf("NewMultipartUpload failed: %v", err)
		}
		uploadIDs[uploadID] = struct{}{}
		datas[uploadID] = data

		// Upload each part
		var parts []minio.PartInfo
		for i := 0; i < numPart; i++ {
			rd := bytes.NewReader(data[i*partLen : (i*partLen)+partLen])
			part, err = zo.(*zerostorObjects).putObjectPart(ctx, bucket, object, uploadID, "", i, rd)
			if err != nil {
				t.Fatalf("failed to PutObjectPart %v, err: %v", i, err)
			}
			parts = append(parts, part)
		}
		uploadedPartsMap[uploadID] = parts
	}

	// ListMultipartUpload
	uploads, err := zo.ListMultipartUploads(ctx, bucket, "", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListMultipartUploads failed: %v", err)
	}

	// check uploadID of the listed multipart uploads
	if len(uploads.Uploads) != numUploads {
		t.Fatalf("invalid num uploads: %v, expected: %v", len(uploads.Uploads), numUploads)
	}
	for i, upload := range uploads.Uploads {
		if _, ok := uploadIDs[upload.UploadID]; !ok {
			t.Fatalf("Invalid uploadID of part %v: %v", i, upload.UploadID)
		}
	}

	// check object parts
	for _, upload := range uploads.Uploads {
		var listPartsResult minio.ListPartsInfo
		listPartsResult, err = zo.ListObjectParts(ctx, bucket, upload.Object, upload.UploadID, 0, 1000)
		if err != nil {
			t.Fatalf("Failed to ListObjectPart of upload ID:%v, err: %v", upload.UploadID, err)
		}

		listedParts := listPartsResult.Parts
		// check the parts
		uploadedParts := uploadedPartsMap[upload.UploadID]
		if len(uploadedParts) != len(listedParts) {
			t.Fatalf("invalid number of parts of upload ID `%v`: %v, expected: %v", upload.UploadID, len(uploadedParts), len(listedParts))
		}

		for i, part := range uploadedParts {
			if part.PartNumber != listedParts[i].PartNumber {
				t.Fatalf("invalid part number of uploadID `%v`: %v, expected: %v",
					upload.UploadID, listedParts[i].PartNumber, part.PartNumber)
			}
			if part.ETag != listedParts[i].ETag {
				t.Fatalf("invalid etag of uploadID `%v`: %v, expected: %v",
					upload.UploadID, listedParts[i].ETag, part.ETag)
			}
		}
	}

	// AbortMultipartUpload
	for _, upload := range uploads.Uploads {
		err = zo.AbortMultipartUpload(ctx, bucket, upload.Object, upload.UploadID)
		if err != nil {
			t.Fatalf("failed to AbortMultipartUpload uploadID `%v`: %v", upload.UploadID, err)
		}
	}

	// ListMultipartUploads must return empty after all uploads being aborted
	uploadsAfterAbort, err := zo.ListMultipartUploads(ctx, bucket, "", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListMultipartUploads failed: %v", err)
	}

	// check uploadID of the listed multipart uploads
	if len(uploadsAfterAbort.Uploads) != 0 {
		t.Fatalf("invalid num uploads after abort: %v, expected: 0", len(uploadsAfterAbort.Uploads))
	}

}

// TestMultipartUploadComplete test multipart upload
// using CopyObjectPart API
func TestMultipartUploadCopyComplete(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "bucket"
		object    = "object"
		dataLen   = 1000
		numPart   = 100
	)

	zo, cleanup, err := newZstorGateway(namespace, bucket)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// init the data we want to upload
	var (
		data        = make([]byte, dataLen)
		partLen     = dataLen / numPart
		objectsInfo []minio.ObjectInfo
		info        minio.ObjectInfo
	)
	rand.Read(data)

	// upload the parts using PutObject
	for i := 0; i < numPart; i++ {
		rd := bytes.NewReader(data[i*partLen : (i*partLen)+partLen])
		objectPart := fmt.Sprintf("object_%v", i)
		info, err = zo.(*zerostorObjects).putObject(bucket, objectPart, rd, nil, "")
		if err != nil {
			t.Fatalf("failed to PutObjectPart %v, err: %v", i, err)
		}
		objectsInfo = append(objectsInfo, info)
	}

	// Create Upload
	uploadID, err := zo.NewMultipartUpload(ctx, bucket, object, nil)
	if err != nil {
		t.Fatalf("NewMultipartUpload failed: %v", err)
	}

	// CopyPart
	var (
		uploadedParts []minio.PartInfo
		part          minio.PartInfo
	)
	for i, info := range objectsInfo {
		part, err = zo.CopyObjectPart(ctx, bucket, info.Name, bucket, object, uploadID, i, 0, 0, info)
		if err != nil {
			t.Fatalf("CopyObjectPart failed: %v", err)
		}
		uploadedParts = append(uploadedParts, part)
	}

	// Complete the upload
	var completeParts []minio.CompletePart
	for _, part := range uploadedParts {
		completeParts = append(completeParts, minio.CompletePart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}
	_, err = zo.CompleteMultipartUpload(ctx, bucket, object, uploadID, completeParts)
	if err != nil {
		t.Fatalf("failed to CompleteMultipartUpload:%v", err)
	}

	// check the uploaded object
	checkObject(ctx, t, zo, bucket, object, data)

}

func checkObject(ctx context.Context, t *testing.T, gateway minio.ObjectLayer, bucket, object string, expected []byte) {
	buf := bytes.NewBuffer(nil)

	// get object info
	info, err := gateway.GetObjectInfo(ctx, bucket, object)
	if err != nil {
		t.Fatalf("failed to getObjectInfo(%v,%v): %v", bucket, object, err)
	}
	if info.Bucket != bucket {
		t.Fatalf("invalid bucket: %v, expected: %v", info.Bucket, bucket)
	}
	if info.Name != object {
		t.Fatalf("invalid object name: %v, expected: %v", info.Name, object)
	}

	// check object content
	err = gateway.GetObject(ctx, bucket, object, 0, int64(len(expected)), buf, "")
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
	// creates 0-stor wrapper
	zstor, cleanupZstor, metaDir, err := newTestZerostor(namespace, bucket)
	if err != nil {
		fmt.Println("failed to create test Zerostor:", err.Error())
		return nil, nil, err
	}

	// creates multipart manager
	mpartMgr, err := newMultipartManagerFromCfg(metaCfg{}, metaDir, zstor)
	if err != nil {
		return nil, nil, err
	}

	gl, err := newGatewayLayer(zstor, mpartMgr)
	if err != nil {
		fmt.Println("newGatewayLayerWithZerostor failed:", err.Error())
	}

	return gl, cleanupZstor, err
}

func compareStringArrs(arr1, arr2 []string) error {
	if len(arr1) != len(arr2) {
		return fmt.Errorf("different length : %v and %v", len(arr1), len(arr2))
	}
	sort.Strings(arr1)
	sort.Strings(arr2)

	for i, elem := range arr1 {
		if elem != arr2[i] {
			return fmt.Errorf("elem %v different : `%v` and `%v`", i, elem, arr2[i])
		}
	}
	return nil
}
