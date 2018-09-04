package multipart

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sort"
	"testing"

	minio "github.com/minio/minio/cmd"
	"github.com/threefoldtech/0-stor/client/metastor"
)

func TestManagerComplete(t *testing.T) {
	const (
		bucket   = "bucket"
		object   = "object"
		dataLen  = 1000
		partSize = 100
		key1     = "Key1"
		val1     = "val1"
	)

	mgr, _, stor, cleanup := newTestUploadMgr(t)
	defer cleanup()

	// Init multipart upload
	metadata := map[string]string{
		key1: val1,
	}
	uploadID, err := mgr.Init(bucket, object, metadata)
	if err != nil {
		t.Fatalf("mulipart upload failed: %v", err)
	}

	var (
		data            = make([]byte, dataLen)
		toCompleteParts []minio.CompletePart
	)
	rand.Read(data)

	// UploadPart
	// add parts which we want to complete later
	for i := 0; i < dataLen/partSize; i++ {
		dataPart := data[i*partSize : (i+1)*partSize]
		etag := minio.GenETag()
		partNumber := i

		_, err = mgr.UploadPart(bucket, object, uploadID, etag, partNumber, bytes.NewReader(dataPart))
		if err != nil {
			t.Fatalf("failed to upload part: %v, err: %v", i, err)
		}
		toCompleteParts = append(toCompleteParts, minio.CompletePart{
			ETag:       etag,
			PartNumber: partNumber,
		})
	}

	// add invalid parts
	for i := 0; i < dataLen/partSize; i++ {
		dataPart := data[i*partSize : (i+1)*partSize]

		_, err = mgr.UploadPart(bucket, object, uploadID, minio.GenETag(), i, bytes.NewReader(dataPart))
		if err != nil {
			t.Fatalf("failed to upload part: %v, err: %v", i, err)
		}
	}

	// `Complete` Part
	_, err = mgr.Complete(bucket, object, uploadID, toCompleteParts)
	if err != nil {
		t.Fatalf("Complete failed:%v", err)
	}

	// verify data uploaded
	uploadedBuf := bytes.NewBuffer(nil)
	err = stor.Read(bucket, object, uploadedBuf)
	if err != nil {
		t.Fatalf("failed to read uploaded part: %v", err)
	}

	if bytes.Compare(data, uploadedBuf.Bytes()) != 0 {
		t.Fatalf("uploaded buf is not valid")
	}
}

func TestManagerAbort(t *testing.T) {
	const (
		bucket   = "bucket"
		object   = "object"
		dataLen  = 1000
		partSize = 100
	)

	mgr, metaMgr, stor, cleanup := newTestUploadMgr(t)
	defer cleanup()

	// init
	uploadID, err := mgr.Init(bucket, object, nil)
	if err != nil {
		t.Fatalf("mulipart upload failed: %v", err)
	}

	// add part
	data := make([]byte, dataLen)
	rand.Read(data)

	for i := 0; i < dataLen/partSize; i++ {
		dataPart := data[i*partSize : (i+1)*partSize]

		_, err = mgr.UploadPart(bucket, object, uploadID, minio.GenETag(), i, bytes.NewReader(dataPart))
		if err != nil {
			t.Fatalf("failed to upload part: %v, err: %v", i, err)
		}
	}

	// get parts info
	_, parts, err := metaMgr.GetMultipart(bucket, uploadID)
	if err != nil {
		t.Fatalf("failed to list part :%v", err)
	}

	// Abort
	err = mgr.Abort(bucket, object, uploadID)
	if err != nil {
		t.Fatalf("failed to abort: %v", err)
	}

	// verify data parts deleted
	for _, part := range parts {
		buf := bytes.NewBuffer(nil)
		err = stor.Read(MultipartBucket, part.Object, buf)
		if err != metastor.ErrNotFound {
			t.Fatalf("part `%v` still exist, err: %v", part.PartNumber, err)
		}
	}

	// verify there is no listed uploads after abort
	uploads, err := mgr.ListUpload(bucket, "", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListUpload failed: %v", err)
	}

	if len(uploads.Uploads) != 0 {
		t.Fatalf("invalid uploads len after abort: %v, expected: 0", len(uploads.Uploads))
	}
}

func TestManagerListPart(t *testing.T) {
	const (
		bucket   = "bucket"
		object   = "object"
		dataLen  = 1000
		partSize = 100
	)

	mgr, _, _, cleanup := newTestUploadMgr(t)
	defer cleanup()

	// init
	uploadID, err := mgr.Init(bucket, object, nil)
	if err != nil {
		t.Fatalf("mulipart upload failed: %v", err)
	}

	// upload part
	var (
		uploadedParts []minio.PartInfo
		data          = make([]byte, dataLen)
		part          minio.PartInfo
	)
	rand.Read(data)

	for i := 0; i < dataLen/partSize; i++ {
		dataPart := data[i*partSize : (i+1)*partSize]

		part, err = mgr.UploadPart(bucket, object, uploadID, minio.GenETag(), i, bytes.NewReader(dataPart))
		if err != nil {
			t.Fatalf("failed to upload part: %v, err: %v", i, err)
		}
		uploadedParts = append(uploadedParts, part)
	}

	// list  part
	listPartsInfo, err := mgr.ListParts(bucket, object, uploadID, 0, 1000)
	if err != nil {
		t.Fatalf("ListParts failed: %v", err)
	}
	if listPartsInfo.Bucket != bucket {
		t.Fatalf("invalid bucket: %v, expected: %v", listPartsInfo.Bucket, bucket)
	}
	if listPartsInfo.Object != object {
		t.Fatalf("invalid object: %v, expected: %v", listPartsInfo.Object, object)
	}
	if listPartsInfo.UploadID != uploadID {
		t.Fatalf("invalid uploadID: %v, expected: %v", listPartsInfo.UploadID, uploadID)
	}

	// check the listed parts
	listedParts := listPartsInfo.Parts
	err = func() error {
		if len(listedParts) != len(uploadedParts) {
			return fmt.Errorf("invalid listed parts:%v, expected: %v", len(listedParts), len(uploadedParts))
		}
		for i, part := range uploadedParts {
			if part.PartNumber != listedParts[i].PartNumber {
				return fmt.Errorf("invalid part number for part: %v, got: %v, expected: %v",
					i, listedParts[i].PartNumber, part.PartNumber)
			}
			if part.ETag != listedParts[i].ETag {
				return fmt.Errorf("invalid ETag for part: %v, got: %v, expected: %v",
					i, listedParts[i].ETag, part.ETag)
			}
		}
		return nil
	}()
	if err != nil {
		t.Fatalf("list parts result check failed: %v", err)
	}
}

func TestManagerListUpload(t *testing.T) {
	const (
		bucket    = "bucket"
		object    = "object"
		dataLen   = 1000
		partSize  = 100
		numUpload = 10
	)

	mgr, _, _, cleanup := newTestUploadMgr(t)
	defer cleanup()

	var (
		data      = make([]byte, dataLen)
		uploadIDs []string
	)
	rand.Read(data)

	for i := 0; i < numUpload; i++ {
		// init
		uploadID, err := mgr.Init(bucket, object, nil)
		if err != nil {
			t.Fatalf("mulipart upload failed: %v", err)
		}

		// upload part

		for i := 0; i < dataLen/partSize; i++ {
			_, err := mgr.UploadPart(bucket, object, uploadID, minio.GenETag(), i, bytes.NewReader(data))
			if err != nil {
				t.Fatalf("failed to upload part: %v, err: %v", i, err)
			}
		}

		uploadIDs = append(uploadIDs, uploadID)
	}
	sort.Strings(uploadIDs)

	listMultiparts, err := mgr.ListUpload(bucket, "", "", "", "", 10000)
	if err != nil {
		t.Fatalf("failed to list upload: %v", err)
	}

	uploadeds := listMultiparts.Uploads
	if len(uploadeds) != numUpload {
		t.Fatalf("invalid number of listed multipart upload: %v, expected: %v", len(uploadeds), numUpload)
	}
	for i, uploadID := range uploadIDs {
		if uploadID != uploadeds[i].UploadID {
			t.Fatalf("invalid upload id for upload `%v`: %v, expected: %v", i, uploadeds[i].UploadID, uploadID)
		}
	}
}

func newTestUploadMgr(t *testing.T) (Manager, MetaManager, Storage, func()) {
	uploadMetaMgr, metaStor, cleanupMeta := newTestFilemetaUploadMgr(t)

	stor, storCleanup := newTestZstorClient(t, metaStor)

	mgr := NewManager(stor, uploadMetaMgr)

	cleanup := func() {
		mgr.Close()
		storCleanup()
		cleanupMeta()
	}
	return mgr, uploadMetaMgr, stor, cleanup
}
