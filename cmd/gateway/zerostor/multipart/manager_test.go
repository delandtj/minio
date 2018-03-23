package multipart

import (
	"bytes"
	"crypto/rand"
	//"sort"
	"testing"

	minio "github.com/minio/minio/cmd"
	"github.com/zero-os/0-stor/client/datastor"
)

func TestManagerComplete(t *testing.T) {
	const (
		bucket   = "bucket"
		object   = "object"
		dataLen  = 1000
		partSize = 100
	)
	stor := newStorTest()

	mgr, metaMgr, cleanup := newDefaultUploadMgr(t, stor)
	defer cleanup()

	// Init multipart upload
	uploadID, err := mgr.Init(bucket, object)
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

		_, err := mgr.UploadPart(bucket, object, uploadID, etag, partNumber, bytes.NewReader(dataPart))
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

		_, err := mgr.UploadPart(bucket, object, uploadID, minio.GenETag(), i, bytes.NewReader(dataPart))
		if err != nil {
			t.Fatalf("failed to upload part: %v, err: %v", i, err)
		}
	}

	// all parts that should be deleted
	deletedParts, err := metaMgr.ListPart(uploadID)
	if err != nil {
		t.Fatalf("failed to list part :%v", err)
	}

	// `Complete` Part
	_, err = mgr.Complete(bucket, object, uploadID, toCompleteParts)

	// verify data uploaded
	uploadedBuf := bytes.NewBuffer(nil)
	err = stor.Read(bucket, object, uploadedBuf)
	if err != nil {
		t.Fatalf("failed to read uploaded part: %v", err)
	}

	if bytes.Compare(data, uploadedBuf.Bytes()) != 0 {
		t.Fatalf("uploaded buf is not valid")
	}

	// verify all data parts are deleted
	for _, part := range deletedParts {
		buf := bytes.NewBuffer(nil)
		err = stor.Read(MultipartBucket, part.Object, buf)
		if err != datastor.ErrKeyNotFound {
			t.Fatalf("part `%v` still exist", part.PartNumber)
		}
	}
}

func TestManagerAbort(t *testing.T) {
	const (
		bucket   = "bucket"
		object   = "object"
		dataLen  = 1000
		partSize = 100
	)
	stor := newStorTest()

	mgr, metaMgr, cleanup := newDefaultUploadMgr(t, stor)
	defer cleanup()

	// init
	uploadID, err := mgr.Init(bucket, object)
	if err != nil {
		t.Fatalf("mulipart upload failed: %v", err)
	}

	// add part
	data := make([]byte, dataLen)
	rand.Read(data)

	for i := 0; i < dataLen/partSize; i++ {
		dataPart := data[i*partSize : (i+1)*partSize]

		_, err := mgr.UploadPart(bucket, object, uploadID, minio.GenETag(), i, bytes.NewReader(dataPart))
		if err != nil {
			t.Fatalf("failed to upload part: %v, err: %v", i, err)
		}
	}

	// get parts info
	parts, err := metaMgr.ListPart(uploadID)
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
		if err != datastor.ErrKeyNotFound {
			t.Fatalf("part `%v` still exist", part.PartNumber)
		}
	}
}

func newDefaultUploadMgr(t *testing.T, stor Storage) (Manager, MetaManager, func()) {
	metaMgr, cleanupMeta := newTestFilemetaUploadMgr(t)

	mgr := NewManager(stor, metaMgr)

	cleanup := func() {
		mgr.Close()
		cleanupMeta()
	}
	return mgr, metaMgr, cleanup
}
