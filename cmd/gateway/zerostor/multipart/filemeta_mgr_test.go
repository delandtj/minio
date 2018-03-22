package multipart

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	minio "github.com/minio/minio/cmd"
)

func TestMetaRoundTrip(t *testing.T) {
	const (
		bucket  = "bucket"
		object  = "object"
		numPart = 10
	)

	mm, cleanup := newTestFilemetaUploadMgr(t)
	defer cleanup()

	// init
	uploadID, err := mm.Init(bucket, object)
	if err != nil {
		t.Fatalf("mulipart upload failed: %v", err)
	}

	// add part
	var infos []PartInfo
	for i := 0; i < numPart; i++ {
		partInfo := PartInfo{
			Object: fmt.Sprint(i),
			PartInfo: minio.PartInfo{
				PartNumber:   i,
				LastModified: time.Now(),
				ETag:         fmt.Sprint(i),
				Size:         1,
			},
		}
		err = mm.AddPart(uploadID, i, partInfo)
		if err != nil {
			t.Fatalf("failed to AddPart `%v` : %v", i, err)
		}
		infos = append(infos, partInfo)
	}

	// list part
	parts, err := mm.ListPart(uploadID)
	if err != nil {
		t.Fatalf("failed to ListPart: %v", err)
	}
	if len(parts) != len(infos) {
		t.Fatalf("len of listed Part (%v)  != len of PartInfo (%v)", len(parts), len(infos))
	}

	for i, info := range infos {
		if parts[i].Object != info.Object {
			t.Fatalf("Invalid zstor key for part number: %v", info.PartNumber)
		}
	}

	// clean
	err = mm.Clean(uploadID)
	if err != nil {
		t.Fatalf("cleaning failed: %v", err)
	}

	// check again with list
	parts, err = mm.ListPart(uploadID)
	expectedErr := minio.InvalidUploadID{}
	if err != expectedErr {
		t.Fatalf("Invalid err: %v, expected: %v", err, expectedErr)
	}
}

func newTestFilemetaUploadMgr(t *testing.T) (*filemetaUploadMgr, func()) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("newTestFilemetaUploadMgr failed to create metaDir: %v", err)
	}
	fmu, err := newFilemetaUploadMgr(tmpDir)
	if err != nil {
		t.Fatalf("newTestFilemetaUploadMgr failed to create mgr: %v", err)
	}

	return fmu, func() {
		fmu.Close()
		os.RemoveAll(tmpDir)
	}
}
