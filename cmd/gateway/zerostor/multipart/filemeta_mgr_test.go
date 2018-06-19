package multipart

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"testing"
	"time"

	minio "github.com/minio/minio/cmd"
)

func TestFileMetaRoundTrip(t *testing.T) {
	mm, cleanup := newTestFilemetaUploadMgr(t)
	defer cleanup()

	testMetaRoundTrip(t, mm)
}

func TestFileMetaListUploads(t *testing.T) {
	mm, cleanup := newTestFilemetaUploadMgr(t)
	defer cleanup()

	testMetaListUploads(t, mm)
}

func TestListUploads(t *testing.T) {
	const (
		bucket     = "bucket"
		object     = "object"
		numUploads = 10
		numPart    = 10
	)

	mm, cleanup := newTestFilemetaUploadMgr(t)
	defer cleanup()

	// do upload
	var uploads []Info
	for i := 0; i < numUploads; i++ {
		// init
		upload, err := mm.Init(bucket, object, nil)
		if err != nil {
			t.Fatalf("mulipart upload failed: %v", err)
		}

		// add parts
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
			err = mm.AddPart(bucket, upload.UploadID, i, partInfo)
			if err != nil {
				t.Fatalf("failed to AddPart `%v` : %v", i, err)
			}
		}

		uploads = append(uploads, upload)
	}
	sort.Slice(uploads, func(i, j int) bool {
		return uploads[i].UploadID < uploads[j].UploadID
	})

	// list upload
	listedUploads, err := mm.ListUpload(bucket)
	if err != nil {
		t.Fatalf("ListUpload failed: %v", err)
	}
	if len(uploads) != numUploads {
		t.Fatalf("invalid number of unfinished upload: %v, expected: %v", len(uploads), numUploads)
	}

	// check the listed uploads
	err = checkUploads(uploads, listedUploads)
	if err != nil {
		t.Fatalf("invalid listed uploads: %v", err)
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
