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

func TestMetaRoundTrip(t *testing.T) {
	const (
		bucket  = "bucket"
		object  = "object"
		numPart = 10
	)

	mm, cleanup := newTestFilemetaUploadMgr(t)
	defer cleanup()

	// init
	uploadInfo, err := mm.Init(bucket, object)
	if err != nil {
		t.Fatalf("mulipart upload failed: %v", err)
	}
	uploadID := uploadInfo.UploadID

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
		err = mm.AddPart(bucket, uploadID, i, partInfo)
		if err != nil {
			t.Fatalf("failed to AddPart `%v` : %v", i, err)
		}
		infos = append(infos, partInfo)
	}

	// list part
	parts, err := mm.ListPart(bucket, uploadID)
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

	// list upload
	uploads, err := mm.ListUpload(bucket)
	if err != nil {
		t.Fatalf("ListUpload failed: %v", err)
	}
	if len(uploads) != 1 {
		t.Fatalf("invalid number of unfinished upload: %v, expected: %v", len(uploads), 1)
	}

	// check the listed uploads
	err = checkUploads([]minio.MultipartInfo{uploadInfo}, uploads)
	if err != nil {
		t.Fatalf("invalid listed uploads: %v", err)
	}

	// clean
	err = mm.Clean(bucket, uploadID)
	if err != nil {
		t.Fatalf("cleaning failed: %v", err)
	}

	// check again with list
	parts, err = mm.ListPart(bucket, uploadID)
	expectedErr := minio.InvalidUploadID{}
	if err != expectedErr {
		t.Fatalf("Invalid err: %v, expected: %v", err, expectedErr)
	}
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
	var uploads []minio.MultipartInfo
	for i := 0; i < numUploads; i++ {
		// init
		upload, err := mm.Init(bucket, object)
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

func checkUploads(want, have []minio.MultipartInfo) error {
	if len(want) != len(have) {
		return fmt.Errorf("invalid length: %v, want: %v", len(have), len(want))
	}
	for i, haveUp := range have {
		if haveUp.UploadID != want[i].UploadID {
			return fmt.Errorf("invalid ID for element `%v` : %v, want: %v", i, haveUp.UploadID, want[i].UploadID)
		}
		if haveUp.Object != want[i].Object {
			return fmt.Errorf("invalid object for element `%v` : %v, want: %v", i, haveUp.Object, want[i].Object)
		}
	}
	return nil
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
