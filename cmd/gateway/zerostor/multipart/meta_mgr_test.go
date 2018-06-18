package multipart

import (
	"fmt"
	"sort"
	"testing"
	"time"

	minio "github.com/minio/minio/cmd"
)

func testMetaRoundTrip(t *testing.T, mm MetaManager) {
	const (
		bucket  = "bucket"
		object  = "object"
		numPart = 10
		key1    = "Key1"
		val1    = "val1"
	)

	metadata := map[string]string{
		key1: val1,
	}

	mm, cleanup := newTestFilemetaUploadMgr(t)
	defer cleanup()

	// init
	uploadInfo, err := mm.Init(bucket, object, metadata)
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

	// get multipart
	multipartInfo, parts, err := mm.GetMultipart(bucket, uploadID)
	if err != nil {
		t.Fatalf("failed to ListPart: %v", err)
	}
	if multipartInfo.Metadata[key1] != val1 {
		t.Fatalf("invalid metadata value of %v: %v, expected: %v", key1, multipartInfo.Metadata[key1], val1)
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
	err = checkUploads([]Info{uploadInfo}, uploads)
	if err != nil {
		t.Fatalf("invalid listed uploads: %v", err)
	}

	// clean
	err = mm.Clean(bucket, uploadID)
	if err != nil {
		t.Fatalf("cleaning failed: %v", err)
	}

	// check again with GetMultipart
	_, _, err = mm.GetMultipart(bucket, uploadID)
	expectedErr := minio.InvalidUploadID{}
	if err != expectedErr {
		t.Fatalf("Invalid err: %v, expected: %v", err, expectedErr)
	}
}

func testMetaListUploads(t *testing.T, mm MetaManager) {
	const (
		bucket     = "bucket"
		object     = "object"
		numUploads = 10
		numPart    = 10
	)

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

func checkUploads(want, have []Info) error {
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
