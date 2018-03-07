package zerostor

import (
	"io/ioutil"
	"os"
	"testing"
)

// Test creating bucket and load it from the disk
func TestBucket(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("failed to create temp dir:%v", err)
	}
	defer os.RemoveAll(tmpDir)

	bucketName := "mybucket"

	// creates bucket
	bkt, err := newBucket(bucketName, tmpDir)
	if err != nil {
		t.Fatalf("failed to create bucket:%v", err)
	}

	// load bucket
	ldBkt, err := newBucketFromFile(tmpDir, bucketName)
	if err != nil {
		t.Fatalf("failed to load bucket from file: %v", err)
	}

	if ldBkt.Created.Unix() != bkt.Created.Unix() {
		t.Fatalf("time doesn't match %v:%v", bkt.Created.Unix(), ldBkt.Created.Unix())
	}
}
