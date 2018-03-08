package zerostor

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/zero-os/0-stor/client/metastor/db"
	"github.com/zero-os/0-stor/client/metastor/encoding"
)

func TestRoundTrip(t *testing.T) {
	var (
		bucket    = "bucket"
		namespace = []byte("ns")
		key       = []byte(filepath.Join(bucket, "foo"))
		data      = []byte("bar")
	)

	fm, cleanup, err := newTestFilemeta()
	if err != nil {
		t.Fatalf("failed to create test filemeta: %v", err)
	}
	defer cleanup()
	fm.bktMgr.createBucket("bucket")

	// ensure metadata is not there yet
	_, err = fm.Get(namespace, key)
	if err != db.ErrNotFound {
		t.Errorf("unexpected error: %v, expected: %v", err, db.ErrNotFound)
	}

	// set the metadata
	err = fm.Set(namespace, key, data)
	if err != nil {
		t.Errorf("unexpected error when set meta: %v", err)
	}

	// get it back
	storedData, err := fm.Get(namespace, key)
	if err != nil {
		t.Errorf("unexpected error when set meta: %v", err)
	}
	// check stored value
	if storedData == nil {
		t.Errorf("storedData shouldn't be nil")
	}
	if !bytes.Equal(data, storedData) {
		t.Errorf("stored data is different")
	}

	// delete the metadata
	err = fm.Delete(namespace, key)
	if err != nil {
		t.Errorf("failed to delete meta: %v", err)
	}

	// make sure we can't get it back
	_, err = fm.Get(namespace, key)
	if err != db.ErrNotFound {
		t.Errorf("unexpected error when getting deleted meta: %v, expected: %v", err, db.ErrNotFound)
	}
}

// Test that filemeta could handle dir properly
func TestFilemetaHandleDir(t *testing.T) {
	const (
		bucket = "bucket"
		dir    = "dir"
	)
	var (
		namespace = []byte("ns")
		metadata  = []byte("*****")
	)

	fm, cleanup, err := newTestFilemeta()
	if err != nil {
		t.Fatalf("failed to create test filemeta: %v", err)
	}
	defer cleanup()
	fm.bktMgr.createBucket(bucket)

	// set meta with key that contains dir
	err = fm.Set(namespace, []byte(filepath.Join(bucket, dir, "filename")), metadata)
	if err != nil {
		t.Errorf("failed to set meta: %v", err)
	}

	_, err = fm.Get(namespace, []byte(filepath.Join(bucket, dir)))
	if err != nil {
		t.Errorf("failed to get meta: %v", err)
	}
}

func newTestFilemeta() (fm *filemeta, cleanup func(), err error) {
	marshalFuncPair, err := encoding.NewMarshalFuncPair(encoding.MarshalTypeProtobuf)
	if err != nil {
		return
	}

	metaDir, err := ioutil.TempDir("", "")
	if err != nil {
		return
	}

	bktMgr, err := newBucketMgr(metaDir)
	if err != nil {
		os.RemoveAll(metaDir)
		return
	}

	fm, err = newFilemeta(metaDir, bktMgr, marshalFuncPair)
	if err != nil {
		os.RemoveAll(metaDir)
		return
	}

	cleanup = func() {
		fm.Close()
		os.RemoveAll(metaDir)
	}
	return
}
