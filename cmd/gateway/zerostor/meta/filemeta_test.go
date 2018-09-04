package meta

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/threefoldtech/0-stor/client/metastor/encoding"
)

func TestFilemetaStorRoundTrip(t *testing.T) {
	bucket := "bucket"

	fm, _, cleanup, err := newTestFilemeta(bucket)
	if err != nil {
		t.Fatalf("failed to create test filemeta: %v", err)
	}
	defer cleanup()

	testRoundTrip(t, fm, bucket)
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

	fm, _, cleanup, err := newTestFilemeta(bucket)
	if err != nil {
		t.Fatalf("failed to create test filemeta: %v", err)
	}
	defer cleanup()

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

// Test ListObjects under bucket
// Test ListObjects of a subsubdir
func TestFilemetaStorListObjects(t *testing.T) {
	const (
		bucket = "bucket"
	)
	fm, marshalFuncPair, cleanup, err := newTestFilemeta(bucket)
	if err != nil {
		t.Fatalf("failed to create filemeta: %v", err)
	}
	defer cleanup()

	testMetaStorListObjects(t, fm, marshalFuncPair, bucket)
}

func newTestFilemeta(bucket string) (fm Storage, marshalFuncPair *encoding.MarshalFuncPair, cleanup func(), err error) {
	marshalFuncPair, err = encoding.NewMarshalFuncPair(encoding.MarshalTypeProtobuf)
	if err != nil {
		return
	}

	metaDir, err := ioutil.TempDir("", "")
	if err != nil {
		return
	}

	bktMgr, err := NewDefaultBucketMgr(metaDir)
	if err != nil {
		os.RemoveAll(metaDir)
		return
	}

	fm, err = NewDefaultMetastor(metaDir, marshalFuncPair)
	if err != nil {
		os.RemoveAll(metaDir)
		return
	}

	cleanup = func() {
		fm.Close()
		os.RemoveAll(metaDir)
	}

	err = bktMgr.Create(bucket)
	return
}
