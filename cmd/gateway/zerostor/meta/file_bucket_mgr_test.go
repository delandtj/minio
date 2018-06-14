package meta

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestFileBucketMgrRoundTrip(t *testing.T) {
	bktMgr, metaDir, err := newTestBucketMgr()
	if err != nil {
		t.Fatalf("failed to create test bucket manager: %v", err)
	}
	defer os.RemoveAll(metaDir)

	constructor := func() (BucketManager, error) {
		return NewDefaultBucketMgr(metaDir)
	}

	testBucketMgrRoundTrip(t, bktMgr, constructor)
}

func TestFileBucketMgrPolicy(t *testing.T) {
	bktMgr, metaDir, err := newTestBucketMgr()
	if err != nil {
		t.Fatalf("failed to create test bucket manager: %v", err)
	}
	defer os.RemoveAll(metaDir)

	constructor := func() (BucketManager, error) {
		return NewDefaultBucketMgr(metaDir)
	}
	testBucketMgrPolicy(t, bktMgr, constructor)
}

func newTestBucketMgr() (bktMgr BucketManager, metaDir string, err error) {
	metaDir, err = ioutil.TempDir("", "")
	if err != nil {
		return
	}

	bktMgr, err = NewDefaultBucketMgr(metaDir)
	if err != nil {
		os.RemoveAll(metaDir)
	}
	return
}
