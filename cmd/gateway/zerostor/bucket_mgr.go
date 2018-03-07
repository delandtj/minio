package zerostor

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	minio "github.com/minio/minio/cmd"
)

type bucketMgr struct {
	mux     sync.RWMutex
	buckets map[string]*bucket
	dir     string
	objDir  string
}

func newBucketMgr(dir, objDir string) (*bucketMgr, error) {

	buckets := make(map[string]*bucket)

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		bkt, err := newBucketFromFile(dir, f.Name())
		if err != nil {
			return nil, err
		}
		buckets[bkt.Name] = bkt
	}

	return &bucketMgr{
		buckets: buckets,
		dir:     dir,
		objDir:  objDir,
	}, nil
}

func (bm *bucketMgr) createBucket(bucket string) error {
	bm.mux.Lock()
	defer bm.mux.Unlock()

	if _, ok := bm.buckets[bucket]; ok {
		return minio.BucketExists{}
	}

	// create bucket in objdir
	if err := os.MkdirAll(filepath.Join(bm.objDir, bucket), rootDirPerm); err != nil {
		return err
	}

	// creates the actual bucket
	bkt, err := newBucket(bucket, bm.dir)
	if err != nil {
		return err
	}

	bm.buckets[bucket] = bkt
	return nil
}

func (bm *bucketMgr) get(bucket string) (*bucket, bool) {
	bm.mux.RLock()
	bkt, ok := bm.buckets[bucket]
	bm.mux.RUnlock()
	return bkt, ok
}

func (bm *bucketMgr) getAllBuckets() []bucket {
	bm.mux.RLock()
	defer bm.mux.RUnlock()

	buckets := make([]bucket, 0, len(bm.buckets))
	for _, bkt := range bm.buckets {
		buckets = append(buckets, *bkt)
	}
	return buckets
}

func (bm *bucketMgr) del(bucket string) error {
	if err := os.Remove(filepath.Join(bm.dir, bucket)); err != nil {
		return err
	}
	delete(bm.buckets, bucket)
	return nil
}
