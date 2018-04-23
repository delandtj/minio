package meta

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
)

const (
	bucketDir = "buckets"
)

// bucketMgr implements bucketManager interface using
// file based implementation
type bucketMgr struct {
	mux            sync.RWMutex
	buckets        map[string]*bucket // in memory bucket objects, for faster access
	dir            string             // directory of the bucket metadata
	specialBuckets []string           // special buckets, which should be hidden
}

// NewDefaultBucketMgr creates default BucketManager implementation.
// The implementation is using file based storage
func NewDefaultBucketMgr(metaDir string, specialBuckets ...string) (BucketManager, error) {
	dir := filepath.Join(metaDir, bucketDir)
	// initialize bucket dir, if not exist
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}

	// load existing buckets
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var (
		buckets = make(map[string]*bucket)
		bkt     *bucket
	)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		bkt, err = newBucketFromFile(dir, f.Name())
		if err != nil {
			return nil, err
		}
		buckets[bkt.Name] = bkt
	}

	// creates zdb clients
	mgr := &bucketMgr{
		buckets:        buckets,
		dir:            dir,
		specialBuckets: specialBuckets,
	}

	// creates special bucket
	for _, bucket := range specialBuckets {
		if _, err = mgr.Get(bucket); err == nil {
			continue
		}

		err = mgr.Create(bucket)
		if err != nil {
			return nil, err
		}
	}

	// create special buckets
	return mgr, nil
}

// Create implements bucketManager.Create interface
func (bm *bucketMgr) Create(bucket string) error {
	bm.mux.Lock()
	defer bm.mux.Unlock()

	if _, ok := bm.buckets[bucket]; ok {
		return minio.BucketExists{}
	}

	// creates the actual bucket
	bkt, err := newBucket(bucket, bm.dir)
	if err != nil {
		return err
	}

	bm.buckets[bucket] = bkt
	return nil
}

// Get implements bucketManager.Get interface
func (bm *bucketMgr) Get(bucket string) (*Bucket, error) {
	bkt, ok := bm.get(bucket)
	if !ok {
		return nil, minio.BucketNotFound{}
	}
	return &bkt.Bucket, nil
}
func (bm *bucketMgr) get(bucket string) (*bucket, bool) {
	bm.mux.RLock()
	bkt, ok := bm.buckets[bucket]
	bm.mux.RUnlock()
	return bkt, ok
}

// GetAllBuckets implements bucketManager.GetAllBuckets interface
func (bm *bucketMgr) GetAllBuckets() ([]Bucket, error) {
	bm.mux.RLock()
	defer bm.mux.RUnlock()

	buckets := make([]Bucket, 0, len(bm.buckets))
	for _, bkt := range bm.buckets {
		if !bm.isSpecialBucket(bkt.Name) {
			buckets = append(buckets, bkt.Bucket)
		}
	}
	return buckets, nil
}

// del deletes a bucket
func (bm *bucketMgr) Del(bucket string) error {
	if err := os.Remove(filepath.Join(bm.dir, bucket)); err != nil {
		return err
	}
	delete(bm.buckets, bucket)
	return nil
}

// SetPolicy implements bucketManager.SetPolicy interface
func (bm *bucketMgr) SetPolicy(bucket string, pol policy.BucketPolicy) error {
	bkt, ok := bm.get(bucket)
	if !ok {
		return minio.BucketNotFound{}
	}

	bm.mux.Lock()
	defer bm.mux.Unlock()

	bkt.Policy = pol
	return bkt.save()
}

func (bm *bucketMgr) isSpecialBucket(bucket string) bool {
	for _, b := range bm.specialBuckets {
		if b == bucket {
			return true
		}
	}
	return false
}

var (
	_ BucketManager = (*bucketMgr)(nil)
)
