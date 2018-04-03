package zerostor

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
)

var (
	// errDifPolicy returned when servers in the cluster
	// doesn't have same policy
	errDifPolicy = errors.New("cluster has different policy")
)

// bucketMgr defines the manager of this zerostor gateway's
// buckets
type bucketMgr struct {
	mux     sync.RWMutex
	buckets map[string]*bucket // in memory bucket objects, for faster access
	dir     string             // directory of the bucket metadata
	objDir  string             // directory of the object metadata
}

// newBucketMgr creates new bucketMgr object
func newBucketMgr(metaDir string) (*bucketMgr, error) {
	var (
		buckets = make(map[string]*bucket)
		dir     = filepath.Join(metaDir, metaBucketDir)
	)

	// initialize bucket dir, if not exist
	if err := os.MkdirAll(dir, rootDirPerm); err != nil {
		return nil, err
	}

	// load existing buckets
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

	// creates zdb clients
	return &bucketMgr{
		buckets: buckets,
		dir:     dir,
		objDir:  filepath.Join(metaDir, metaObjectDir),
	}, nil
}

// creates bucket
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

// get bucket object
func (bm *bucketMgr) get(bucket string) (*bucket, bool) {
	bm.mux.RLock()
	bkt, ok := bm.buckets[bucket]
	bm.mux.RUnlock()
	return bkt, ok
}

// get all buckets
func (bm *bucketMgr) getAllBuckets() []bucket {
	bm.mux.RLock()
	defer bm.mux.RUnlock()

	buckets := make([]bucket, 0, len(bm.buckets))
	for _, bkt := range bm.buckets {
		buckets = append(buckets, *bkt)
	}
	return buckets
}

// del deletes a bucket
func (bm *bucketMgr) del(bucket string) error {
	if err := os.Remove(filepath.Join(bm.dir, bucket)); err != nil {
		return err
	}
	delete(bm.buckets, bucket)
	return nil
}

func (bm *bucketMgr) setPolicy(bucket string, pol policy.BucketPolicy) error {
	bkt, ok := bm.get(bucket)
	if !ok {
		return minio.BucketNotFound{}
	}

	bm.mux.Lock()
	defer bm.mux.Unlock()

	bkt.Policy = pol
	return bkt.save()
}
