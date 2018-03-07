package zerostor

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
	"golang.org/x/sync/errgroup"
)

var (
	// errDifPolicy returned when servers in the cluster
	// doesn't have same policy
	errDifPolicy = errors.New("cluster has different policy")
)

// bucketMgr defines the manager of this zerostor gateway's
// buckets
type bucketMgr struct {
	mux        sync.RWMutex
	buckets    map[string]*bucket // in memory bucket objects, for faster access
	dir        string             // directory of the bucket metadata
	objDir     string             // directory of the object metadata
	zdbClients []*zdbClient
}

// newBucketMgr creates new bucketMgr object
func newBucketMgr(metaDir string, shards []string, namespace string) (*bucketMgr, error) {
	var (
		buckets    = make(map[string]*bucket)
		zdbClients []*zdbClient
		dir        = filepath.Join(metaDir, metaBucketDir)
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
	for _, shard := range shards {
		zc, err := newZdbClient(shard, namespace)
		if err != nil {
			return nil, err
		}
		zdbClients = append(zdbClients, zc)
	}

	return &bucketMgr{
		buckets:    buckets,
		dir:        dir,
		objDir:     filepath.Join(metaDir, metaObjectDir),
		zdbClients: zdbClients,
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

// get bucket policy
// Because 0-stor doesn't have `bucket` notion,
// getting bucket policy is translated into getting
// policy or access right of the 0-db namespace
func (bm *bucketMgr) getBucketPolicy() (policy.BucketPolicy, error) {
	var (
		pol   policy.BucketPolicy
		group errgroup.Group
		perms = make([]nsPerm, len(bm.zdbClients))
	)

	for i, zc := range bm.zdbClients {
		i := i
		group.Go(func() error {
			perm, err := zc.nsPerm()
			if err != nil {
				return err
			}
			perms[i] = perm
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return pol, err
	}

	// make sure all namespaces has same policy
	pol = perms[0].toBucketPolicy()

	for i := 1; i < len(perms); i++ {
		curPol := perms[i].toBucketPolicy()
		if curPol != pol {
			return pol, errDifPolicy
		}
	}
	return pol, nil
}
