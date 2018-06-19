package zerostor

import (
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/garyburd/redigo/redis"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	//"github.com/minio/minio/cmd/gateway/zerostor/multipart"
	"github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/datastor"
	"github.com/zero-os/0-stor/client/datastor/pipeline"
	"github.com/zero-os/0-stor/client/datastor/zerodb"
	"github.com/zero-os/0-stor/client/metastor"
	"github.com/zero-os/0-stor/client/metastor/db"
	"github.com/zero-os/0-stor/client/metastor/encoding"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

// zerostor defines 0-stor storage
type zerostor struct {
	storCli   zstorClient
	bktMgr    meta.BucketManager
	metaStor  meta.Storage
	zdbShards []string
	namespace string
}

// newZerostor creates new zerostor object
func newZerostor(cfg client.Config, metaDir, metaPrivKey string) (*zerostor, error) {
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("empty namespace")
	}

	// creates bucket manager
	/*bktMgr, err := meta.NewDefaultBucketMgr(metaDir, multipart.MultipartBucket)
	if err != nil {
		return nil, err
	}*/

	// creates meta client
	fm, bktMgr, metaCli, err := createMestatorClient(cfg.Namespace, metaDir, metaPrivKey)
	if err != nil {
		return nil, err
	}

	datastorCluster, err := createDataClusterFromConfig(&cfg)
	if err != nil {
		return nil, err
	}

	// create data pipeline, using our datastor cluster
	dataPipeline, err := pipeline.NewPipeline(cfg.DataStor.Pipeline, datastorCluster, 0)
	if err != nil {
		return nil, err
	}

	// creates 0-stor cli
	cli := client.NewClient(metaCli, dataPipeline)

	return &zerostor{
		storCli:   cli,
		metaStor:  fm,
		bktMgr:    bktMgr,
		zdbShards: cfg.DataStor.Shards,
		namespace: cfg.Namespace,
	}, nil
}

// Write write object from the given reader and metadata
func (zc *zerostor) Write(bucket, object string, rd io.Reader, userDefMeta map[string]string) (*metatypes.Metadata, error) {
	// convert the header key to canonical header key format
	// so we can use it easily when getting the object info
	userDef := make(map[string]string, len(userDefMeta))
	for k, v := range userDefMeta {
		userDef[http.CanonicalHeaderKey(k)] = v
	}

	key := zc.toZstorKey(bucket, object)
	return zc.storCli.WriteWithUserMeta(key, rd, userDef)
}

// Read reads object and write it to the given writer
func (zc *zerostor) Read(bucket, object string, writer io.Writer) error {
	md, err := zc.getMeta(bucket, object)
	if err != nil {
		return err
	}
	return zc.storCli.Read(*md, writer)
}

// ReadRange reads object and write it to the given writer with specified
// offset and length
func (zc *zerostor) ReadRange(bucket, object string, writer io.Writer, offset, length int64) error {
	md, err := zc.getMeta(bucket, object)
	if err != nil {
		return err
	}
	return zc.storCli.ReadRange(*md, writer, offset, length)
}

// getMeta get metadata of the given bucket-object
func (zc *zerostor) getMeta(bucket, object string) (*metatypes.Metadata, error) {
	return zc.metaStor.GetDecodeMeta(zc.toZstorKey(bucket, object))
}

// Delete deletes the object
func (zc *zerostor) Delete(bucket, object string) error {
	if !zc.bucketExist(bucket) {
		return minio.BucketNotFound{}
	}

	// get meta first, because it doesn't
	// return error when the object is not exist
	md, err := zc.getMeta(bucket, object)
	if err != nil {
		if err == db.ErrNotFound {
			return nil
		}
		return err
	}

	if err := zc.storCli.Delete(*md); err == nil {
		return nil
	}

	// if the deletion failed, we still need to cleanup
	// the metadata.
	return zc.metaStor.Delete(nil, zc.toZstorKey(bucket, object))
}

// ListObjects list object in a bucket
func (zc *zerostor) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	return zc.metaStor.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
}

// StorageInfo returns information about current storage
func (zc *zerostor) StorageInfo() (minio.StorageInfo, error) {
	var (
		err   error
		used  uint64
		total uint64
	)

	// iterate all shards, get info from each of it
	// returns immediately once we got an answer
	for _, shard := range zc.zdbShards {
		used, total, err = func() (used, total uint64, err error) {
			// get conn
			conn, err := redis.Dial("tcp", shard)
			if err != nil {
				return
			}
			// request the info
			nsinfo, err := redis.String(conn.Do("NSINFO", zc.namespace))
			if err != nil {
				return
			}
			total, used, err = parseNsInfo(nsinfo)
			return
		}()
		if err == nil {
			return minio.StorageInfo{
				Total: total,
				Free:  total - used,
			}, nil
		}
	}
	return minio.StorageInfo{}, err
}

// Close closes the this 0-stor client
func (zc *zerostor) Close() error {
	return zc.storCli.Close()
}

func parseNsInfo(nsinfo string) (total, used uint64, err error) {
	// parse the info
	for _, line := range strings.Split(nsinfo, "\n") {
		elems := strings.Split(line, ":")
		if len(elems) != 2 {
			continue
		}
		val := strings.TrimSpace(elems[1])
		switch strings.TrimSpace(elems[0]) {
		case "data_size_bytes":
			used, err = strconv.ParseUint(val, 10, 64)
		case "data_limits_bytes":
			total, err = strconv.ParseUint(val, 10, 64)
		}
		if err != nil {
			return
		}
	}
	if total == 0 {
		total = defaultNamespaceMaxSize
	}

	return
}

// bucketExist checks if the given bucket is exist
func (zc *zerostor) bucketExist(bucket string) bool {
	_, err := zc.bktMgr.Get(bucket)
	return err == nil
}

// toZstorKey generates 0-stor key from the given bucket/object
func (zc *zerostor) toZstorKey(bucket, object string) []byte {
	return []byte(filepath.Join(bucket, object))
}

func createDataClusterFromConfig(cfg *client.Config) (datastor.Cluster, error) {
	return zerodb.NewCluster(cfg.DataStor.Shards, cfg.Password, cfg.Namespace, nil)
}

func createMestatorClient(namespace, metaDir, metaPrivKey string) (fm meta.Storage, bktMgr meta.BucketManager, mc *metastor.Client, err error) {
	// create the metadata encoding func pair
	marshalFuncPair, err := encoding.NewMarshalFuncPair(encoding.DefaultMarshalType)
	if err != nil {
		return
	}

	// create metastor database first,
	// so that then we can create the Metastor client itself
	//fm, err = meta.NewDefaultMetastor(metaDir, marshalFuncPair)
	var mms *meta.MongoMetaStor
	mms, err = meta.NewMongoMetaStor("localhost:27017", "kodok", marshalFuncPair)
	if err != nil {
		log.Printf("failed to connect to metastor:%v\n", err)
		return
	}

	// TODO : clean it up
	bktMgr = meta.NewMongoBucketManager(mms)
	fm = mms

	mc, err = metastor.NewClient(namespace, fm, metaPrivKey)
	return
}

const (
	defaultNamespaceMaxSize = 10e14 // default max size =  1PB
)
