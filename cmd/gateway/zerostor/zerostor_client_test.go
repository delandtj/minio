package zerostor

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/zero-os/0-stor/client/datastor"
	"github.com/zero-os/0-stor/client/datastor/pipeline/storage"
	"github.com/zero-os/0-stor/client/metastor"
	"github.com/zero-os/0-stor/client/metastor/encoding"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

// in memory zerostor client for testing purpose
// it simply store the data in memory
type inMemZstorClient struct {
	namespace []byte
	metaCli   *metastor.Client
	filemeta  meta.Storage
	kv        map[string][]byte
	mux       sync.Mutex
}

func newInMemZstorClient(namespace string, metaCli *metastor.Client, fm meta.Storage) *inMemZstorClient {
	return &inMemZstorClient{
		namespace: []byte(namespace),
		metaCli:   metaCli,
		filemeta:  fm,
		kv:        make(map[string][]byte),
	}
}

func (zc *inMemZstorClient) WriteWithUserMeta(key []byte, r io.Reader, userMeta map[string]string) (*metatypes.Metadata, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	epoch := time.Now().Unix()
	md := metatypes.Metadata{
		Namespace:      zc.namespace,
		Key:            key,
		Size:           int64(len(data)),
		StorageSize:    int64(len(data)),
		CreationEpoch:  epoch,
		LastWriteEpoch: epoch,
		UserDefined:    userMeta,
	}

	zc.mux.Lock()
	defer zc.mux.Unlock()

	zc.kv[string(key)] = data

	return &md, zc.metaCli.SetMetadata(md)
}

func (zc *inMemZstorClient) Read(md metatypes.Metadata, w io.Writer) error {
	return zc.readRange(md, w, 0, 0)
}

func (zc *inMemZstorClient) ReadRange(md metatypes.Metadata, w io.Writer, offset, length int64) error {
	return zc.readRange(md, w, offset, length)
}
func (zc *inMemZstorClient) readRange(md metatypes.Metadata, w io.Writer, offset, length int64) error {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	key := md.Key
	val, ok := zc.kv[string(key)]
	if !ok {
		return datastor.ErrKeyNotFound
	}

	var err error
	if length != 0 {
		_, err = io.Copy(w, bytes.NewReader(val[int(offset):int(offset+length)]))
	} else {
		_, err = io.Copy(w, bytes.NewReader(val))
	}
	return err
}

func (zc *inMemZstorClient) Delete(md metatypes.Metadata) error {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	key := md.Key
	if _, ok := zc.kv[string(key)]; !ok {
		return datastor.ErrKeyNotFound
	}

	if err := zc.metaCli.DeleteMetadata(key); err != nil {
		return err
	}

	delete(zc.kv, string(key))
	return nil
}

func (zc *inMemZstorClient) Check(md metatypes.Metadata, fast bool) (storage.CheckStatus, error) {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	if _, ok := zc.kv[string(md.Key)]; !ok {
		return storage.CheckStatusInvalid, datastor.ErrKeyNotFound
	}
	return storage.CheckStatusOptimal, nil
}

func (zc *inMemZstorClient) Repair(md metatypes.Metadata) (*metatypes.Metadata, error) {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	key := md.Key
	_, ok := zc.kv[string(key)]
	if !ok {
		return nil, datastor.ErrKeyNotFound
	}

	return zc.metaCli.GetMetadata(key)
}

func (zc *inMemZstorClient) Close() error {
	return nil
}

func newTestInMemZstorClient(namespace string) (*inMemZstorClient, meta.Storage, meta.BucketManager, func(), string, error) {
	metaDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, nil, nil, nil, "", err
	}

	bktMgr, err := meta.NewDefaultBucketMgr(metaDir)
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	// create the metadata encoding func pair
	marshalFuncPair, err := encoding.NewMarshalFuncPair(encoding.DefaultMarshalType)
	if err != nil {
		return nil, nil, nil, nil, "", err
	}

	// create metastor database first,
	// so that then we can create the Metastor client itself
	fm, err := meta.NewDefaultMetastor(metaDir, marshalFuncPair)
	if err != nil {
		return nil, nil, nil, nil, "", err
	}

	metaCli, err := metastor.NewClient(namespace, fm, "")
	if err != nil {
		return nil, nil, nil, nil, "", err
	}

	cleanup := func() {
		os.RemoveAll(metaDir)
	}
	return newInMemZstorClient(namespace, metaCli, fm), fm, bktMgr, cleanup, metaDir, nil
}
