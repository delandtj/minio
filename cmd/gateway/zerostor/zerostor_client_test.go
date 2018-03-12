package zerostor

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/datastor"
	"github.com/zero-os/0-stor/client/datastor/pipeline/storage"
	"github.com/zero-os/0-stor/client/metastor"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

// in memory zerostor client for testing purpose
// it simply store the data in memory
type inMemZstorClient struct {
	namespace []byte
	metaCli   *metastor.Client
	filemeta  *filemeta
	kv        map[string][]byte
	mux       sync.Mutex
}

func newInMemZstorClient(namespace string, metaCli *metastor.Client, fm *filemeta) *inMemZstorClient {
	return &inMemZstorClient{
		namespace: []byte(namespace),
		metaCli:   metaCli,
		filemeta:  fm,
		kv:        make(map[string][]byte),
	}
}

func (zc *inMemZstorClient) Write(key []byte, r io.Reader) (*metatypes.Metadata, error) {
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
	}

	zc.mux.Lock()
	defer zc.mux.Unlock()

	zc.kv[string(key)] = data

	return &md, zc.metaCli.SetMetadata(md)
}

func (zc *inMemZstorClient) ReadWithMeta(md metatypes.Metadata, w io.Writer) error {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	val, ok := zc.kv[string(md.Key)]
	if !ok {
		return datastor.ErrKeyNotFound
	}

	_, err := io.Copy(w, bytes.NewReader(val))
	return err
}

func (zc *inMemZstorClient) Read(key []byte, w io.Writer) error {
	return zc.readRange(key, w, 0, 0)
}

func (zc *inMemZstorClient) ReadRange(key []byte, w io.Writer, offset, length int64) error {
	return zc.readRange(key, w, offset, length)
}
func (zc *inMemZstorClient) readRange(key []byte, w io.Writer, offset, length int64) error {
	zc.mux.Lock()
	defer zc.mux.Unlock()

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

func (zc *inMemZstorClient) Delete(key []byte) error {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	if _, ok := zc.kv[string(key)]; !ok {
		return datastor.ErrKeyNotFound
	}

	if err := zc.metaCli.DeleteMetadata(key); err != nil {
		return err
	}

	delete(zc.kv, string(key))
	return nil
}

func (zc *inMemZstorClient) CheckWithMeta(md metatypes.Metadata, fast bool) (storage.CheckStatus, error) {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	if _, ok := zc.kv[string(md.Key)]; !ok {
		return storage.CheckStatusInvalid, datastor.ErrKeyNotFound
	}
	return storage.CheckStatusOptimal, nil
}

func (zc *inMemZstorClient) Repair(key []byte) (*metatypes.Metadata, error) {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	_, ok := zc.kv[string(key)]
	if !ok {
		return nil, datastor.ErrKeyNotFound
	}

	return zc.metaCli.GetMetadata(key)
}

func (zc *inMemZstorClient) Close() error {
	return nil
}

func newTestInMemZstorClient(namespace string) (*inMemZstorClient, func(), error) {
	metaDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, nil, err
	}
	bktMgr, err := newBucketMgr(metaDir)
	if err != nil {
		return nil, nil, err
	}

	fm, metaCli, err := createMestatorClient(client.MetaStorConfig{}, bktMgr, namespace, metaDir)
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		os.RemoveAll(metaDir)
	}
	return newInMemZstorClient(namespace, metaCli, fm), cleanup, nil
}
