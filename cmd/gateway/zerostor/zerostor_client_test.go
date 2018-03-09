package zerostor

import (
	"bytes"
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/zero-os/0-stor/client/datastor"
	"github.com/zero-os/0-stor/client/datastor/pipeline/storage"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

// in memory zerostor client for testing purpose
// it simply store the data in memory
type inMemZstorClient struct {
	namespace []byte
	kv        map[string]inMemZstorClientData
	mux       sync.Mutex
}

type inMemZstorClientData struct {
	data []byte
	meta metatypes.Metadata
}

func newInMemZstorClient(namespace string) *inMemZstorClient {
	return &inMemZstorClient{
		namespace: []byte(namespace),
		kv:        make(map[string]inMemZstorClientData),
	}
}

func (zc *inMemZstorClient) Write(key []byte, r io.Reader) (*metatypes.Metadata, error) {
	zc.mux.Lock()
	defer zc.mux.Unlock()

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

	zc.kv[string(key)] = inMemZstorClientData{
		data: data,
		meta: md,
	}
	return &md, nil
}

func (zc *inMemZstorClient) ReadWithMeta(md metatypes.Metadata, w io.Writer) error {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	val, ok := zc.kv[string(md.Key)]
	if !ok {
		return datastor.ErrKeyNotFound
	}

	_, err := io.Copy(w, bytes.NewReader(val.data))
	return err
}

func (zc *inMemZstorClient) ReadRange(key []byte, w io.Writer, offset, length int64) error {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	val, ok := zc.kv[string(key)]
	if !ok {
		return datastor.ErrKeyNotFound
	}

	_, err := io.Copy(w, bytes.NewReader(val.data[int(offset):int(offset+length)]))
	return err
}

func (zc *inMemZstorClient) Delete(key []byte) error {
	zc.mux.Lock()
	defer zc.mux.Unlock()

	if _, ok := zc.kv[string(key)]; !ok {
		return datastor.ErrKeyNotFound
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

	val, ok := zc.kv[string(key)]
	if !ok {
		return nil, datastor.ErrKeyNotFound
	}
	return &val.meta, nil
}

func (zc *inMemZstorClient) Close() error {
	return nil
}
