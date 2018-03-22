package multipart

import (
	"bytes"
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/zero-os/0-stor/client/datastor"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

// storTest provide simple in memory storage.
// implemented using Go map
type storTest struct {
	mux sync.Mutex
	kv  map[string][]byte
}

func newStorTest() *storTest {
	return &storTest{
		kv: make(map[string][]byte),
	}
}

func (st *storTest) Write(bucket, object string, rd io.Reader) (*metatypes.Metadata, error) {

	data, err := ioutil.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	key := st.key(bucket, object)

	st.mux.Lock()
	defer st.mux.Unlock()

	st.kv[key] = data

	now := time.Now().UnixNano()
	dataSize := int64(len(data))
	return &metatypes.Metadata{
		Key:            []byte(key),
		Size:           dataSize,
		StorageSize:    dataSize,
		CreationEpoch:  now,
		LastWriteEpoch: now,
	}, nil
}

func (st *storTest) Read(bucket, object string, writer io.Writer) error {
	key := st.key(bucket, object)

	st.mux.Lock()
	defer st.mux.Unlock()

	data, ok := st.kv[key]
	if !ok {
		return datastor.ErrKeyNotFound
	}

	_, err := io.Copy(writer, bytes.NewReader(data))
	return err
}

func (st *storTest) Delete(bucket, object string) error {
	key := st.key(bucket, object)

	st.mux.Lock()
	defer st.mux.Unlock()

	_, ok := st.kv[key]
	if !ok {
		return datastor.ErrKeyNotFound
	}
	delete(st.kv, key)
	return nil
}

func (st *storTest) key(bucket, object string) string {
	return bucket + ":" + object
}
