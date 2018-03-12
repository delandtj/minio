package zerostor

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/zero-os/0-stor/client/datastor"
)

// Test roundtrip: get set delete
func TestZerostorRoundTrip(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "buket"
		object    = "myKey"
		dataLen   = 4096
	)
	var (
		data = make([]byte, dataLen)
	)
	rand.Read(data)

	zstor, cleanup, err := newTestZerostor(namespace, bucket)
	if err != nil {
		t.Fatalf("failed to create test zerostor: %v", err)
	}
	defer cleanup()

	// make sure the object is not exist yet
	buf := bytes.NewBuffer(nil)
	err = zstor.get(bucket, object, buf, 0, dataLen)
	if err != datastor.ErrKeyNotFound {
		t.Fatalf("expect error: %v, got: %v", datastor.ErrKeyNotFound, err)
	}
	buf.Reset()

	// set
	_, err = zstor.write(bucket, object, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	// get
	err = zstor.get(bucket, object, buf, 0, dataLen)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !bytes.Equal(data, buf.Bytes()) {
		t.Fatalf("data read is not valid")
	}

	// delete
	err = zstor.del(bucket, object)
	if err != nil {
		t.Fatalf("delete data failed error: %v", err)
	}

	// make sure the object is not exist anymore
	err = zstor.get(bucket, object, buf, 0, dataLen)
	if err != datastor.ErrKeyNotFound {
		t.Fatalf("expect error: %v, got: %v", datastor.ErrKeyNotFound, err)
	}

}

func newTestZerostor(namespace, bucket string) (*zerostor, func(), error) {
	zstorCli, cleanup, err := newTestInMemZstorClient(namespace)
	if err != nil {
		return nil, nil, err
	}

	err = zstorCli.filemeta.bktMgr.createBucket(bucket)
	if err != nil {
		return nil, nil, err
	}

	return &zerostor{
		bktMgr:   zstorCli.filemeta.bktMgr,
		filemeta: zstorCli.filemeta,
		storCli:  zstorCli,
	}, cleanup, nil
}
