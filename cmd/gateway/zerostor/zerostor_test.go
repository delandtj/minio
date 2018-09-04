package zerostor

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/threefoldtech/0-stor/client/metastor/db"
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

	zstor, _, cleanup, _ := newTestZerostor(t, namespace, bucket)
	defer cleanup()

	// make sure the object is not exist yet
	buf := bytes.NewBuffer(nil)
	err := zstor.ReadRange(bucket, object, buf, 0, dataLen)
	if err != db.ErrNotFound {
		t.Fatalf("expect error: %v, got: %v", db.ErrNotFound, err)
	}
	buf.Reset()

	// set
	_, err = zstor.Write(bucket, object, bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	// get
	err = zstor.ReadRange(bucket, object, buf, 0, dataLen)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !bytes.Equal(data, buf.Bytes()) {
		t.Fatalf("data read is not valid")
	}

	// delete
	err = zstor.Delete(bucket, object)
	if err != nil {
		t.Fatalf("delete data failed error: %v", err)
	}

	// make sure the object is not exist anymore
	err = zstor.ReadRange(bucket, object, buf, 0, dataLen)
	if err != db.ErrNotFound {
		t.Fatalf("expect error: %v, got: %v", db.ErrNotFound, err)
	}

}
func TestParseNsInfo(t *testing.T) {
	testCases := []struct {
		name   string
		nsInfo string
		total  uint64
		used   uint64
	}{
		{
			name:   "max data size is not set",
			nsInfo: "# namespace\nname: thedisk\nentries: 37\npublic: yes\npassword: no\ndata_size_bytes: 141652\ndata_size_mb: 0.14\ndata_limits_bytes: 0\nindex_size_bytes: 1998\nindex_size_kb: 1.95\n",
			total:  defaultNamespaceMaxSize,
			used:   141652,
		},

		{
			name:   "max data size is set",
			nsInfo: "# namespace\nname: thedisk\nentries: 37\npublic: yes\npassword: no\ndata_size_bytes: 141652\ndata_size_mb: 0.14\ndata_limits_bytes: 1234567891234\nindex_size_bytes: 1998\nindex_size_kb: 1.95\n",
			total:  1234567891234,
			used:   141652,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			total, used, err := parseNsInfo(tc.nsInfo)
			if err != nil {
				t.Fatalf("failed to parse ns info:%v", err)
			}

			if used != tc.used {
				t.Fatalf("invalid used value: %v, expected: %v", used, tc.used)
			}
			if total != tc.total {
				t.Fatalf("invalid total:%v, expected: %v", total, tc.total)
			}
		})
	}
}
func newTestZerostor(t *testing.T, namespace, bucket string) (*zerostor, meta.Storage, func(), string) {
	zstorCli, metaStor, bktMgr, cleanup, metaDir := newTestInMemZstorClient(t, namespace)

	err := bktMgr.Create(bucket)
	if err != nil {
		t.Fatalf("failed to create bucket manager: %v", err)
	}

	return &zerostor{
		bktMgr:   bktMgr,
		metaStor: metaStor,
		storCli:  zstorCli,
	}, metaStor, cleanup, metaDir
}
