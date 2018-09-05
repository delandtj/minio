package multipart

import (
	"io"
	"path"
	"testing"

	"github.com/threefoldtech/0-stor/client"
	zdbtest "github.com/threefoldtech/0-stor/client/datastor/zerodb/test"
	"github.com/threefoldtech/0-stor/client/metastor"
	"github.com/threefoldtech/0-stor/client/metastor/db"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

type testZstorClient struct {
	zstorCli *client.Client
	metaCli  *metastor.Client
}

func newTestZstorClient(t *testing.T, metaDB db.DB) (*testZstorClient, func()) {
	const (
		namespace = "namespace1"
	)
	// creates in-memory 0-db server
	shards, serverClean := testZdbServer(t, 4)

	// creates 0-stor client config
	cfg := client.Config{
		Namespace: namespace,
		DataStor:  client.DataStorConfig{Shards: shards},
	}
	cfg.DataStor.Pipeline.BlockSize = 64

	// creates 0-stor metastor client
	zMetaStorCli, err := metastor.NewClient(namespace, metaDB, "")
	if err != nil {
		t.Fatalf("failed to creates 0-stor metastor client:%v", err)
	}

	// creates metadata storage
	cl, err := client.NewClientFromConfig(cfg, zMetaStorCli, 0)
	if err != nil {
		t.Fatalf("failed to creates 0-stor client:%v", err)
	}

	tz := testZstorClient{
		zstorCli: cl,
		metaCli:  zMetaStorCli,
	}

	return &tz, func() {
		cl.Close()
		serverClean()
	}
}

func (tz *testZstorClient) Write(bucket, object string, r io.Reader, userDef map[string]string) (*metatypes.Metadata, error) {
	return tz.zstorCli.Write(tz.getKey(bucket, object), r)
}
func (tz *testZstorClient) Read(bucket, object string, w io.Writer) error {
	md, err := tz.metaCli.GetMetadata(tz.getKey(bucket, object))
	if err != nil {
		return err
	}
	return tz.zstorCli.Read(*md, w)
}

func (tz *testZstorClient) Delete(bucket, object string) error {
	md, err := tz.metaCli.GetMetadata(tz.getKey(bucket, object))
	if err != nil {
		return err
	}
	return tz.zstorCli.Delete(*md)
}

func (tz *testZstorClient) getKey(bucket, object string) []byte {
	return []byte(path.Join(bucket, object))
}

func testZdbServer(t *testing.T, n int) (shards []string, cleanups func()) {
	var (
		namespace    = "ns"
		cleanupFuncs []func()
	)

	for i := 0; i < n; i++ {
		addr, cleanup, err := zdbtest.NewInMem0DBServer(namespace)
		if err != nil {
			t.Fatalf("failed to create zdb server:%v", err)
		}

		cleanupFuncs = append(cleanupFuncs, cleanup)
		shards = append(shards, addr)
	}

	cleanups = func() {
		for _, cleanup := range cleanupFuncs {
			cleanup()
		}
	}
	return
}
