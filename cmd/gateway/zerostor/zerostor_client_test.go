package zerostor

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/zero-os/0-stor/client"
	zdbtest "github.com/zero-os/0-stor/client/datastor/zerodb/test"
	"github.com/zero-os/0-stor/client/metastor"
	"github.com/zero-os/0-stor/client/metastor/encoding"
)

func newTestInMemZstorClient(t *testing.T, namespace string) (zstorClient, meta.Storage, meta.BucketManager, func(), string) {
	metaDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	bktMgr, err := meta.NewDefaultBucketMgr(metaDir)
	if err != nil {
		t.Fatalf("failed to create bucket manager:%v", err)
	}
	// create the metadata encoding func pair
	marshalFuncPair, err := encoding.NewMarshalFuncPair(encoding.DefaultMarshalType)
	if err != nil {
		t.Fatalf("failed to create marshaller: %v", err)
	}

	// create metastor database first,
	// so that then we can create the Metastor client itself
	fm, err := meta.NewDefaultMetastor(metaDir, marshalFuncPair)
	if err != nil {
		t.Fatalf("failed to create metastor: %v", err)
	}

	metaCli, err := metastor.NewClient(namespace, fm, "")
	if err != nil {
		t.Fatalf("failed to create metastor: %v", err)
	}

	zStorCli, zStorCleanup := newTestZstorClient(t, namespace, metaCli)
	cleanup := func() {
		os.RemoveAll(metaDir)
		zStorCleanup()
	}
	return zStorCli, fm, bktMgr, cleanup, metaDir
}

func newTestZstorClient(t *testing.T, namespace string, metaCli *metastor.Client) (zstorClient, func()) {
	// creates in-memory 0-db server
	shards, serverClean := testZdbServer(t, 4)

	// creates 0-stor client config
	cfg := client.Config{
		Namespace: namespace,
		DataStor:  client.DataStorConfig{Shards: shards},
	}
	cfg.DataStor.Pipeline.BlockSize = 64

	// creates metadata storage
	client, err := client.NewClientFromConfig(cfg, metaCli, 0)
	if err != nil {
		t.Fatalf("failed to creates 0-stor client:%v", err)
	}

	return client, func() {
		client.Close()
		serverClean()
	}
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
