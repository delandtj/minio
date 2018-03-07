package zerostor

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/errors"

	"github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/datastor"
	"github.com/zero-os/0-stor/client/datastor/pipeline"
	"github.com/zero-os/0-stor/client/datastor/zerodb"
	"github.com/zero-os/0-stor/client/metastor"
	"github.com/zero-os/0-stor/client/metastor/encoding"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
	"github.com/zero-os/0-stor/client/processing"
)

type zstorClient struct {
	storCli  *client.Client
	metaCli  *metastor.Client
	bktMgr   *bucketMgr
	filemeta *filemeta
}

func newZstorClient(cfg client.Config, metaDir string) (*zstorClient, error) {
	// creates meta client
	fm, metaCli, err := createMestatorClient(cfg.MetaStor, cfg.Namespace, metaDir)
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

	return &zstorClient{
		storCli:  cli,
		metaCli:  metaCli,
		filemeta: fm,
		bktMgr:   fm.bktMgr,
	}, nil
}
func (zc *zstorClient) write(bucket, object string, rd io.Reader) (*metatypes.Metadata, error) {
	if !zc.bucketExist(bucket) {
		return nil, minio.BucketNotFound{}
	}

	key := zc.toZstorKey(bucket, object)
	return zc.storCli.Write(key, rd)
}

func (zc *zstorClient) get(bucket, object string, writer io.Writer, length int64) error {
	if !zc.bucketExist(bucket) {
		return minio.BucketNotFound{}
	}

	key := zc.toZstorKey(bucket, object)

	// get meta
	// we need to get meta first instead of using client.Read directly
	// because 0-stor client still can't support offset & length when reading object
	// https://github.com/zero-os/0-stor/issues/499
	// this will cause issue if object length different than requested length
	md, err := zc.metaCli.GetMetadata(key)
	if err != nil {
		return err
	}

	if length >= 0 && md.Size != length {
		// TODO : return proper error
		log.Println("\t unsupported, object length != requested length")
		return errors.Trace(minio.NotImplemented{})
	}

	return zc.storCli.ReadWithMeta(*md, writer)
}

func (zc *zstorClient) getMeta(bucket, object string) (*metatypes.Metadata, error) {
	return zc.metaCli.GetMetadata(zc.toZstorKey(bucket, object))
}

func (zc *zstorClient) del(bucket, object string) error {
	if !zc.bucketExist(bucket) {
		return minio.BucketNotFound{}
	}
	return zc.storCli.Delete(zc.toZstorKey(bucket, object))
}

func (zc *zstorClient) repair(bucket, object string) (*metatypes.Metadata, error) {
	if !zc.bucketExist(bucket) {
		return nil, minio.BucketNotFound{}
	}
	return zc.storCli.Repair(zc.toZstorKey(bucket, object))
}

func (zc *zstorClient) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	return zc.filemeta.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
}

func (zc *zstorClient) Close() error {
	zc.metaCli.Close()
	zc.storCli.Close()
	return nil
}

func (zc *zstorClient) bucketExist(bucket string) bool {
	_, ok := zc.bktMgr.get(bucket)
	return ok
}

func (zc *zstorClient) toZstorKey(bucket, object string) []byte {
	return []byte(filepath.Join(bucket, object))
}

func createDataClusterFromConfig(cfg *client.Config) (datastor.Cluster, error) {
	// optionally create the global datastor TLS config
	tlsConfig, err := createTLSConfigFromDatastorTLSConfig(&cfg.DataStor.TLS)
	if err != nil {
		return nil, err
	}

	return zerodb.NewCluster(cfg.DataStor.Shards, cfg.Password, cfg.Namespace, tlsConfig)
}

func createTLSConfigFromDatastorTLSConfig(config *client.DataStorTLSConfig) (*tls.Config, error) {
	if config == nil || !config.Enabled {
		return nil, nil
	}
	tlsConfig := &tls.Config{
		MinVersion: config.MinVersion.VersionTLSOrDefault(tls.VersionTLS11),
		MaxVersion: config.MaxVersion.VersionTLSOrDefault(tls.VersionTLS12),
	}

	if config.ServerName != "" {
		tlsConfig.ServerName = config.ServerName
	} else {
		log.Println("TLS is configured to skip verificaitons of certs, " +
			"making the client susceptible to man-in-the-middle attacks!!!")
		tlsConfig.InsecureSkipVerify = true
	}

	if config.RootCA == "" {
		var err error
		tlsConfig.RootCAs, err = x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to create datastor TLS config: %v", err)
		}
	} else {
		tlsConfig.RootCAs = x509.NewCertPool()
		caFile, err := ioutil.ReadFile(config.RootCA)
		if err != nil {
			return nil, err
		}
		if !tlsConfig.RootCAs.AppendCertsFromPEM(caFile) {
			return nil, fmt.Errorf("error reading CA file '%s', while creating datastor TLS config: %v",
				config.RootCA, err)
		}
	}

	return tlsConfig, nil
}

func createMestatorClient(cfg client.MetaStorConfig, namespace, metaDir string) (fm *filemeta, mc *metastor.Client, err error) {
	var metaCfg metastor.Config

	// create the metadata encoding func pair
	metaCfg.MarshalFuncPair, err = encoding.NewMarshalFuncPair(cfg.Encoding)
	if err != nil {
		return
	}

	// create metastor database first,
	// so that then we can create the Metastor client itself
	// TODO: support other types of databases (e.g. badger)
	fm, err = newFilemeta(metaDir, metaCfg.MarshalFuncPair)
	if err != nil {
		return
	}
	metaCfg.Database = fm

	if len(cfg.Encryption.PrivateKey) == 0 {
		// create potentially insecure metastor storage
		mc, err = metastor.NewClient([]byte(namespace), metaCfg)
		fm.metaCli = mc
		return
	}

	// create the constructor which will create our encrypter-decrypter when needed
	metaCfg.ProcessorConstructor = func() (processing.Processor, error) {
		return processing.NewEncrypterDecrypter(
			cfg.Encryption.Type, []byte(cfg.Encryption.PrivateKey))
	}
	// ensure the constructor is valid,
	// as most errors (if not all) are static, and will only fail due to the given input,
	// meaning that if it can be created it now, it should be fine later on as well
	_, err = metaCfg.ProcessorConstructor()
	if err != nil {
		return
	}

	// create our full-metaCfgured metastor client,
	// including encryption support for our metadata in binary form
	mc, err = metastor.NewClient([]byte(namespace), metaCfg)
	fm.metaCli = mc
	return
}
