package zerostor

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"

	minio "github.com/minio/minio/cmd"
	"github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/datastor"
	"github.com/zero-os/0-stor/client/datastor/pipeline"
	"github.com/zero-os/0-stor/client/datastor/zerodb"
	"github.com/zero-os/0-stor/client/metastor"
	"github.com/zero-os/0-stor/client/metastor/encoding"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
	"github.com/zero-os/0-stor/client/processing"
)

// zerostor defines 0-stor storage
type zerostor struct {
	storCli  zstorClient
	bktMgr   *bucketMgr
	filemeta *filemeta
}

// newZerostor creates new zerostor object
func newZerostor(cfg client.Config, metaDir string) (*zerostor, error) {
	// creates bucket manager
	bktMgr, err := newBucketMgr(metaDir)
	if err != nil {
		return nil, err
	}

	// creates meta client
	fm, metaCli, err := createMestatorClient(cfg.MetaStor, bktMgr, cfg.Namespace, metaDir)
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
		storCli:  cli,
		filemeta: fm,
		bktMgr:   bktMgr,
	}, nil
}

// write writes object from the given reader
func (zc *zerostor) write(bucket, object string, rd io.Reader) (*metatypes.Metadata, error) {
	if !zc.bucketExist(bucket) {
		return nil, minio.BucketNotFound{}
	}

	key := zc.toZstorKey(bucket, object)
	return zc.storCli.Write(key, rd)
}

// get get object and write it to the given writer
func (zc *zerostor) get(bucket, object string, writer io.Writer, offset, length int64) error {
	key := zc.toZstorKey(bucket, object)
	if offset == 0 && length <= 0 {
		debugln("\tGetObject using zerostor Read")
		return zc.storCli.Read(key, writer)
	}
	debugln("\tGetObject using zerostor ReadRange")
	return zc.storCli.ReadRange(key, writer, offset, length)
}

// getMeta get metadata of the given bucket-object
func (zc *zerostor) getMeta(bucket, object string) (*metatypes.Metadata, error) {
	return zc.filemeta.getDecodeMeta(zc.toZstorKey(bucket, object))
}

// del deletes the object
func (zc *zerostor) del(bucket, object string) error {
	if !zc.bucketExist(bucket) {
		return minio.BucketNotFound{}
	}
	return zc.storCli.Delete(zc.toZstorKey(bucket, object))
}

// repair repairs an object
func (zc *zerostor) repair(bucket, object string) (*metatypes.Metadata, error) {
	if !zc.bucketExist(bucket) {
		return nil, minio.BucketNotFound{}
	}
	return zc.storCli.Repair(zc.toZstorKey(bucket, object))
}

// ListObjects list object in a bucket
func (zc *zerostor) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	return zc.filemeta.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
}

// Close closes the this 0-stor client
func (zc *zerostor) Close() error {
	return zc.storCli.Close()
}

// bucketExist checks if the given bucket is exist
func (zc *zerostor) bucketExist(bucket string) bool {
	_, ok := zc.bktMgr.get(bucket)
	return ok
}

// toZstorKey generates 0-stor key from the given bucket/object
func (zc *zerostor) toZstorKey(bucket, object string) []byte {
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

func createMestatorClient(cfg client.MetaStorConfig, bktMgr *bucketMgr, namespace, metaDir string) (fm *filemeta, mc *metastor.Client, err error) {
	var metaCfg metastor.Config

	// create the metadata encoding func pair
	metaCfg.MarshalFuncPair, err = encoding.NewMarshalFuncPair(cfg.Encoding)
	if err != nil {
		return
	}

	// create metastor database first,
	// so that then we can create the Metastor client itself
	// TODO: support other types of databases (e.g. badger)
	fm, err = newFilemeta(metaDir, bktMgr, metaCfg.MarshalFuncPair)
	if err != nil {
		return
	}
	metaCfg.Database = fm

	if len(cfg.Encryption.PrivateKey) == 0 {
		// create potentially insecure metastor storage
		mc, err = metastor.NewClient([]byte(namespace), metaCfg)
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
	return
}
