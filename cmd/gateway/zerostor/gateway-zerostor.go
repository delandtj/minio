package zerostor

import (
	goerrors "errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"

	"github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/datastor"
	"github.com/zero-os/0-stor/client/metastor"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

const (
	zerostorBackend         = "zerostor"
	minioZstorConfigFileVar = "MINIO_ZEROSTOR_CONFIG_FILE"
	minioZstorMetaDirVar    = "MINIO_ZEROSTOR_META_DIR"
)

var (
	errBucketNotFound = goerrors.New("bucket not found")
	errBucketExists   = goerrors.New("bucket already exists")
)

var (
	log = minio.NewLogger()
)

func init() {
	const zerostorGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Access key of 0-stor storage.
     MINIO_SECRET_KEY: Secret key of 0-stor storage.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  UPDATE:
     MINIO_UPDATE: To turn off in-place upgrades, set this value to "off".

  ` + minioZstorConfigFileVar + `  Zerostor config file(default : $MINIO_CONFIG_DIR/zerostor.yaml)
  ` + minioZstorMetaDirVar + `  Zerostor metadata directory(default : $MINIO_CONFIG_DIR/zerostor_meta)

EXAMPLES:
  1. Start minio gateway server for 0-stor Storage backend.
      $ export MINIO_ACCESS_KEY=zerostoraccountname
      $ export MINIO_SECRET_KEY=zerostoraccountkey
      $ {{.HelpName}}

`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               zerostorBackend,
		Usage:              "zero-os 0-stor.",
		Action:             zerostorGatewayMain,
		CustomHelpTemplate: zerostorGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway zerostor' command line.
func zerostorGatewayMain(ctx *cli.Context) {
	// config file
	confFile := os.Getenv(minioZstorConfigFileVar)
	if confFile == "" {
		confFile = filepath.Join(ctx.String("config-dir"), "zerostor.yaml")
	}

	// meta dir
	metaDir := os.Getenv(minioZstorMetaDirVar)
	if metaDir == "" {
		metaDir = filepath.Join(ctx.String("config-dir"), "zerostor_meta")
	}

	minio.StartGateway(ctx, &Zerostor{
		confFile: confFile,
		metaDir:  metaDir,
	})
}

// Zerostor implements minio.Gateway interface
type Zerostor struct {
	confFile string
	metaDir  string
}

// Name implements minio.Gateway.Name interface
func (g *Zerostor) Name() string {
	return zerostorBackend
}

// Production implements minio.Gateway.Production interface
func (g *Zerostor) Production() bool {
	return false
}

// NewGatewayLayer implements minio.Gateway.NewGatewayLayer interface
func (g *Zerostor) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	// check options
	log.Println("zerostor config file = ", g.confFile)

	// read zerostor config
	storCfg, err := client.ReadConfig(g.confFile)
	if err != nil {
		return nil, err
	}

	// creates 0-stor  wrapper
	zstor, err := newZerostor(*storCfg, g.metaDir)
	if err != nil {
		log.Println("new zstor client: ", err.Error())
		return nil, err
	}

	return &zerostorObjects{
		zstor:  zstor,
		bktMgr: zstor.bktMgr,
	}, nil
}

type zerostorObjects struct {
	minio.GatewayUnsupported
	mux    sync.RWMutex
	zstor  *zerostor
	bktMgr *bucketMgr
}

func (zo *zerostorObjects) GetBucketInfo(bucket string) (bucketInfo minio.BucketInfo, err error) {
	bkt, err := zo.getBucket(bucket)
	if err != nil {
		return
	}

	bucketInfo.Name = bucket
	bucketInfo.Created = bkt.Created
	return
}

func (zo *zerostorObjects) DeleteBucket(bucket string) error {
	log.Println("DeleteBucket ", bucket)
	err := zo.bktMgr.del(bucket)
	return zstorToObjectErr(errors.Trace(err), bucket)
}

func (zo *zerostorObjects) ListBuckets() (buckets []minio.BucketInfo, err error) {
	for _, bkt := range zo.getAllBuckets() {
		buckets = append(buckets, minio.BucketInfo{
			Name:    bkt.Name,
			Created: bkt.Created,
		})
	}
	return
}

func (zo *zerostorObjects) MakeBucketWithLocation(bucket string, location string) error {
	log.Printf("MakeBucketWithLocation bucket=%v, location=%v\n", bucket, location)

	_, err := zo.getBucket(bucket)
	if err == nil {
		return zstorToObjectErr(errors.Trace(errBucketExists), bucket)
	}

	err = zo.bktMgr.createBucket(bucket)
	return zstorToObjectErr(errors.Trace(err), bucket)
}

// GetBucketPolicy implements minio.ObjectLayer.GetBucketPolicy interface
func (zo *zerostorObjects) GetBucketPolicy(bucket string) (policy.BucketAccessPolicy, error) {
	var pol policy.BucketAccessPolicy

	bkt, err := zo.getBucket(bucket)
	if err != nil {
		return pol, zstorToObjectErr(errors.Trace(err), bucket)
	}

	pol.Statements = policy.SetPolicy(pol.Statements, bkt.Policy, bucket, "")

	return pol, nil
}

// SetBucketPolicy implements minio.ObjectLayer.SetBucketPolicy
func (zo *zerostorObjects) SetBucketPolicy(bucket string, policyInfo policy.BucketAccessPolicy) error {
	policies := policy.GetPolicies(policyInfo.Statements, bucket)
	// we currently only support one policy per bucket
	supportedPrefix := bucket + "/*"
	if len(policies) != 1 {
		log.Println("SetBucketPolicy unsupported error: setting with multiple policies")
		return errors.Trace(minio.NotImplemented{})
	}
	pol, ok := policies[supportedPrefix]
	if !ok {
		log.Println("SetBucketPolicy unsupported prefix")
		return errors.Trace(minio.NotImplemented{})
	}

	// save the new policy
	err := zo.bktMgr.setPolicy(bucket, pol)

	return zstorToObjectErr(errors.Trace(err), bucket)
}

func (zo *zerostorObjects) DeleteBucketPolicy(bucket string) error {
	err := zo.bktMgr.setPolicy(bucket, policy.BucketPolicyNone)
	return zstorToObjectErr(errors.Trace(err), bucket)
}

func (zo *zerostorObjects) DeleteObject(bucket, object string) error {
	err := zo.zstor.del(bucket, object)
	return zstorToObjectErr(errors.Trace(err), bucket, object)
}

func (zo *zerostorObjects) CopyObject(srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo) (objInfo minio.ObjectInfo, err error) {

	// get meta of src object
	srcMd, err := zo.zstor.getMeta(srcBucket, srcObject)
	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), srcBucket, srcObject)
		return
	}

	// check dest bucket
	_, err = zo.getBucket(destBucket)
	if err != nil {
		return
	}

	// creates pipe
	// - read from  src object
	// - write to dst object
	pr, pw := io.Pipe()
	defer pr.Close()

	go func() {
		defer pw.Close()
		zo.zstor.storCli.ReadWithMeta(*srcMd, pw)
	}()

	dstMd, err := zo.zstor.write(destBucket, destObject, pr)
	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), destBucket, destObject)
		return
	}

	objInfo = createObjectInfo(destBucket, destObject, dstMd)
	return
}

func (zo *zerostorObjects) GetObject(bucket, object string, startOffset int64, length int64,
	writer io.Writer, etag string) error {
	// TODO : handle etag
	err := zo.zstor.get(bucket, object, writer, startOffset, length)
	return zstorToObjectErr(errors.Trace(err), bucket, object)
}

func (zo *zerostorObjects) GetObjectInfo(bucket, object string) (objInfo minio.ObjectInfo, err error) {
	// get meta
	md, err := zo.zstor.getMeta(bucket, object)
	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), bucket, object)
		return
	}

	return createObjectInfo(bucket, object, md), nil
}

func (zo *zerostorObjects) ListObjects(bucket, prefix, marker, delimiter string,
	maxKeys int) (result minio.ListObjectsInfo, err error) {
	//log.Printf("ListObjects bucket=%v, prefix=%v, marker=%v, delimiter=%v, maxKeys=%v\n",
	//	bucket, prefix, marker, delimiter, maxKeys)

	// get objects
	result, err = zo.zstor.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), bucket)
		return
	}

	return
}

func (zo *zerostorObjects) PutObject(bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {
	// write to 0-stor
	md, err := zo.zstor.write(bucket, object, data)
	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), bucket, object)
		return
	}

	objInfo = createObjectInfo(bucket, object, md)
	return
}

func (zo *zerostorObjects) HealObject(bucket, object string, dryRun bool) (madmin.HealResultItem, error) {
	log.Printf("healObject %v/%v dryRun=%v\n", bucket, object, dryRun)

	md, err := zo.zstor.getMeta(bucket, object)
	if err != nil {
		return madmin.HealResultItem{}, zstorToObjectErr(errors.Trace(err), bucket, object)
	}

	res := madmin.HealResultItem{
		Bucket: bucket,
		Object: object,
		Type:   madmin.HealItemObject,
		//ObjectSize : get from the metadata
		//DiskCount: len(md.Chunks[0].Objects),
	}

	if dryRun {
		_, err = zo.zstor.storCli.CheckWithMeta(*md, false)
		if err != nil {
			return res, zstorToObjectErr(errors.Trace(err), bucket, object)
		}
		return res, nil
	}

	_, err = zo.zstor.repair(bucket, object)
	if err != nil {
		return res, zstorToObjectErr(errors.Trace(err), bucket, object)
	}
	return res, nil

}

func (zo *zerostorObjects) Shutdown() error {
	return zo.zstor.Close()
}

func (zo *zerostorObjects) StorageInfo() (info minio.StorageInfo) {
	log.Println("StorageInfo")
	return
}

func createObjectInfo(bucket, object string, md *metatypes.Metadata) minio.ObjectInfo {
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		Size:    md.Size, // TODO : returns the actual size
		ModTime: zstorEpochToTimestamp(md.LastWriteEpoch),
		// TODO:
		// ETag:"",
		// ContentType:
		// UserDefined
	}
}

func (zo *zerostorObjects) getBucket(name string) (*bucket, error) {
	bkt, ok := zo.bktMgr.get(name)
	if !ok {
		return nil, minio.BucketNotFound{}
	}
	return bkt, nil
}

func (zo *zerostorObjects) getAllBuckets() []bucket {
	return zo.bktMgr.getAllBuckets()
}

// convert 0-stor error to minio error
func zstorToObjectErr(err error, params ...string) error {
	if err == nil {
		return nil
	}

	e, ok := err.(*errors.Error)
	if !ok {
		// Code should be fixed if this function is called without doing errors.Trace()
		// Else handling different situations in this function makes this function complicated.
		minio.ErrorIf(err, "Expected type *Error")
		return err
	}

	err = e.Cause

	var (
		bucket string
		object string
	)

	if len(params) >= 1 {
		bucket = params[0]
	}

	if len(params) == 2 {
		object = params[1]
	}

	switch err {
	case metastor.ErrNotFound, datastor.ErrMissingKey, datastor.ErrMissingData, datastor.ErrKeyNotFound:
		err = minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	case errBucketNotFound:
		err = minio.BucketNotFound{
			Bucket: bucket,
		}
	case errBucketExists:
		err = minio.BucketExists{
			Bucket: bucket,
		}

	default:
		return e
	}
	e.Cause = err
	return e
}

// convert zerostor epoch time to Go timestamp
func zstorEpochToTimestamp(epoch int64) time.Time {
	return time.Unix(epoch/1e9, epoch%1e9)
}
