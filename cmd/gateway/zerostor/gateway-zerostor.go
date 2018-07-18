package zerostor

import (
	"context"
	goerrors "errors"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/minio/cli"
	"github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/minio/minio/cmd/gateway/zerostor/multipart"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"

	"github.com/zero-os/0-stor/client/datastor"
	"github.com/zero-os/0-stor/client/metastor"
)

const (
	zerostorBackend         = "zerostor"
	minioZstorConfigFileVar = "MINIO_ZEROSTOR_CONFIG_FILE"
	minioZstorMetaDirVar    = "MINIO_ZEROSTOR_META_DIR"
	minioZstorMetaPrivKey   = "MINIO_ZEROSTOR_META_PRIVKEY"
	minioZstorDebug         = "MINIO_ZEROSTOR_DEBUG"
)

var (
	errBucketNotFound = goerrors.New("bucket not found")
	errBucketExists   = goerrors.New("bucket already exists")
)

var (
	log       = minio.NewLogger()
	debugFlag = false
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
  ` + minioZstorMetaDirVar + `     Zerostor metadata directory(default : $MINIO_CONFIG_DIR/zerostor_meta)
  ` + minioZstorMetaPrivKey + ` Zerostor metadata private key(default : ""). Metadata won't be encrypted if the key is not provided
  ` + minioZstorDebug + `        Zerostor debug flag. Set to "1" to enable debugging (default : 0)

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

	debugFlag = os.Getenv("MINIO_ZEROSTOR_DEBUG") == "1"
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
		confFile:    confFile,
		metaDir:     metaDir,
		metaPrivKey: os.Getenv(minioZstorMetaPrivKey),
	})
}

// Zerostor implements minio.Gateway interface
type Zerostor struct {
	confFile    string
	metaDir     string
	metaPrivKey string
	zo          *zerostorObjects
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
	log.Println("debugging flag: ", debugFlag)
	log.Println("metadata encrypted: ", g.metaPrivKey != "")

	// creates 0-stor  wrapper
	zstor, err := newZerostor(g.confFile, g.metaDir, g.metaPrivKey)
	if err != nil {
		log.Println("failed to creates zstor client: ", err.Error())
		return nil, err
	}

	// creates multipart upload manager
	mpartMgr, err := newMultipartManager(g.confFile, g.metaDir, zstor, zstor.metaStor)
	if err != nil {
		return nil, err
	}

	zo, err := newGatewayLayer(zstor, mpartMgr)
	if err != nil {
		return nil, err
	}

	go zo.handleConfigReload(g.confFile, g.metaDir, g.metaPrivKey)
	return zo, nil
}

func newGatewayLayer(zstor *zerostor, mpartMgr multipart.Manager) (*zerostorObjects, error) {

	return &zerostorObjects{
		_zstor:        zstor,
		_multipartMgr: mpartMgr,
	}, nil
}

type zerostorObjects struct {
	minio.GatewayUnsupported
	_zstor        *zerostor
	_multipartMgr multipart.Manager
	mux           sync.RWMutex
	debug         bool

	// configuration fields
	metaDir     string
	metaPrivKey string
	confFile    string
}

func (zo *zerostorObjects) handleConfigReload(confFile, metaDir, metaPrivKey string) {
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, syscall.SIGHUP)

	go func() {
		for {
			<-sigCh
			log.Println("Got SIGHUP:reload the config")
			zo.loadConfig(confFile, metaDir, metaPrivKey)
		}
	}()
}

func (zo *zerostorObjects) loadConfig(confFile, metaDir, metaPrivKey string) error {
	// creates 0-stor  wrapper
	zstor, err := newZerostor(confFile, metaDir, metaPrivKey)
	if err != nil {
		log.Println("failed to creates zstor client: ", err.Error())
		return err
	}

	// creates multipart upload manager
	mpartMgr, err := newMultipartManager(confFile, metaDir, zstor, zstor.metaStor)
	if err != nil {
		return err
	}

	zo.mux.Lock()
	zo._zstor = zstor
	zo._multipartMgr = mpartMgr
	zo.mux.Unlock()

	return nil
}

func (zo *zerostorObjects) getZstor() *zerostor {
	zo.mux.RLock()
	zstor := zo._zstor
	zo.mux.RUnlock()

	return zstor
}

func (zo *zerostorObjects) getBktMgr() meta.BucketManager {
	zo.mux.RLock()
	mgr := zo._zstor.bktMgr
	zo.mux.RUnlock()
	return mgr
}

func (zo *zerostorObjects) getMultipartMgr() multipart.Manager {
	zo.mux.RLock()
	mgr := zo._multipartMgr
	zo.mux.RUnlock()

	return mgr
}

func (zo *zerostorObjects) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo minio.BucketInfo, err error) {
	bktMgr := zo.getBktMgr()

	bkt, err := bktMgr.Get(bucket)
	if err != nil {
		return
	}

	bucketInfo.Name = bucket
	bucketInfo.Created = bkt.Created
	return
}

func (zo *zerostorObjects) DeleteBucket(ctx context.Context, bucket string) error {
	log.Println("DeleteBucket ", bucket)
	bktMgr := zo.getBktMgr()

	err := bktMgr.Del(bucket)
	return zstorToObjectErr(errors.Trace(err), bucket)
}

func (zo *zerostorObjects) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	bktMgr := zo.getBktMgr()

	allBuckets, err := bktMgr.GetAllBuckets()
	if err != nil {
		return nil, zstorToObjectErr(errors.Trace(err))
	}

	var buckets []minio.BucketInfo

	for _, bkt := range allBuckets {
		buckets = append(buckets, minio.BucketInfo{
			Name:    bkt.Name,
			Created: bkt.Created,
		})
	}
	return buckets, nil
}

func (zo *zerostorObjects) MakeBucketWithLocation(ctx context.Context, bucket string, location string) error {
	log.Printf("MakeBucketWithLocation bucket=%v, location=%v\n", bucket, location)
	bktMgr := zo.getBktMgr()
	err := bktMgr.Create(bucket)
	return zstorToObjectErr(errors.Trace(err), bucket)
}

// GetBucketPolicy implements minio.ObjectLayer.GetBucketPolicy interface
func (zo *zerostorObjects) GetBucketPolicy(ctx context.Context, bucket string) (policy.BucketAccessPolicy, error) {
	var pol policy.BucketAccessPolicy

	bktMgr := zo.getBktMgr()

	bkt, err := bktMgr.Get(bucket)
	if err != nil {
		return pol, zstorToObjectErr(errors.Trace(err), bucket)
	}

	pol.Statements = policy.SetPolicy(pol.Statements, bkt.Policy, bucket, "")

	return pol, nil
}

// SetBucketPolicy implements minio.ObjectLayer.SetBucketPolicy
func (zo *zerostorObjects) SetBucketPolicy(ctx context.Context, bucket string, policyInfo policy.BucketAccessPolicy) error {

	policies := policy.GetPolicies(policyInfo.Statements, bucket, "")
	// we currently only support one policy per bucket

	if len(policies) != 1 {
		log.Println("SetBucketPolicy unsupported error: setting with multiple policies")
		return errors.Trace(minio.NotImplemented{})
	}

	supportedPrefix := bucket + supportedBucketPolicyPrefix

	pol, ok := policies[supportedPrefix]
	if !ok {
		log.Println("SetBucketPolicy unsupported prefix")
		return errors.Trace(minio.NotImplemented{})
	}

	// save the new policy
	bktMgr := zo.getBktMgr()
	err := bktMgr.SetPolicy(bucket, pol)

	return zstorToObjectErr(errors.Trace(err), bucket)
}

func (zo *zerostorObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	bktMgr := zo.getBktMgr()
	err := bktMgr.SetPolicy(bucket, policy.BucketPolicyNone)
	return zstorToObjectErr(errors.Trace(err), bucket)
}

func (zo *zerostorObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	zstor := zo.getZstor()

	err := zstor.Delete(bucket, object)
	return zstorToObjectErr(errors.Trace(err), bucket, object)
}

func (zo *zerostorObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo) (objInfo minio.ObjectInfo, err error) {

	zstor := zo.getZstor()
	// get meta of src object
	srcMd, err := zstor.getMeta(srcBucket, srcObject)
	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), srcBucket, srcObject)
		return
	}

	// creates pipe
	// - read from  src object
	// - write to dst object
	pr, pw := io.Pipe()
	defer pr.Close()

	go func() {
		defer pw.Close()
		zstor.storCli.Read(*srcMd, pw)
	}()

	dstMd, err := zstor.Write(destBucket, destObject, pr, srcInfo.UserDefined)
	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), destBucket, destObject)
		return
	}

	objInfo = meta.CreateObjectInfo(destBucket, destObject, dstMd)
	return
}

func (zo *zerostorObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64,
	writer io.Writer, etag string) error {
	debugf("GetObject bucket:%v, object:%v, offset:%v, length:%v, etag:%v\n",
		bucket, object, startOffset, length, etag)

	zstor := zo.getZstor()
	// TODO : handle etag

	var err error
	if startOffset == 0 && length <= 0 {
		debugln("\tGetObject using zerostor Read")
		err = zstor.Read(bucket, object, writer)
	} else {
		debugln("\tGetObject using zerostor ReadRange")
		err = zstor.ReadRange(bucket, object, writer, startOffset, length)
	}
	return zstorToObjectErr(errors.Trace(err), bucket, object)
}

func (zo *zerostorObjects) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo minio.ObjectInfo, err error) {
	zstor := zo.getZstor()

	// get meta
	md, err := zstor.getMeta(bucket, object)
	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), bucket, object)
		return
	}

	return meta.CreateObjectInfo(bucket, object, md), nil
}

func (zo *zerostorObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string,
	maxKeys int) (result minio.ListObjectsInfo, err error) {
	debugf("ListObjects bucket:%v, prefix:%v, marker:%v, delimiter:%v, maxKeys:%v\n",
		bucket, prefix, marker, delimiter, maxKeys)

	zstor := zo.getZstor()

	// get objects
	result, err = zstor.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), bucket)
		return
	}

	return
}

// PutObject implements ObjectLayer.PutObject
func (zo *zerostorObjects) PutObject(ctx context.Context, bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {
	debugf("PutObject bucket:%v, object:%v, metadata:%v\n", bucket, object, metadata)
	return zo.putObject(bucket, object, data, metadata, data.MD5HexString())
}

func (zo *zerostorObjects) putObject(bucket, object string, rd io.Reader, metadata map[string]string, etag string) (objInfo minio.ObjectInfo, err error) {
	if etag == "" {
		etag = minio.GenETag()
	}
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata[meta.ETagKey] = etag

	zstor := zo.getZstor()

	// write to 0-stor
	md, err := zstor.Write(bucket, object, rd, metadata)
	if err != nil {
		log.Printf("PutObject bucket:%v, object:%v, failed: %v\n", bucket, object, err)
		err = zstorToObjectErr(errors.Trace(err), bucket, object)
		return
	}

	objInfo = meta.CreateObjectInfo(bucket, object, md)
	return
}

// NewMultipartUpload implements minio.ObjectLayer.NewMultipartUpload
func (zo *zerostorObjects) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string) (uploadID string, err error) {
	multipartMgr := zo.getMultipartMgr()

	uploadID, err = multipartMgr.Init(bucket, object, metadata)

	debugf("NewMultipartUpload bucket:%v, object:%v, uploadID:%v\n", bucket, object, uploadID)

	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), bucket, object)
	}
	return
}

// PutObjectPart implements minio.ObjectLayer.PutObjectPart
func (zo *zerostorObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *hash.Reader) (info minio.PartInfo, err error) {
	return zo.putObjectPart(ctx, bucket, object, uploadID, data.MD5HexString(), partID, data)
}

func (zo *zerostorObjects) putObjectPart(ctx context.Context, bucket, object, uploadID, etag string, partID int, rd io.Reader) (info minio.PartInfo, err error) {
	if etag == "" {
		etag = minio.GenETag()
	}
	multipartMgr := zo.getMultipartMgr()

	info, err = multipartMgr.UploadPart(bucket, object, uploadID, etag, partID, rd)
	if err != nil {
		log.Printf("PutObjectPart id:%v, partID:%v, err: %v\n", uploadID, partID, err)
		err = zstorToObjectErr(errors.Trace(err), bucket, object)
	}
	return
}

// CopyObjectPart implements ObjectLayer.CopyObjectPart
func (zo *zerostorObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo minio.ObjectInfo) (minio.PartInfo, error) {

	zstor := zo.getZstor()
	// get metadata of source object
	storRd, storWr := io.Pipe()
	defer storRd.Close()

	go func() {
		defer storWr.Close()
		if startOffset == 0 && length <= 0 {
			zstor.Read(srcBucket, srcObject, storWr)
		} else {
			zstor.ReadRange(srcBucket, srcObject, storWr, startOffset, length)
		}
	}()

	return zo.putObjectPart(ctx, destBucket, destObject, uploadID, minio.GenETag(), partID, storRd)
}

// CompleteMultipartUpload implements minio.ObjectLayer.CompleteMultipartUpload
func (zo *zerostorObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string,
	parts []minio.CompletePart) (info minio.ObjectInfo, err error) {

	multipartMgr := zo.getMultipartMgr()

	md, err := multipartMgr.Complete(bucket, object, uploadID, parts)
	if err != nil {
		log.Printf("CompleteMultipartUpload for uploadID `%v` failed: %v\n", uploadID, err)
		err = zstorToObjectErr(errors.Trace(err), bucket, object)
		return
	}
	info = meta.CreateObjectInfo(bucket, object, md)
	return
}

// AbortMultipartUpload implements minio.ObjectLayer.AbortMultipartUpload
func (zo *zerostorObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	multipartMgr := zo.getMultipartMgr()
	err := multipartMgr.Abort(bucket, object, uploadID)
	return zstorToObjectErr(errors.Trace(err), bucket, object)
}

// ListMultipartUploads implements ObjectLayer.ListMultipartUploads
// Note: because of lack of docs and example in production ready gateway,
// we don't respect : prefix, keyMarker, uploadIDMarker, delimiter, and maxUploads
func (zo *zerostorObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	multipartMgr := zo.getMultipartMgr()
	result, err = multipartMgr.ListUpload(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		err = zstorToObjectErr(errors.Trace(err), bucket)
	}
	return
}

// ListObjectParts implements ObjectLayer.ListObjectParts
func (zo *zerostorObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (result minio.ListPartsInfo, err error) {
	multipartMgr := zo.getMultipartMgr()
	result, err = multipartMgr.ListParts(bucket, object, uploadID, partNumberMarker, maxParts)
	if err != nil {
		log.Println("ListObjectParts failed: %v", err)
	}
	return
}

// Shutdown implements ObjectLayer.Shutdown
func (zo *zerostorObjects) Shutdown(ctx context.Context) error {
	zstor := zo.getZstor()
	return zstor.Close()
}

// StorageInfo implements ObjectLayer.StorageInfo
func (zo *zerostorObjects) StorageInfo(ctx context.Context) (info minio.StorageInfo) {
	log.Println("StorageInfo")
	zstor := zo.getZstor()
	info, err := zstor.StorageInfo()
	if err != nil {
		log.Printf("StorageInfo failed: %v", err)
	}
	return
}

func debugf(format string, args ...interface{}) {
	if debugFlag {
		log.Printf(format, args...)
	}
}

func debugln(args ...interface{}) {
	if debugFlag {
		log.Println(args...)
	}
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

const (
	supportedBucketPolicyPrefix = "/*"
)
