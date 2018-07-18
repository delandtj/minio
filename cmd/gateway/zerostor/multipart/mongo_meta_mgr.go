package multipart

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

// MongoMetaMgr defines upload metadata manager backed by mongodb.
type MongoMetaMgr struct {
	ses      *mgo.Session // mongodb main session
	dbName   string       // mongodb DB name
	metaStor meta.Storage
}

type mongoMetaEntry struct {
	ID     string `bson:"_id"`
	Info   Info   `bson:"info"`
	Bucket string `bson:"bucket"`
}

// NewMongoMetaMgr creates meta manager with mongodb backend
func NewMongoMetaMgr(url, dbName string, metaStor meta.Storage) (*MongoMetaMgr, error) {
	ses, err := mgo.Dial(url)
	if err != nil {
		return nil, err
	}

	return &MongoMetaMgr{
		ses:      ses,
		dbName:   dbName,
		metaStor: metaStor,
	}, nil
}

// Init implements MetaManager.Init
func (mmm *MongoMetaMgr) Init(bucket, object string, metadata map[string]string) (Info, error) {
	uploadID := bson.NewObjectId().Hex()
	info := Info{
		MultipartInfo: minio.MultipartInfo{
			UploadID:  uploadID,
			Object:    object,
			Initiated: time.Now(),
		},
		Metadata: metadata,
	}

	// store
	ses, col := mmm.getSesColUpload()
	defer ses.Close()

	err := col.Insert(&mongoMetaEntry{
		ID:     uploadID,
		Info:   info,
		Bucket: bucket,
	})
	return info, err
}

type mongoPartInfo struct {
	ID       string   `bson:"_id"`
	Bucket   string   `bson:"bucket"`
	UploadID string   `bson:"upload_id"`
	PartID   int      `bson:"part_id"`
	Info     PartInfo `bson:"info"`
}

// AddPart implements MetaManager.AddPart
func (mmm *MongoMetaMgr) AddPart(bucket, uploadID string, partID int, info PartInfo) error {
	ses, col := mmm.getSesColPart()
	defer ses.Close()

	return col.Insert(&mongoPartInfo{
		ID:       bson.NewObjectId().Hex(),
		Bucket:   bucket,
		PartID:   partID,
		UploadID: uploadID,
		Info:     info,
	})
}

// DelPart implements MetaManager.DelPart
func (mmm *MongoMetaMgr) DelPart(bucket, uploadID string, etag string, partID int) error {
	ses, col := mmm.getSesColPart()
	defer ses.Close()

	return col.Remove(bson.M{
		"bucket":    bucket,
		"upload_id": uploadID,
		"part_id":   partID,
	})
}

// ListUpload implements MetaManager.ListUpload
func (mmm *MongoMetaMgr) ListUpload(bucket string) ([]Info, error) {
	ses, col := mmm.getSesColUpload()
	defer ses.Close()

	var (
		entries []mongoMetaEntry
		infos   []Info
	)

	err := col.Find(bson.M{"bucket": bucket}).All(&entries)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		infos = append(infos, e.Info)
	}

	return infos, nil
}

// SetZstorMeta implements MetaManager.SetZstorMeta
func (mmm *MongoMetaMgr) SetZstorMeta(md metatypes.Metadata) error {
	rawMd, err := mmm.metaStor.Encode(md)
	if err != nil {
		return err
	}
	return mmm.metaStor.Set(md.Namespace, md.Key, rawMd)
}

// GetMultipart implements MetaManager.GetMultipart
func (mmm *MongoMetaMgr) GetMultipart(bucket, uploadID string) (Info, []PartInfo, error) {
	ses := mmm.getSes()
	defer ses.Close()

	var (
		info  Info
		mme   mongoMetaEntry
		mpi   []mongoPartInfo
		parts []PartInfo
	)

	// get info
	err := mmm.getCol(ses, mongoUploadCol).Find(bson.M{
		"bucket": bucket,
		"_id":    uploadID,
	}).One(&mme)
	if err != nil {
		return info, parts, err
	}
	info = mme.Info

	// get part
	err = mmm.getCol(ses, mongoPartCol).Find(bson.M{
		"bucket":    bucket,
		"upload_id": uploadID,
	}).All(&mpi)
	if err != nil {
		return info, parts, err
	}
	for _, part := range mpi {
		parts = append(parts, part.Info)
	}

	return info, parts, nil
}

// Clean implements MetaManager.Clean
func (mmm *MongoMetaMgr) Clean(bucket, uploadID string) error {
	ses := mmm.getSes()
	defer ses.Close()

	_, err := mmm.getCol(ses, mongoUploadCol).RemoveAll(bson.M{
		"bucket": bucket,
		"_id":    uploadID,
	})
	if err != nil {
		return err
	}

	_, err = mmm.getCol(ses, mongoPartCol).RemoveAll(bson.M{
		"bucket":    bucket,
		"upload_id": uploadID,
	})

	return err
}

func (mmm *MongoMetaMgr) getSes() *mgo.Session {
	return mmm.ses.Copy()
}

func (mmm *MongoMetaMgr) getCol(ses *mgo.Session, colName string) *mgo.Collection {
	return ses.DB(mmm.dbName).C(colName)
}

func (mmm *MongoMetaMgr) getSesColUpload() (*mgo.Session, *mgo.Collection) {
	ses := mmm.getSes()
	col := mmm.getCol(ses, mongoUploadCol)
	return ses, col
}

func (mmm *MongoMetaMgr) getSesColPart() (*mgo.Session, *mgo.Collection) {
	ses := mmm.getSes()
	col := mmm.getCol(ses, mongoPartCol)
	return ses, col
}

func (mmm *MongoMetaMgr) dropDB() {
	mmm.ses.DB(mmm.dbName).DropDatabase()
}

const (
	mongoUploadCol = "multipart_upload"
	mongoPartCol   = "multipart_part"
)

var (
	_ MetaManager = (*MongoMetaMgr)(nil)
)
