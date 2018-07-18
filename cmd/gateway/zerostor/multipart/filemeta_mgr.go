package multipart

import (
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

const (
	uploadIDLen    = 8
	uploadMetaFile = ".meta" // metadata of an upload
)

var (
	dirPerm  = os.FileMode(0755)
	filePerm = os.FileMode(0644)
)

// filemetaUploadMgr defines filebased metadata manager that implements
// MetaManager interface
type filemetaUploadMgr struct {
	rootDir  string
	metaStor meta.Storage
}

func newFilemetaUploadMgr(metaDir string, metaStor meta.Storage) (*filemetaUploadMgr, error) {
	rootDir := filepath.Join(metaDir, "multipart")

	if err := os.MkdirAll(rootDir, dirPerm); err != nil {
		return nil, err
	}

	return &filemetaUploadMgr{
		rootDir:  rootDir,
		metaStor: metaStor,
	}, nil
}

// Init implements MetaManager.Init
func (fu *filemetaUploadMgr) Init(bucket, object string, metadata map[string]string) (info Info, err error) {
	// create upload ID
	uploadID, err := fu.createUploadID(bucket)
	if err != nil {
		return
	}

	info = Info{
		MultipartInfo: minio.MultipartInfo{
			UploadID:  uploadID,
			Object:    object,
			Initiated: time.Now(),
		},
		Metadata: metadata,
	}

	// creates the dir
	uploadDir := fu.uploadDir(bucket, uploadID)
	err = os.MkdirAll(uploadDir, dirPerm)
	if err != nil {
		return
	}

	// creates meta file
	uploadMetaFile := filepath.Join(uploadDir, uploadMetaFile)
	f, err := os.OpenFile(uploadMetaFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return
	}
	defer f.Close()

	err = gob.NewEncoder(f).Encode(info)
	return
}

// AddManager implements MetaManager.AddPart
func (fu *filemetaUploadMgr) AddPart(bucket, uploadID string, partID int, info PartInfo) error {
	partFile := fu.partFile(bucket, uploadID, info.ETag, partID)

	f, err := os.OpenFile(partFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(info)
}

// DelPart implements MetaManager.DelPart
func (fu *filemetaUploadMgr) DelPart(bucket, uploadID, etag string, partID int) error {
	return os.Remove(fu.partFile(bucket, uploadID, etag, partID))
}

// ListUpload implements MetaManager.ListUpload
func (fu *filemetaUploadMgr) ListUpload(bucket string) ([]Info, error) {
	// get upload list
	fileInfos, err := ioutil.ReadDir(filepath.Join(fu.rootDir, bucket))
	if err != nil {
		return nil, err
	}

	uploads := make([]Info, 0, len(fileInfos))

	// get metadata of each upload
	for _, fi := range fileInfos {
		if !fi.IsDir() {
			continue
		}
		upload, err := fu.loadUpload(bucket, fi.Name())
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, upload)
	}

	// sort by ID
	sort.Slice(uploads, func(i, j int) bool {
		return uploads[i].UploadID < uploads[j].UploadID
	})

	return uploads, nil
}

func (fu *filemetaUploadMgr) SetZstorMeta(md metatypes.Metadata) error {
	rawMd, err := fu.metaStor.Encode(md)
	if err != nil {
		return err
	}
	return fu.metaStor.Set(md.Namespace, md.Key, rawMd)
}

// GetMultipart implements MetaManager.ListPart
func (fu *filemetaUploadMgr) GetMultipart(bucket, uploadID string) (Info, []PartInfo, error) {
	mi, err := fu.loadUpload(bucket, uploadID)
	if err != nil {
		return mi, nil, err
	}

	// read from the uploadID dir
	uploadDir := fu.uploadDir(bucket, uploadID)

	fis, err := ioutil.ReadDir(uploadDir)
	if err != nil {
		if os.IsNotExist(err) {
			err = minio.InvalidUploadID{}
		}
		return mi, nil, err
	}

	var (
		infos []PartInfo
		info  PartInfo
	)

	// read-decode each file
	for _, fi := range fis {
		if fi.IsDir() { // should never happen
			continue
		}
		if fi.Name() == uploadMetaFile {
			continue
		}

		info, err = fu.decodePart(filepath.Join(uploadDir, fi.Name()))
		if err != nil {
			return mi, nil, err
		}
		infos = append(infos, info)
	}

	sort.Slice(infos, func(i, j int) bool {
		if infos[i].PartNumber != infos[j].PartNumber { // sort by part number first
			return infos[i].PartNumber < infos[j].PartNumber
		}
		return infos[i].LastModified.Before(infos[j].LastModified)

	})

	return mi, infos, err
}

// Clean implements MetaManager.Clean
func (fu *filemetaUploadMgr) Clean(bucket, uploadID string) error {
	return os.RemoveAll(fu.uploadDir(bucket, uploadID))
}

// Close implements MetaManager.Close
func (fu *filemetaUploadMgr) Close() error {
	return nil
}

// create unique upload ID
func (fu *filemetaUploadMgr) createUploadID(bucket string) (string, error) {
	buf := make([]byte, uploadIDLen)

	for i := 0; i < 3; i++ {
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}

		uploadID := fmt.Sprintf("%x", buf)

		if !fu.checkUploadIDExist(bucket, uploadID) {
			return uploadID, nil
		}
	}
	return "", ErrCreateUploadID
}

// decode part info from the given part file
func (fu *filemetaUploadMgr) decodePart(partFile string) (info PartInfo, err error) {
	// open file
	f, err := os.Open(partFile)
	if err != nil {
		return
	}
	defer f.Close()

	// decode
	err = gob.NewDecoder(f).Decode(&info)

	return
}

// check whether the given upload ID already exist in the system
func (fu *filemetaUploadMgr) checkUploadIDExist(bucket, uploadID string) bool {
	_, err := os.Stat(fu.uploadDir(bucket, uploadID))
	return !os.IsNotExist(err)
}

func (fu *filemetaUploadMgr) uploadDir(bucket, uploadID string) string {
	return filepath.Join(fu.rootDir, bucket, uploadID)
}

func (fu *filemetaUploadMgr) partFile(bucket, uploadID, etag string, partID int) string {
	return filepath.Join(fu.uploadDir(bucket, uploadID), fmt.Sprintf("%s_%d", etag, partID))
}

func (fu *filemetaUploadMgr) loadUpload(bucket, uploadID string) (up Info, err error) {
	// open upload's meta file
	filename := filepath.Join(fu.uploadDir(bucket, uploadID), uploadMetaFile)
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			err = minio.InvalidUploadID{}
		}
		return
	}
	defer f.Close()

	err = gob.NewDecoder(f).Decode(&up)
	return
}

var (
	_ MetaManager = (*filemetaUploadMgr)(nil)
)
