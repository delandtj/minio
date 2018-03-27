package multipart

import (
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	minio "github.com/minio/minio/cmd"
)

const (
	uploadIDLen = 8
)

var (
	dirPerm  = os.FileMode(0755)
	filePerm = os.FileMode(0644)
)

type filemetaUploadMgr struct {
	rootDir string
}

func newFilemetaUploadMgr(metaDir string) (*filemetaUploadMgr, error) {
	rootDir := filepath.Join(metaDir, "multipart")

	if err := os.MkdirAll(rootDir, dirPerm); err != nil {
		return nil, err
	}

	return &filemetaUploadMgr{
		rootDir: rootDir,
	}, nil
}

// Init implements MetaManager.Init
func (fu *filemetaUploadMgr) Init(bucket, object string) (string, error) {
	// create upload ID
	uploadID, err := fu.createUploadID()
	if err != nil {
		return "", err
	}

	// create the dir
	return uploadID, os.MkdirAll(fu.uploadDir(uploadID), dirPerm)
}

// AddManager implements MetaManager.AddPart
func (fu *filemetaUploadMgr) AddPart(uploadID string, partID int, info PartInfo) error {
	partFile := fu.partFile(uploadID, info.ETag, partID)

	f, err := os.OpenFile(partFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(info)
}

// DelPart implements MetaManager.DelPart
func (fu *filemetaUploadMgr) DelPart(uploadID, etag string, partID int) error {
	return os.Remove(fu.partFile(uploadID, etag, partID))
}

// ListPart implements MetaManager.ListPart
func (fu *filemetaUploadMgr) ListPart(uploadID string) ([]PartInfo, error) {
	// read from the uploadID dir
	fis, err := ioutil.ReadDir(fu.uploadDir(uploadID))
	if err != nil {
		if os.IsNotExist(err) {
			err = minio.InvalidUploadID{}
		}
		return nil, err
	}

	var (
		uploadDir = fu.uploadDir(uploadID)
		infos     = make([]PartInfo, 0, len(fis))
	)

	// read-decode each file
	for _, fi := range fis {
		if fi.IsDir() { // should never happen
			continue
		}
		info, err := fu.decodePart(filepath.Join(uploadDir, fi.Name()))
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}

	sort.Sort(PartInfoSorter(infos))
	return infos, err
}

// Clean implements MetaManager.Clean
func (fu *filemetaUploadMgr) Clean(uploadID string) error {
	return os.RemoveAll(fu.uploadDir(uploadID))
}

// Close implements MetaManager.Close
func (fu *filemetaUploadMgr) Close() error {
	return nil
}

// create unique upload ID
func (fu *filemetaUploadMgr) createUploadID() (string, error) {
	buf := make([]byte, uploadIDLen)

	for i := 0; i < 3; i++ {
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}

		uploadID := fmt.Sprintf("%x", buf)

		if !fu.checkUploadIDExist(uploadID) {
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
func (fu *filemetaUploadMgr) checkUploadIDExist(uploadID string) bool {
	_, err := os.Stat(fu.uploadDir(uploadID))
	return !os.IsNotExist(err)
}

func (fu *filemetaUploadMgr) uploadDir(uploadID string) string {
	return filepath.Join(fu.rootDir, uploadID)
}

func (fu *filemetaUploadMgr) partFile(uploadID, etag string, partID int) string {
	return filepath.Join(fu.uploadDir(uploadID), fmt.Sprintf("%s_%d", etag, partID))
}

var (
	_ MetaManager = (*filemetaUploadMgr)(nil)
)
