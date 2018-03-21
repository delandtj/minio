package zerostor

import (
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/minio/minio/cmd/gateway/zerostor/multipart"
)

const (
	uploadIDLen = 8
)

type filemetaUploadMgr struct {
	rootDir string
}

func newFilemetaUploadMgr(metaDir string) (*filemetaUploadMgr, error) {
	rootDir := filepath.Join(metaDir, "multipart")

	if err := os.MkdirAll(rootDir, rootDirPerm); err != nil {
		return nil, err
	}

	return &filemetaUploadMgr{
		rootDir: rootDir,
	}, nil
}

func (fu *filemetaUploadMgr) Init(bucket, object string) (string, error) {
	// create upload ID
	uploadID, err := fu.createUploadID()
	if err != nil {
		return "", err
	}

	// create the dir
	return uploadID, os.MkdirAll(fu.uploadDir(uploadID), rootDirPerm)
}

func (fu *filemetaUploadMgr) AddPart(uploadID string, partID int, info multipart.PartInfo) error {
	partFile := fu.partFile(uploadID, partID)

	f, err := os.OpenFile(partFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMetaPerm)
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(info)
}

func (fu *filemetaUploadMgr) ListPart(uploadID string) (map[int]multipart.PartInfo, error) {
	fis, err := ioutil.ReadDir(fu.uploadDir(uploadID))
	if err != nil {
		return nil, err
	}

	uploadDir := fu.uploadDir(uploadID)
	infos := make(map[int]multipart.PartInfo, len(fis))

	for _, fi := range fis {
		if fi.IsDir() { // should never happen
			continue
		}
		info, err := fu.decodePart(filepath.Join(uploadDir, fi.Name()))
		if err != nil {
			return nil, err
		}
		infos[info.PartNumber] = info
	}

	return infos, err
}

func (fu *filemetaUploadMgr) Clean(uploadID string) error {
	return os.RemoveAll(fu.uploadDir(uploadID))
}

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
	return "", multipart.ErrCreateUploadID
}

func (fu *filemetaUploadMgr) decodePart(partFile string) (info multipart.PartInfo, err error) {
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

func (fu *filemetaUploadMgr) checkUploadIDExist(uploadID string) bool {
	_, err := os.Stat(fu.uploadDir(uploadID))
	return !os.IsNotExist(err)
}

func (fu *filemetaUploadMgr) uploadDir(uploadID string) string {
	return filepath.Join(fu.rootDir, uploadID)
}

func (fu *filemetaUploadMgr) partFile(uploadID string, partID int) string {
	return filepath.Join(fu.uploadDir(uploadID), fmt.Sprint(partID))
}

var (
	_ multipart.MetaManager = (*filemetaUploadMgr)(nil)
)
