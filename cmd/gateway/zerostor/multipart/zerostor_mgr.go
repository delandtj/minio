package multipart

import (
	"fmt"
	"io"
	"path"

	minio "github.com/minio/minio/cmd"
)

const (
	// MultipartBucket is bucket in 0-stor server used as temporary
	// storage for multipart upload part.
	// Client couldn't create bucket with this name
	MultipartBucket = ".multipart"
)

type zstorMultipartMgr struct {
	mm   MetaManager
	stor Storage
}

// Init implements Manager.Init
func (zm *zstorMultipartMgr) Init(bucket, object string) (string, error) {
	return zm.mm.Init(bucket, object)
}

func (zm *zstorMultipartMgr) UploadPart(bucket, object, uploadID, etag string, partID int, rd io.Reader) (info minio.PartInfo, err error) {
	key := zm.objectName(uploadID, partID)

	// stor the part to 0-stor server as temporary object
	md, err := zm.stor.Write(MultipartBucket, key, rd)
	if err != nil {
		return
	}

	// minio part info
	info = minio.PartInfo{
		PartNumber:   partID,
		LastModified: minio.UTCNow(),
		Size:         md.Size,
		ETag:         etag,
	}

	// meta part info
	metaInfo := PartInfo{
		ZstorKey: key,
		PartInfo: info,
	}
	err = zm.mm.AddPart(uploadID, partID, metaInfo)
	return
}

func (zm *zstorMultipartMgr) Complete(bucket, object, uploadID string, parts []minio.CompletePart) error {
	storedInfos, err := zm.mm.ListPart(uploadID)
	if err != nil {
		return err
	}

	storRd, storWr := io.Pipe()
	errCh := make(chan error, 1)

	go func() {
		defer storWr.Close()

		for _, part := range parts {
			info, ok := storedInfos[part.PartNumber]
			if !ok {
				errCh <- nil
				return
			}

			// get data
			if err := zm.stor.Read(MultipartBucket, info.ZstorKey, storWr, 0, 0); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	// write from the pipe
	_, err = zm.stor.Write(bucket, object, storRd)
	if err != nil {
		return err
	}

	// check error from the reader
	err = <-errCh
	if err != nil {
		return err
	}

	// clean metadata
	return zm.mm.Clean(uploadID)
}

func (zm *zstorMultipartMgr) Abort(bucket, object, uploadID string) error {
	return fmt.Errorf("not implemented")
}

func (zm *zstorMultipartMgr) objectName(uploadID string, partID int) string {
	return path.Join(uploadID) + "/" + fmt.Sprint(partID)
}

var (
	_ Manager = (*zstorMultipartMgr)(nil)
)
