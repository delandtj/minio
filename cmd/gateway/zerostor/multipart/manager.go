package multipart

import (
	"fmt"
	"io"
	"path"

	minio "github.com/minio/minio/cmd"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

const (
	// MultipartBucket is bucket in 0-stor server used as temporary
	// storage for multipart upload part.
	// Client couldn't create bucket with this name
	MultipartBucket = ".multipart"
)

type manager struct {
	metaMgr MetaManager
	stor    Storage
}

func newManager(stor Storage, metaMgr MetaManager) *manager {
	return &manager{
		stor:    stor,
		metaMgr: metaMgr,
	}
}

// Init implements Manager.Init
func (m *manager) Init(bucket, object string, metadata map[string]string) (string, error) {
	info, err := m.metaMgr.Init(bucket, object, metadata)
	if err != nil {
		return "", err
	}
	return info.UploadID, nil
}

// UploadPart implements Manager.UploadPart
func (m *manager) UploadPart(bucket, object, uploadID, etag string, partID int, rd io.Reader) (info minio.PartInfo, err error) {
	if etag == "" {
		err = minio.InvalidETag{}
		return
	}
	partObject := m.objectName(uploadID, partID)

	// stor the part to 0-stor server as temporary object
	md, err := m.stor.Write(MultipartBucket, partObject, rd, nil)
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
		Object:   partObject,
		PartInfo: info,
	}
	err = m.metaMgr.AddPart(bucket, uploadID, partID, metaInfo)
	return
}

// Complete implements Manager.Complete
func (m *manager) Complete(bucket, object, uploadID string, parts []minio.CompletePart) (*metatypes.Metadata, error) {
	// get all  part info of this upload ID
	multipartInfo, storedPartInfos, err := m.getStoredInfo(bucket, uploadID)
	if err != nil {
		return nil, err
	}

	var (
		// pipe to connect `reader` that download from temporary
		// storage to `writer` that upload to permanent storage
		storRd, storWr = io.Pipe()
		errCh          = make(chan error, 1)
	)

	// read the data one by one and stream it to the writer
	go func() {
		defer storWr.Close()

		for _, part := range parts {
			info, ok := storedPartInfos[part.ETag]
			if !ok {
				errCh <- minio.InvalidPart{}
				return
			}

			// get data
			if err := m.stor.Read(MultipartBucket, info.Object, storWr); err != nil {
				errCh <- err
				return
			}

			// delete the temporary object
			if err := m.stor.Delete(MultipartBucket, info.Object); err != nil {
				// we don't return error here because we don't return error
				// on failed deletion.
				// Other tool should do garbage storage cleanup
				continue
			}
			delete(storedPartInfos, part.ETag)

			// we don't check the error here because
			// we want to keep delete next part.
			// another tools should do metadata cleanup
			m.metaMgr.DelPart(bucket, uploadID, part.ETag, part.PartNumber)
		}
		errCh <- nil
	}()

	// write from the pipe
	md, err := m.stor.Write(bucket, object, storRd, multipartInfo.Metadata)
	if err != nil {
		return nil, err
	}

	// check error from the reader
	err = <-errCh
	if err != nil {
		return nil, err
	}

	// delete undeleted part
	for _, info := range storedPartInfos {
		if err = m.stor.Delete(MultipartBucket, info.Object); err == nil {
			// we don't return error here because we don't return error
			// on failed deletion.
			// Other tool should do garbage storage cleanup

			m.metaMgr.DelPart(bucket, uploadID, info.ETag, info.PartNumber)
			// we don't check the error here because
			// we want to keep delete next part.
			// another tools should do metadata cleanup
		}
	}

	// clean metadata
	return md, m.metaMgr.Clean(bucket, uploadID)
}

// Abort implements Manager.Abort
func (m *manager) Abort(bucket, object, uploadID string) error {
	_, parts, err := m.metaMgr.GetMultipart(bucket, uploadID)
	if err != nil {
		return err
	}

	for _, part := range parts {
		if err := m.stor.Delete(MultipartBucket, part.Object); err != nil {
			// we don't return error because we want to delete next part.
			// another tool shoudl do storage cleanup
			continue
		}
		// we don't check the error here because
		// we want to keep delete next part.
		// another tools should do metadata cleanup
		m.metaMgr.DelPart(bucket, uploadID, part.ETag, part.PartNumber)
	}
	return m.metaMgr.Clean(bucket, uploadID)
}

// ListUpload implements Manager.ListUpload
func (m *manager) ListUpload(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	metaUploads, err := m.metaMgr.ListUpload(bucket)
	if err != nil {
		return
	}

	uploads := make([]minio.MultipartInfo, 0, len(metaUploads))
	for _, up := range metaUploads {
		uploads = append(uploads, up.MultipartInfo)
	}
	result = minio.ListMultipartsInfo{
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		Uploads:        uploads,
	}
	return
}

// ListParts implements Manager.ListParts
func (m *manager) ListParts(bucket, object, uploadID string, partMarker, maxParts int) (minio.ListPartsInfo, error) {
	_, parts, err := m.metaMgr.GetMultipart(bucket, uploadID)
	if err != nil {
		return minio.ListPartsInfo{}, err
	}

	infos := make([]minio.PartInfo, 0, len(parts))
	for _, part := range parts {
		infos = append(infos, part.PartInfo)
	}

	return minio.ListPartsInfo{
		Bucket:           bucket,
		Object:           object,
		UploadID:         uploadID,
		MaxParts:         len(parts),
		PartNumberMarker: partMarker,
		Parts:            infos,
	}, nil
}

// Close implements Manager.Close
func (m *manager) Close() error {
	return nil
}

func (m *manager) getStoredInfo(bucket, uploadID string) (MultipartInfo, map[string]PartInfo, error) {
	mi, infoArr, err := m.metaMgr.GetMultipart(bucket, uploadID)
	if err != nil {
		return mi, nil, err
	}
	infos := make(map[string]PartInfo, len(infoArr))
	for _, info := range infoArr {
		infos[info.ETag] = info
	}
	return mi, infos, nil
}

func (m *manager) objectName(uploadID string, partID int) string {
	return path.Join(uploadID) + "/" + fmt.Sprint(partID)
}

var (
	_ Manager = (*manager)(nil)
)
