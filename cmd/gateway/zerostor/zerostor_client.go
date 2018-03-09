package zerostor

import (
	"io"

	"github.com/zero-os/0-stor/client/datastor/pipeline/storage"
	"github.com/zero-os/0-stor/client/metastor/metatypes"
)

// zstorClient define 0-stor client
type zstorClient interface {
	Write(key []byte, r io.Reader) (*metatypes.Metadata, error)
	ReadWithMeta(meta metatypes.Metadata, w io.Writer) error
	ReadRange(key []byte, w io.Writer, offset, length int64) error
	Delete(key []byte) error
	CheckWithMeta(meta metatypes.Metadata, fast bool) (storage.CheckStatus, error)
	Repair(key []byte) (*metatypes.Metadata, error)
	Close() error
}
