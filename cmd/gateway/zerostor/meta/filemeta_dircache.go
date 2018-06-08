package meta

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru"
)

const (
	rdcCacheSize = 512 // number of dirs to be cached
)

// readDirCache defines cache for ReadDir operation.
//
// It works in these way:
// - if cache not exist or expired, creates new entry by reading the whole dir
// - cache expired if dir's ModTime is different
type readDirCache struct {
	//cache map[string]*dirCache
	cache *lru.Cache
	mux   sync.Mutex
}

func newReadDirCache() (*readDirCache, error) {
	cache, err := lru.New(rdcCacheSize)
	if err != nil {
		return nil, err
	}

	return &readDirCache{
		//cache: make(map[string]*dirCache),
		cache: cache,
	}, nil
}

func (rdc *readDirCache) getCache(absDir string) (*dirCache, bool) {
	val, ok := rdc.cache.Get(absDir)
	if !ok {
		return nil, false
	}

	return val.(*dirCache), true
}

func (rdc *readDirCache) GetNames(absDir string) ([]string, error) {
	rdc.mux.Lock()
	defer rdc.mux.Unlock()

	// open the dir, to get latest info
	fDir, err := os.Open(absDir)
	if err != nil {
		return nil, err
	}

	info, err := fDir.Stat()
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%v is not dir", absDir)
	}

	// get from cache
	dc, ok := rdc.getCache(absDir)
	if ok && dc.modTime.Equal(info.ModTime()) {
		return dc.names, nil
	}

	// update cache
	dCache, err := newDirCache(fDir, info.ModTime())
	if err != nil {
		return nil, err
	}

	rdc.cache.Add(absDir, dCache)

	return dCache.names, nil
}

type dirCache struct {
	modTime time.Time
	names   []string
}

func newDirCache(fDir *os.File, modTime time.Time) (*dirCache, error) {
	names, err := fDir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	sort.Strings(names)

	return &dirCache{
		modTime: modTime,
		names:   names,
	}, nil
}
