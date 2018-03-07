package zerostor

import (
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/minio/minio-go/pkg/policy"
)

var (
	redisReadTimeout    = 30 * time.Second
	redisWriteTimeout   = 30 * time.Second
	redisConnectTimeout = 30 * time.Second
)

// zdbClient defines a client for a single 0-db server
type zdbClient struct {
	pool      *redis.Pool
	namespace string
}

// newZdbClient creates new zdbClient object
func newZdbClient(addr, namespace string) (*zdbClient, error) {
	var opts = []redis.DialOption{
		redis.DialReadTimeout(redisReadTimeout),
		redis.DialWriteTimeout(redisWriteTimeout),
		redis.DialConnectTimeout(redisConnectTimeout),
	}

	// creates pool
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr, opts...)
		},
	}

	return &zdbClient{
		pool:      pool,
		namespace: namespace,
	}, nil
}

// nsPerm returns 0-db permission of a namespace
func (zc *zdbClient) nsPerm() (perm nsPerm, err error) {
	conn := zc.pool.Get()
	defer conn.Close()

	infoStr, err := redis.String(conn.Do("NSINFO", zc.namespace))
	if err != nil {
		return
	}

	perm = newNsPerm(infoStr)
	return
}

// nsPerm defines 0-db namespace permission
type nsPerm struct {
	public   string
	password string
}

// newNsPerm creates nsPerm object from 0-db NSINFO response
func newNsPerm(nsInfo string) (perm nsPerm) {
	for _, info := range strings.Split(nsInfo, "\n") {
		elems := strings.Split(info, ":")
		if len(elems) != 2 {
			continue
		}
		val := strings.TrimSpace(elems[1])
		switch strings.TrimSpace(elems[0]) {
		case "public":
			perm.public = val
		case "password":
			perm.password = val
		}
	}
	return
}

// toBucketPolicy convert 0-db namespace permission
// to minio BucketPolicy
func (np nsPerm) toBucketPolicy() policy.BucketPolicy {
	if np.public == yes {
		if np.password == yes {
			return policy.BucketPolicyReadOnly
		}
		return policy.BucketPolicyReadWrite
	}
	return policy.BucketPolicyNone
}

const (
	yes = "yes"
	no  = "no"
)
