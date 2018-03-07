package zerostor

import (
	"testing"

	"github.com/minio/minio-go/pkg/policy"
)

// Test converting 0-stor permission to minio bucket policy
func TestZerostorPermToBucketPolicy(t *testing.T) {
	testCases := []struct {
		perm nsPerm
		pol  policy.BucketPolicy
	}{
		{
			nsPerm{public: yes, password: yes},
			policy.BucketPolicyReadOnly,
		},
		{
			nsPerm{public: yes, password: no},
			policy.BucketPolicyReadWrite,
		},
		{
			nsPerm{public: no, password: yes},
			policy.BucketPolicyNone,
		},
		{
			nsPerm{public: no, password: no},
			policy.BucketPolicyNone,
		},
	}

	for _, tc := range testCases {
		if tc.perm.toBucketPolicy() != tc.pol {
			t.Errorf("%#v expect `%v` got `%v`", tc.perm, tc.pol, tc.perm.toBucketPolicy())
		}
	}
}

// TestNewNsPerm tests `newNsPerm` which involves parsing 0-db `NSINFO` return value
func TestNewNsPerm(t *testing.T) {
	testCases := []struct {
		infoStr string
		perm    nsPerm
	}{
		{
			"# namespace\nname: thedisk\nentries: 3\npublic: yes\npassword: yes\ndata_size_bytes: 5394\ndata_size_mb: 0.01\ndata_limits_bytes: 0\nindex_size_bytes: 162\nindex_size_kb: 0.16\n",
			nsPerm{public: yes, password: yes},
		},
		{
			"# namespace\nname: thedisk\nentries: 3\npublic: yes\npassword: no\ndata_size_bytes: 5394\ndata_size_mb: 0.01\ndata_limits_bytes: 0\nindex_size_bytes: 162\nindex_size_kb: 0.16\n",
			nsPerm{public: yes, password: no},
		},
		{
			"# namespace\nname: thedisk\nentries: 3\npublic: no\npassword: yes\ndata_size_bytes: 5394\ndata_size_mb: 0.01\ndata_limits_bytes: 0\nindex_size_bytes: 162\nindex_size_kb: 0.16\n",
			nsPerm{public: no, password: yes},
		},
		{
			"# namespace\nname: thedisk\nentries: 3\npublic: no\npassword: no\ndata_size_bytes: 5394\ndata_size_mb: 0.01\ndata_limits_bytes: 0\nindex_size_bytes: 162\nindex_size_kb: 0.16\n",
			nsPerm{public: no, password: no},
		},
	}

	for _, tc := range testCases {
		perm := newNsPerm(tc.infoStr)
		if perm.public != tc.perm.public {
			t.Errorf("expect public=%v got=%v for str=%v", tc.perm.public, perm.public, tc.infoStr)
		}
		if perm.password != tc.perm.password {
			t.Errorf("expect password=%v got=%v for str=%v", tc.perm.password, perm.password, tc.infoStr)
		}
	}
}
