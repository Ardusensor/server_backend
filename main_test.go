package main

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *C) {
	redisPool = getRedisPool(*redisHost)
}

func (s *TestSuite) TearDownSuite(c *C) {
	redisPool.Close()
}

func (s *TestSuite) TestProcessExample(c *C) {
	b, err := ioutil.ReadFile(filepath.Join("testdata", "example.json"))
	c.Assert(err, Equals, nil)
	u, err := handleJSONUpload(bytes.NewBuffer(b))
	c.Assert(err, Equals, nil)
	c.Assert(len(u.ticks), Equals, 20)
}
