package main

import (
	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *C) {
	*environment = "test"
	rand.Seed(time.Now().UTC().UnixNano())
	redisPool = getRedisPool(*redisHost)
}

func (s *TestSuite) TearDownSuite(c *C) {
	redisPool.Close()
	redisPool = nil
}

func (s *TestSuite) TestParseEntry(c *C) {
	entry, err := NewEntry("<2012-12-26 12:46:5;75942;60;3158;5632;1584;144>")
	c.Assert(err, IsNil)
	c.Assert(entry, Not(IsNil))
	c.Assert(fmt.Sprintf("%d", entry.SensorID), Equals, "75942")
	c.Assert(entry.NextDataSession, Equals, "60")
	c.Assert(entry.BatteryVoltage, Equals, "3158")
	c.Assert(entry.Sensor1, Equals, "5632")
	c.Assert(entry.Sensor2, Equals, "1584")
	c.Assert(entry.RadioQuality, Equals, "144")
}

func (s *TestSuite) TestParseDateTime(c *C) {
	entry, err := NewEntry("<2012-12-26 12:46:5;75942;60;3158;5632;1584;144>")
	c.Assert(err, IsNil)
	c.Assert(entry, Not(IsNil))
	c.Assert(entry.Datetime.Year(), Equals, 2012)
	c.Assert(entry.Datetime.Month(), Equals, time.December)
	c.Assert(entry.Datetime.Day(), Equals, 26)
	c.Assert(entry.Datetime.Hour(), Equals, 12)
	c.Assert(entry.Datetime.Minute(), Equals, 46)
	c.Assert(entry.Datetime.Second(), Equals, 5)
	name, _ := entry.Datetime.Zone()
	c.Assert(name, Equals, "UTC")
}

func (s *TestSuite) TestParseDateTimeMonthAndDayNotPadded(c *C) {
	entry, err := NewEntry("<2012-2-5 12:46:5;75942;60;3158;5632;1584;144>")
	c.Assert(err, IsNil)
	c.Assert(entry, Not(IsNil))
	c.Assert(entry.Datetime.Year(), Equals, 2012)
	c.Assert(entry.Datetime.Month(), Equals, time.February)
	c.Assert(entry.Datetime.Day(), Equals, 5)
	c.Assert(entry.Datetime.Hour(), Equals, 12)
	c.Assert(entry.Datetime.Minute(), Equals, 46)
	c.Assert(entry.Datetime.Second(), Equals, 5)
	name, _ := entry.Datetime.Zone()
	c.Assert(name, Equals, "UTC")
}

func (s *TestSuite) TestParseDateTimeWithPaddedSeconds(c *C) {
	entry, err := NewEntry("<2012-12-26 12:46:05;75942;60;3158;5632;1584;144>")
	c.Assert(err, IsNil)
	c.Assert(entry, Not(IsNil))
	c.Assert(entry.Datetime.Second(), Equals, 5)
}

func (s *TestSuite) TestParseDateTimeSeconds(c *C) {
	entry, err := NewEntry("<2012-12-26 12:46:35;75942;60;3158;5632;1584;144>")
	c.Assert(err, IsNil)
	c.Assert(entry, Not(IsNil))
	c.Assert(entry.Datetime.Second(), Equals, 35)
}

func (s *TestSuite) TestParseDateTimeMinutes(c *C) {
	entry, err := NewEntry("<2012-12-26 13:2:36;75942;10;3202;6784;1580;150>")
	c.Assert(err, IsNil)
	c.Assert(entry, Not(IsNil))
	c.Assert(entry.Datetime.Minute(), Equals, 2)
}

func (s *TestSuite) TestProcessEntries(c *C) {
	b, err := ioutil.ReadFile(filepath.Join("test", "testfile.txt"))
	c.Assert(err, IsNil)
	count, err := ProcessEntries(string(b))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 107)
}

func (s *TestSuite) TestProcessExample(c *C) {
	b, err := ioutil.ReadFile(filepath.Join("test", "example.txt"))
	c.Assert(err, IsNil)
	count, err := ProcessEntries(string(b))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 5)
}

func (s *TestSuite) TestProcessSingleLineExample(c *C) {
	count, err := ProcessEntries("<2013-4-7 10:24:39;426842;60;3135;6656;2312;126>")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)
}

func (s *TestSuite) TestProcessSingleLineStartingWithR(c *C) {
	count, err := ProcessEntries("\r<2013-4-7 10:24:39;426842;60;3135;6656;2312;126>")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)
}
