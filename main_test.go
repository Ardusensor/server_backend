package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

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

func (s *TestSuite) TestParseTick(c *C) {
	entry, err := parseTickV1("<2012-12-26 12:46:5;75942;60;3158;5632;1584;144>")
	c.Assert(err, Equals, nil)
	c.Assert(entry, Not(Equals), nil)
	c.Assert(fmt.Sprintf("%d", entry.SensorID), Equals, "75942")
	c.Assert(entry.NextDataSession, Equals, "60")
	c.Assert(entry.BatteryVoltage, Equals, 3158.0)
	c.Assert(entry.Temperature, Equals, int64(5632))
	c.Assert(entry.Humidity, Equals, int64(1584))
	c.Assert(entry.RadioQuality, Equals, int64(144))
}

func (s *TestSuite) TestParseDateTime(c *C) {
	entry, err := parseTickV1("<2012-12-26 12:46:5;75942;60;3158;5632;1584;144>")
	c.Assert(err, Equals, nil)
	c.Assert(entry, Not(Equals), nil)
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
	entry, err := parseTickV1("<2012-2-5 12:46:5;75942;60;3158;5632;1584;144>")
	c.Assert(err, Equals, nil)
	c.Assert(entry, Not(Equals), nil)
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
	entry, err := parseTickV1("<2012-12-26 12:46:05;75942;60;3158;5632;1584;144>")
	c.Assert(err, Equals, nil)
	c.Assert(entry, Not(Equals), nil)
	c.Assert(entry.Datetime.Second(), Equals, 5)
}

func (s *TestSuite) TestParseDateTimeSeconds(c *C) {
	entry, err := parseTickV1("<2012-12-26 12:46:35;75942;60;3158;5632;1584;144>")
	c.Assert(err, Equals, nil)
	c.Assert(entry, Not(Equals), nil)
	c.Assert(entry.Datetime.Second(), Equals, 35)
}

func (s *TestSuite) TestParseDateTimeMinutes(c *C) {
	entry, err := parseTickV1("<2012-12-26 13:2:36;75942;10;3202;6784;1580;150>")
	c.Assert(err, Equals, nil)
	c.Assert(entry, Not(Equals), nil)
	c.Assert(entry.Datetime.Minute(), Equals, 2)
}

func (s *TestSuite) TestProcessTicks(c *C) {
	b, err := ioutil.ReadFile(filepath.Join("testdata", "testfile.txt"))
	c.Assert(err, Equals, nil)
	u, err := handleUploadV1(bytes.NewBuffer(b))
	c.Assert(err, Equals, nil)
	c.Assert(len(u.ticks), Equals, 107)
}

func (s *TestSuite) TestProcessExample(c *C) {
	b, err := ioutil.ReadFile(filepath.Join("testdata", "example.txt"))
	c.Assert(err, Equals, nil)
	u, err := handleUploadV1(bytes.NewBuffer(b))
	c.Assert(err, Equals, nil)
	c.Assert(len(u.ticks), Equals, 5)
}

func (s *TestSuite) TestProcessSingleLineExample(c *C) {
	u, err := handleUploadV1(bytes.NewBufferString("<2013-4-7 10:24:39;426842;60;3135;6656;2312;126>"))
	c.Assert(err, Equals, nil)
	c.Assert(len(u.ticks), Equals, 1)
}

func (s *TestSuite) TestProcessSingleLineStartingWithR(c *C) {
	u, err := handleUploadV1(bytes.NewBufferString("\r<2013-4-7 10:24:39;426842;60;3135;6656;2312;126>"))
	c.Assert(err, Equals, nil)
	c.Assert(len(u.ticks), Equals, 1)
}

func (s *TestSuite) TestDecodeTemperature(c *C) {
	c.Assert(decodeTemperature(5056), Equals, 19.5)
	c.Assert(decodeTemperature(2528), Equals, 9.5)
	c.Assert(decodeTemperature(2240), Equals, 8.5)
}

func (s *TestSuite) TestHandleV2(c *C) {
	now := time.Now()
	u, err := handleUploadV2(bytes.NewBufferString("<13;347;886;199;51>(132207)<13;22;196>"))
	c.Assert(err, Equals, nil)
	c.Assert(u, Not(Equals), nil)
	c.Assert(len(u.ticks), Equals, 1)

	tk := u.ticks[0]
	c.Assert(tk.SensorID, Equals, int64(13))
	c.Assert(tk.Temperature, Equals, int64(347))
	c.Assert(tk.BatteryVoltage, Equals, float64(886))
	c.Assert(tk.Humidity, Equals, int64(199))
	c.Assert(tk.Sendcounter, Equals, int64(51))
	c.Assert(tk.Datetime.Unix(), Equals, now.Unix())

	c.Assert(u.cr, Not(Equals), nil)
	/*
		c.Assert(u.cr.ControllerID, Equals, "13")
		c.Assert(u.cr.GSMCoverage, Equals, "22")
		c.Assert(u.cr.BatteryVoltage, Equals, "196")
		c.Assert(u.cr.Datetime.Unix(), Equals, now.Unix())
	*/
}
