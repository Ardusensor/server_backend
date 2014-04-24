package main

import (
	"bytes"
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

func (s *TestSuite) TestProcessExample(c *C) {
	b, err := ioutil.ReadFile(filepath.Join("testdata", "example.json"))
	c.Assert(err, Equals, nil)
	u, err := handleUploadV1(bytes.NewBuffer(b))
	c.Assert(err, Equals, nil)
	c.Assert(len(u.ticks), Equals, 20)
}

func (s *TestSuite) TestHandleV2(c *C) {
	now := time.Now()
	u, err := handleUploadV2(bytes.NewBufferString("<13;347;886;199;51>(132207)<13;22;196>"))
	c.Assert(err, Equals, nil)
	c.Assert(u, Not(Equals), nil)
	c.Assert(len(u.ticks), Equals, 1)

	tk := u.ticks[0]
	c.Assert(tk.SensorID, Equals, "13")
	c.Assert(tk.Temperature, Equals, float64(18.598360655737704))
	c.Assert(tk.BatteryVoltage, Equals, float64(3.40224))
	c.Assert(tk.Humidity, Equals, int64(199))
	c.Assert(tk.Sendcounter, Equals, int64(51))
	c.Assert(tk.Datetime.Unix(), Equals, now.Unix())

	c.Assert(u.cr.ControllerID, Equals, "13")
	c.Assert(u.cr.GSMCoverage, Equals, int64(22))
	c.Assert(u.cr.BatteryVoltage, Equals, float64(196))
	c.Assert(u.cr.Datetime.Unix(), Equals, now.Unix())
}

func (s *TestSuite) TestHandleV2withSpaces(c *C) {
	now := time.Now()
	u, err := handleUploadV2(bytes.NewBufferString("<10;344;875;195;49>                <12;350;895;159;18>(34204)<10;344;875;195;49>                <12;350;895;159;18>(411279)<12;350;895;159;18>(525538)<13;347;888;195;57>(1101950)<17;343;883;159;51>(1229059)<16;145;379;159;17>(1253399)<11;338;879;287;16>(1416289)<15;345;1023;211;17>(1515808)<13;21;350>"))
	c.Assert(err, Equals, nil)
	c.Assert(u, Not(Equals), nil)
	c.Assert(len(u.ticks), Equals, 10)

	tk := u.ticks[0]
	c.Assert(tk.controllerID, Equals, "13")
	c.Assert(tk.SensorID, Equals, "10")
	c.Assert(tk.Temperature, Equals, float64(16.13934426229508))
	c.Assert(tk.BatteryVoltage, Equals, float64(3.36))
	c.Assert(tk.Humidity, Equals, int64(195))
	c.Assert(tk.Sendcounter, Equals, int64(49))
	c.Assert(tk.Datetime.Unix(), Equals, now.Unix())

	tk = u.ticks[len(u.ticks)-1]
	c.Assert(tk.controllerID, Equals, "13")
	c.Assert(tk.SensorID, Equals, "15")
	c.Assert(tk.Temperature, Equals, float64(16.95901639344262))
	c.Assert(tk.BatteryVoltage, Equals, float64(3.9283200000000003))
	c.Assert(tk.Humidity, Equals, int64(211))
	c.Assert(tk.Sendcounter, Equals, int64(17))
	c.Assert(tk.Datetime.Unix(), Equals, now.Unix())

	c.Assert(u.cr.ControllerID, Equals, "13")
	c.Assert(u.cr.GSMCoverage, Equals, int64(21))
	c.Assert(u.cr.BatteryVoltage, Equals, float64(350))
	c.Assert(u.cr.Datetime.Unix(), Equals, now.Unix())
}

func (s *TestSuite) TestHandleV2garbage(c *C) {
	now := time.Now()
	u, err := handleUploadV2(bytes.NewBufferString("<11;335;838;343;200>><10;344;873;211;175>40>><10;344;871;211;177>41>><10;344;873;211;179>42>>~<13;23;200>	"))
	c.Assert(err, Equals, nil)
	c.Assert(u, Not(Equals), nil)
	c.Assert(len(u.ticks), Equals, 4)

	tk := u.ticks[0]
	c.Assert(tk.controllerID, Equals, "13")
	c.Assert(tk.SensorID, Equals, "11")
	c.Assert(tk.Temperature, Equals, float64(8.762295081967212))
	c.Assert(tk.BatteryVoltage, Equals, float64(3.21792))
	c.Assert(tk.Humidity, Equals, int64(343))
	c.Assert(tk.Sendcounter, Equals, int64(200))
	c.Assert(tk.Datetime.Unix(), Equals, now.Unix())

	tk = u.ticks[len(u.ticks)-1]
	c.Assert(tk.controllerID, Equals, "13")
	c.Assert(tk.SensorID, Equals, "10")
	c.Assert(tk.Temperature, Equals, float64(16.13934426229508))
	c.Assert(tk.BatteryVoltage, Equals, float64(3.35232))
	c.Assert(tk.Humidity, Equals, int64(211))
	c.Assert(tk.Sendcounter, Equals, int64(179))
	c.Assert(tk.Datetime.Unix(), Equals, now.Unix())

	c.Assert(u.cr.ControllerID, Equals, "13")
	c.Assert(u.cr.GSMCoverage, Equals, int64(23))
	c.Assert(u.cr.BatteryVoltage, Equals, float64(200))
	c.Assert(u.cr.Datetime.Unix(), Equals, now.Unix())
}
