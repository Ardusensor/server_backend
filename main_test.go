package main

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"
)

const equals = true

func assert(t *testing.T, a interface{}, mustEqual bool, b interface{}) {
	if mustEqual {
		if a != b {
			t.Fatalf("%v did not equal %v", a, b)
		}
	} else {
		if a == b {
			t.Fatalf("%v did equal %v", a, b)
		}
	}
}

func TestParseTick(t *testing.T) {
	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()
	entry, err := NewTick("<2012-12-26 12:46:5;75942;60;3158;5632;1584;144>")
	assert(t, err, equals, nil)
	assert(t, entry, !equals, nil)
	assert(t, fmt.Sprintf("%d", entry.SensorID), equals, "75942")
	assert(t, entry.NextDataSession, equals, "60")
	assert(t, entry.BatteryVoltage, equals, 3158.0)
	assert(t, entry.Sensor1, equals, int64(5632))
	assert(t, entry.Sensor2, equals, int64(1584))
	assert(t, entry.RadioQuality, equals, int64(144))
}

func TestParseDateTime(t *testing.T) {
	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()
	entry, err := NewTick("<2012-12-26 12:46:5;75942;60;3158;5632;1584;144>")
	assert(t, err, equals, nil)
	assert(t, entry, !equals, nil)
	assert(t, entry.Datetime.Year(), equals, 2012)
	assert(t, entry.Datetime.Month(), equals, time.December)
	assert(t, entry.Datetime.Day(), equals, 26)
	assert(t, entry.Datetime.Hour(), equals, 12)
	assert(t, entry.Datetime.Minute(), equals, 46)
	assert(t, entry.Datetime.Second(), equals, 5)
	name, _ := entry.Datetime.Zone()
	assert(t, name, equals, "UTC")
}

func TestParseDateTimeMonthAndDayNotPadded(t *testing.T) {
	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()
	entry, err := NewTick("<2012-2-5 12:46:5;75942;60;3158;5632;1584;144>")
	assert(t, err, equals, nil)
	assert(t, entry, !equals, nil)
	assert(t, entry.Datetime.Year(), equals, 2012)
	assert(t, entry.Datetime.Month(), equals, time.February)
	assert(t, entry.Datetime.Day(), equals, 5)
	assert(t, entry.Datetime.Hour(), equals, 12)
	assert(t, entry.Datetime.Minute(), equals, 46)
	assert(t, entry.Datetime.Second(), equals, 5)
	name, _ := entry.Datetime.Zone()
	assert(t, name, equals, "UTC")
}

func TestParseDateTimeWithPaddedSeconds(t *testing.T) {
	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()
	entry, err := NewTick("<2012-12-26 12:46:05;75942;60;3158;5632;1584;144>")
	assert(t, err, equals, nil)
	assert(t, entry, !equals, nil)
	assert(t, entry.Datetime.Second(), equals, 5)
}

func TestParseDateTimeSeconds(t *testing.T) {
	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()
	entry, err := NewTick("<2012-12-26 12:46:35;75942;60;3158;5632;1584;144>")
	assert(t, err, equals, nil)
	assert(t, entry, !equals, nil)
	assert(t, entry.Datetime.Second(), equals, 35)
}

func TestParseDateTimeMinutes(t *testing.T) {
	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()
	entry, err := NewTick("<2012-12-26 13:2:36;75942;10;3202;6784;1580;150>")
	assert(t, err, equals, nil)
	assert(t, entry, !equals, nil)
	assert(t, entry.Datetime.Minute(), equals, 2)
}

func TestProcessTicks(t *testing.T) {
	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()
	b, err := ioutil.ReadFile(filepath.Join("test", "testfile.txt"))
	assert(t, err, equals, nil)
	count, err := ProcessTicks(string(b))
	assert(t, err, equals, nil)
	assert(t, count, equals, 107)
}

func TestProcessExample(t *testing.T) {
	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()
	b, err := ioutil.ReadFile(filepath.Join("test", "example.txt"))
	assert(t, err, equals, nil)
	count, err := ProcessTicks(string(b))
	assert(t, err, equals, nil)
	assert(t, count, equals, 5)
}

func TestProcessSingleLineExample(t *testing.T) {
	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()
	count, err := ProcessTicks("<2013-4-7 10:24:39;426842;60;3135;6656;2312;126>")
	assert(t, err, equals, nil)
	assert(t, count, equals, 1)
}

func TestProcessSingleLineStartingWithR(t *testing.T) {
	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()
	count, err := ProcessTicks("\r<2013-4-7 10:24:39;426842;60;3135;6656;2312;126>")
	assert(t, err, equals, nil)
	assert(t, count, equals, 1)
}

func TestDecodeTemperature(t *testing.T) {
	assert(t, decodeTemperature(5056), equals, 19.5)
	assert(t, decodeTemperature(2528), equals, 9.5)
	assert(t, decodeTemperature(2240), equals, 8.5)
}
