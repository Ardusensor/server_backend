package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	portV1        = flag.Int("port", 8090, "TCP upload port, V1 format")
	portV2        = flag.Int("portv2", 18150, "TCP upload port, V2 format")
	webserverPort = flag.Int("webserver_port", 8084, "HTTP port")
	environment   = flag.String("environment", "development", "environment")
	redisHost     = flag.String("redis", "127.0.0.1:6379", "host:ip of Redis instance")
	workdir       = flag.String("workdir", ".", "workdir of API, where log folder resides etc")
)

var redisPool *redis.Pool

const keyControllers = "osp:controllers"
const keyLogsV1 = "osp:logs"
const keyLogsV2 = "osp:logs:v2"
const keySensorToController = "osp:sensor_to_controller"

const socketTimeoutSeconds = 30
const defaultControllerID = "1"

func keyOfSensor(sensorID int64) string {
	return fmt.Sprintf("osp:sensor:%d:fields", sensorID)
}

func keyOfController(controllerID string) string {
	return "osp:controller:" + controllerID + ":fields"
}

func keyOfControllerSensors(controllerID string) string {
	return "osp:controller:" + controllerID + ":sensors"
}

func keyOfSensorTicks(sensorID int64) string {
	return fmt.Sprintf("osp:sensor:%d:ticks", sensorID)
}

type (
	controller struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Token string
	}
	controllerReading struct {
		ControllerID   string  `json:"controller_id"`
		GSMCoverage    int64   `json:"gsm_coverage"`
		BatteryVoltage float64 `json:"battery_voltage"`
	}
	sensor struct {
		ID           int64      `json:"id"`
		LastTick     *time.Time `json:"last_tick,omitempty"`
		ControllerID string     `json:"controller_id"`
		Lat          string     `json:"lat,omitempty"`
		Lng          string     `json:"lng,omitempty"`
	}
	tick struct {
		SensorID        int64     `json:"sensor_id,omitempty"`
		Datetime        time.Time `json:"datetime"`
		NextDataSession string    `json:"next_data_session,omitempty"` // sec
		BatteryVoltage  float64   `json:"battery_voltage,omitempty"`   // mV
		Temperature     int64     `json:"sensor1,omitempty"`           // encoded temperature
		Humidity        int64     `json:"sensor2,omitempty"`           // humidity
		RadioQuality    int64     `json:"radio_quality,omitempty"`     // (LQI=0..255)
		Sendcounter     int64     `json:"send_counter,omitempty"`      // (LQI=0..255)
		// Visual/rendering
		TemperatureVisual    float64 `json:"temperature,omitempty"`
		BatteryVoltageVisual float64 `json:"battery_voltage_visual,omitempty"` // actual mV value, for visual
		// Controller ID is not serialized
		controllerID string
	}
)

func main() {
	flag.Parse()

	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()

	defineRoutes()

	if *environment == "production" || *environment == "staging" {
		f, err := os.OpenFile(filepath.Join(*workdir, "log", *environment+".log"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	serveTCP(*portV1, onUploadV1, keyLogsV1)
	serveTCP(*portV2, onUploadV2, keyLogsV2)

	log.Println("API started on port", *webserverPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *webserverPort), http.DefaultServeMux))
}

type tickParser func(input string) (*tick, error)

func serveTCP(port int, parser tickParser, keyLogs string) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Println("Upload server started on port", port)
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println("Error while accepting connection:", err)
				continue
			}
			go handleConnection(conn, port, parser, keyLogs)
		}
	}()
}

func handleConnection(conn net.Conn, port int, parser tickParser, keyLogs string) {
	defer conn.Close()
	log.Println("New connection on port", port)

	ch := make(chan string, 1)
	go func() {
		buf := &bytes.Buffer{}
		for {
			data := make([]byte, 256)
			n, err := conn.Read(data)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Println("Error while reading from connection:", err)
				return
			}
			buf.Write(data[:n])
			if data[0] == 13 && data[1] == 10 {
				break
			}
		}
		ch <- buf.String()
	}()

	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(socketTimeoutSeconds * time.Second)
		timeout <- true
	}()

	var payload string
	select {
	case payload = <-ch:
		break
	case <-timeout:
		log.Println("Connection timeout!")
		break
	}

	start := time.Now()
	go logTick(payload, keyLogs)
	count, err := processTicks(payload, parser)
	if err != nil {
		log.Println("Error while processing ticks:", err)
		return
	}
	log.Println("Processed", count, "ticks in", time.Since(start))
}

func logTick(payload string, keyLogs string) {
	redisClient := redisPool.Get()
	defer redisClient.Close()
	if _, err := redisClient.Do("LPUSH", keyLogs, time.Now().String()+" "+payload); err != nil {
		log.Println(err)
		return
	}
	if _, err := redisClient.Do("LTRIM", keyLogs, 0, 1000); err != nil {
		log.Println(err)
		return
	}
}

func unmarshalTickJSON(b []byte) (*tick, error) {
	var values map[string]interface{}
	if err := json.Unmarshal(b, &values); err != nil {
		return nil, err
	}
	var t tick
	var err error
	if t.Datetime, err = time.Parse(time.RFC3339, values["datetime"].(string)); err != nil {
		return nil, err
	}
	if t.SensorID, err = parseInt(values["sensor_id"]); err != nil {
		return nil, fmt.Errorf("Invalid or missing sensor_id in JSON: %s", err.Error())
	}
	t.NextDataSession = values["next_data_session"].(string)
	if t.BatteryVoltage, err = parseFloat(values["battery_voltage"]); err != nil {
		log.Println("Warning: Invalid or missing battery_voltage in JSON", err.Error())
	}
	if t.Temperature, err = parseInt(values["sensor1"]); err != nil {
		log.Println("Warning: Invalid or missing sensor1 in JSON", err.Error())
	}
	if t.Humidity, err = parseInt(values["sensor2"]); err != nil {
		log.Println("Warning: Invalid or missing sensor2 in JSON", err.Error())
	}
	if t.RadioQuality, err = parseInt(values["radio_quality"]); err != nil {
		log.Println("Warning: Invalid or missing radio_quality in JSON", err.Error())
	}
	return &t, nil
}

func parseFloat(value interface{}) (float64, error) {
	s, isString := value.(string)
	if isString {
		return strconv.ParseFloat(s, 64)
	}
	if n, isNumeric := value.(float64); isNumeric {
		return float64(n), nil
	}
	return 0, nil
}

func parseInt(value interface{}) (int64, error) {
	s, isString := value.(string)
	if isString {
		return strconv.ParseInt(s, 10, 64)
	}
	if n, isNumeric := value.(float64); isNumeric {
		return int64(n), nil
	}
	return 0, nil
}

func findAverages(ticks []*tick, dotsPerDay int, start int, end int) []*tick {
	startTime := time.Unix(int64(start), 0)
	endTime := time.Unix(int64(end), 0)
	// 6 dots means: 24h / 6 = 4 hour increment
	// 12 dots means: 24h / 12 = 2 hour increment
	hours := 24 / dotsPerDay
	increment := time.Duration(hours) * time.Hour
	var result []*tick
	for startTime.Before(endTime) {
		next := startTime.Add(increment)
		dot := averageMatching(ticks, startTime, next)
		dot.decodeForVisual()
		result = append(result, &dot)
		startTime = next
	}
	return result
}

func averageMatching(ticks []*tick, start time.Time, end time.Time) tick {
	var matching int64
	var avgBatteryVoltage float64
	var avgTemperature int64
	var avgHumidity int64
	var avgRadioQuality int64
	for _, tick := range ticks {
		if tick.Datetime.Before(start) || tick.Datetime.After(end) {
			continue
		}
		avgBatteryVoltage += tick.BatteryVoltage
		avgRadioQuality += tick.RadioQuality
		avgTemperature += tick.Temperature
		avgHumidity += tick.Humidity
		matching += 1
	}
	if matching > 0 {
		avgBatteryVoltage /= float64(matching)
		avgRadioQuality /= matching
		avgTemperature /= matching
		avgHumidity /= matching
	}
	return tick{
		Datetime:       start,
		BatteryVoltage: avgBatteryVoltage,
		RadioQuality:   avgRadioQuality,
		Temperature:    avgTemperature,
		Humidity:       avgHumidity,
	}
}

func getRedisPool(host string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", host)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func onUploadV1(input string) (*tick, error) {
	log.Println("onUploadv1, input: ", input)
	contents := input[1 : len(input)-1]
	parts := strings.Split(contents, ";")
	datetime, err := time.Parse("2006-1-2 15:4:5", parts[0])
	if err != nil {
		return nil, err
	}
	sensorID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}
	t := &tick{
		Datetime:        datetime,
		SensorID:        sensorID,
		NextDataSession: parts[2],
	}
	t.BatteryVoltage, err = strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return nil, err
	}
	t.Temperature, err = strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return nil, err
	}
	t.Humidity, err = strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return nil, err
	}
	t.RadioQuality, err = strconv.ParseInt(parts[6], 10, 64)
	if err != nil {
		return nil, err
	}
	return t, err
}

func onUploadV2(input string) (*tick, error) {
	log.Println("NewTick v2, input: ", input)
	contents := input[1 : len(input)-1]
	parts := strings.Split(contents, ";")
	datetime, err := time.Parse("2006-1-2 15:4:5", parts[0])
	if err != nil {
		return nil, err
	}
	sensorID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}
	t := &tick{
		Datetime:        datetime,
		SensorID:        sensorID,
		NextDataSession: parts[2],
	}
	t.BatteryVoltage, err = strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return nil, err
	}
	t.Temperature, err = strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return nil, err
	}
	t.Humidity, err = strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return nil, err
	}
	t.RadioQuality, err = strconv.ParseInt(parts[6], 10, 64)
	if err != nil {
		return nil, err
	}
	return t, err
}

func findTicksByRange(sensorID int64, startIndex, stopIndex int) ([]*tick, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("ZREVRANGE", keyOfSensorTicks(sensorID), startIndex, stopIndex)
	if err != nil {
		return nil, err
	}

	var ticks []*tick
	for _, value := range bb.([]interface{}) {
		t, err := unmarshalTickJSON(value.([]byte))
		if err != nil {
			return nil, err
		}
		t.decodeForVisual()
		ticks = append(ticks, t)
	}

	return ticks, nil
}

func findTicksByScore(sensorID int64, start, end int) ([]*tick, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("ZRANGEBYSCORE", keyOfSensorTicks(sensorID), start, end)
	if err != nil {
		return nil, err
	}

	var ticks []*tick
	for _, value := range bb.([]interface{}) {
		t, err := unmarshalTickJSON(value.([]byte))
		if err != nil {
			return nil, err
		}
		t.decodeForVisual()
		ticks = append(ticks, t)
	}
	return ticks, nil
}

// FIXME: do this when saving
func (t *tick) decodeForVisual() {
	t.TemperatureVisual = decodeTemperature(int32(t.Temperature))
	t.BatteryVoltageVisual = t.BatteryVoltage / 1000.0
}

func (t tick) Save() error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	b, err := json.Marshal(t)
	if err != nil {
		return err
	}

	_, err = redisClient.Do("ZADD", t.key(), t.score(), b)
	return err
}

func (t tick) score() float64 {
	return float64(t.Datetime.Unix())
}

func (t tick) key() string {
	return keyOfSensorTicks(t.SensorID)
}

func (t tick) String() string {
	return fmt.Sprintf("datetime: %v, sensor ID: %d, next: %s, battery: %f, sensor1: %d, humidity: %d, radio: %d",
		t.Datetime, t.SensorID, t.NextDataSession, t.BatteryVoltage, t.Temperature, t.Humidity, t.RadioQuality)
}

func (c controller) key() string {
	return keyOfController(c.ID)
}

func (c controller) generateToken() string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("OPEN%dSENSOR%dPLATFORM", c.ID, c.ID)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func processTicks(tickList string, parser tickParser) (int, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	tickList = strings.Replace(tickList, "\r", "\n", -1)
	processedCount := 0
	for _, s := range strings.Split(tickList, "\n") {
		if len(s) == 0 {
			continue
		}
		err := processTick(redisClient, s, parser)
		if err != nil {
			return 0, err
		}
		processedCount += 1
	}

	return processedCount, nil
}

func processTick(redisClient redis.Conn, s string, parseTick tickParser) error {
	t, err := parseTick(s)
	if err != nil {
		return err
	}
	if err := t.Save(); err != nil {
		return err
	}
	log.Println("Saved:", t)

	if t.controllerID == "" {
		id, err := redis.String(redisClient.Do("HGET", keySensorToController, t.SensorID))
		if err != nil && err != redis.ErrNil {
			return err
		}
		t.controllerID = id
	}

	if t.controllerID == "" {
		log.Println("Achtung! Controller ID not found by sensor ID", t.SensorID, "saving tick to controller ", defaultControllerID)
		t.controllerID = defaultControllerID
	}

	c := &controller{ID: t.controllerID}
	if _, err := redisClient.Do("SADD", keyControllers, c.ID); err != nil {
		return err
	}
	if _, err := redisClient.Do("HSET", c.key(), "token", c.generateToken()); err != nil {
		return err
	}
	if _, err := redisClient.Do("HSET", keySensorToController, t.SensorID, c.ID); err != nil {
		return err
	}
	if _, err := redisClient.Do("SADD",
		keyOfControllerSensors(t.controllerID), fmt.Sprintf("%d", t.SensorID)); err != nil {
		return err
	}
	return nil
}

func decodeTemperature(n int32) float64 {
	sum := 0.0
	if n&(1<<7) != 0 {
		sum += 0.5
	}
	if n&(1<<8) != 0 {
		sum += 1
	}
	if n&(1<<9) != 0 {
		sum += 2
	}
	if n&(1<<10) != 0 {
		sum += 4
	}
	if n&(1<<11) != 0 {
		sum += 8
	}
	if n&(1<<12) != 0 {
		sum += 16
	}
	if n&(1<<13) != 0 {
		sum += 32
	}
	if n&(1<<14) != 0 {
		sum += 64
	}
	if n&(1<<15) != 0 {
		return -sum
	}
	return sum
}
