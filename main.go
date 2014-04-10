package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
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
const loggingKeyV1 = "osp:logs"
const loggingKeyV2 = "osp:logs:v2"
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
		ControllerID   string    `json:"controller_id"`
		Datetime       time.Time `json:"datetime"`
		GSMCoverage    int64     `json:"gsm_coverage"`
		BatteryVoltage float64   `json:"battery_voltage"`
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
		NextDataSession string    `json:"next_data_session,omitempty"`      // sec
		BatteryVoltage  float64   `json:"battery_voltage_visual,omitempty"` // mV
		Temperature     float64   `json:"temperature,omitempty"`            // encoded temperature
		Humidity        int64     `json:"sensor2,omitempty"`                // humidity
		RadioQuality    int64     `json:"radio_quality,omitempty"`          // (LQI=0..255)
		Sendcounter     int64     `json:"send_counter,omitempty"`           // (LQI=0..255)
		// Controller ID is not serialized
		controllerID string
		Version      int64 `json:"version"`
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

	serveTCP(*portV1, handleUploadV1, loggingKeyV1)
	serveTCP(*portV2, handleUploadV2, loggingKeyV2)

	log.Println("API started on port", *webserverPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *webserverPort), http.DefaultServeMux))
}

type upload struct {
	ticks []*tick
	cr    controllerReading
}

type uploadHandler func(buf *bytes.Buffer) (*upload, error)

func serveTCP(port int, handler uploadHandler, loggingKey string) {
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
			go handleConnection(conn, port, handler, loggingKey)
		}
	}()
}

func handleConnection(conn net.Conn, port int, handler uploadHandler, loggingKey string) {
	defer conn.Close()
	log.Println("New connection on port", port)

	ch := make(chan *bytes.Buffer, 1)
	go func() {
		buf := &bytes.Buffer{}
		for {
			b := make([]byte, 256)
			n, err := conn.Read(b)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Println("Error while reading from connection:", err)
				return
			}
			buf.Write(b[:n])
			if b[0] == 13 && b[1] == 10 {
				break
			}
		}
		ch <- buf
	}()

	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(socketTimeoutSeconds * time.Second)
		timeout <- true
	}()

	var buf *bytes.Buffer
	select {
	case buf = <-ch:
		break
	case <-timeout:
		log.Println("Connection timeout!")
		break
	}

	go logUpload(buf, loggingKey)

	start := time.Now()
	upload, err := handler(buf)
	if err != nil {
		log.Println("Error while processing ticks:", err)
		return
	}
	log.Println("Processed", upload, "in", time.Since(start))
}

func logUpload(buf *bytes.Buffer, loggingKey string) {
	redisClient := redisPool.Get()
	defer redisClient.Close()
	if _, err := redisClient.Do("LPUSH", loggingKey, time.Now().String()+" "+buf.String()); err != nil {
		log.Println(err)
		return
	}
	if _, err := redisClient.Do("LTRIM", loggingKey, 0, 1000); err != nil {
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
	if _, exists := values["next_data_session"]; exists {
		t.NextDataSession = values["next_data_session"].(string)
	}
	if t.BatteryVoltage, err = parseFloat(values["battery_voltage_visual"]); err != nil {
		log.Println("Warning: Invalid or missing battery_voltage in JSON", err.Error())
	}
	if t.Temperature, err = parseFloat(values["temperature"]); err != nil {
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
		result = append(result, &dot)
		startTime = next
	}
	return result
}

func averageMatching(ticks []*tick, start time.Time, end time.Time) tick {
	var matching int64
	var avgBatteryVoltage float64
	var avgTemperature float64
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
		avgTemperature /= float64(matching)
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

func parseTickV1(input string) (*tick, error) {
	contents := input[1 : len(input)-1]
	parts := strings.Split(contents, ";")
	datetime, err := time.Parse("2006-1-2 15:4:5", parts[0])
	if err != nil {
		return nil, err
	}
	sensorID, err := parseInt(parts[1])
	if err != nil {
		return nil, err
	}
	t := &tick{
		Datetime:        datetime,
		SensorID:        sensorID,
		NextDataSession: parts[2],
		Version:         1,
	}
	t.BatteryVoltage, err = parseFloat(parts[3])
	if err != nil {
		return nil, err
	}
	t.Temperature, err = parseFloat(parts[4])
	if err != nil {
		return nil, err
	}
	t.Humidity, err = parseInt(parts[5])
	if err != nil {
		return nil, err
	}
	t.RadioQuality, err = parseInt(parts[6])
	if err != nil {
		return nil, err
	}
	return t, nil
}

func parseTickV2(input string) (*tick, error) {
	t := &tick{
		Datetime: time.Now(),
		Version:  2,
	}

	parts := strings.Split(input, ";")
	if len(parts) != 5 {
		return nil, fmt.Errorf("%d fields expected, got %d", 5, len(parts))
	}

	var err error
	t.SensorID, err = parseInt(parts[0])
	if err != nil {
		return nil, err
	}

	sensorReading, err := parseFloat(parts[1])
	if err != nil {
		return nil, err
	}
	t.Temperature = (sensorReading - 324.31) / 1.22

	t.BatteryVoltage, err = parseFloat(parts[2])
	if err != nil {
		return nil, err
	}
	t.BatteryVoltage = t.BatteryVoltage * 0.00384

	t.Humidity, err = parseInt(parts[3])
	if err != nil {
		return nil, err
	}

	t.Sendcounter, err = parseInt(parts[4])
	if err != nil {
		return nil, err
	}

	return t, nil
}

func parseMessages(buf *bytes.Buffer) ([]string, error) {
	var messages []string
	var message *bytes.Buffer
	for _, c := range buf.Bytes() {
		if '>' == c {
			if nil == message {
				continue
			}
			messages = append(messages, message.String())
			message = nil
			continue
		}
		if '<' == c {
			message = bytes.NewBuffer(nil)
			continue
		}
		if nil == message {
			continue
		}
		if err := message.WriteByte(c); err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func parseControllerReading(input string) (*controllerReading, error) {
	c := &controllerReading{
		Datetime: time.Now(),
	}

	parts := strings.Split(input, ";")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%d fields expected, got %d", 5, len(parts))
	}

	var err error
	c.ControllerID = parts[0]
	if err != nil {
		return nil, err
	}

	c.GSMCoverage, err = parseInt(parts[1])
	if err != nil {
		return nil, err
	}

	c.BatteryVoltage, err = parseFloat(parts[2])
	if err != nil {
		return nil, err
	}

	return c, nil
}

func handleUploadV2(buf *bytes.Buffer) (*upload, error) {
	messages, err := parseMessages(buf)
	if err != nil {
		return nil, err
	}
	if len(messages) < 2 {
		return nil, errors.New("Invalid package: at least 1 sensor reading and 1 controller reading expected")
	}
	cr, err := parseControllerReading(messages[len(messages)-1])
	if err != nil {
		return nil, err
	}
	ticks, err := parseTicks(messages[0:len(messages)-1], parseTickV2)
	if err != nil {
		return nil, err
	}
	for _, t := range ticks {
		t.controllerID = cr.ControllerID
	}
	err = saveTicks(ticks)
	if err != nil {
		return nil, err
	}
	result := &upload{
		cr:    *cr,
		ticks: ticks,
	}
	return result, nil
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
		ticks = append(ticks, t)
	}
	return ticks, nil
}

func (t *tick) Save() error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	b, err := json.Marshal(t)
	if err != nil {
		return err
	}

	_, err = redisClient.Do("ZADD", t.key(), t.score(), b)
	if err != nil {
		return err
	}

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

type tickParser func(input string) (*tick, error)

func parseTicks(input []string, parse tickParser) ([]*tick, error) {
	var result []*tick
	for _, s := range input {
		if len(s) == 0 {
			continue
		}
		t, err := parse(s)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, nil
}

func saveTicks(ticks []*tick) error {
	for _, t := range ticks {
		if err := t.Save(); err != nil {
			return err
		}
	}
	return nil
}

func handleUploadV1(buf *bytes.Buffer) (*upload, error) {
	input := strings.Split(strings.Replace(buf.String(), "\r", "\n", -1), "\n")
	ticks, err := parseTicks(input, parseTickV1)
	if err != nil {
		return nil, err
	}
	err = saveTicks(ticks)
	if err != nil {
		return nil, err
	}
	return &upload{
		ticks: ticks,
	}, nil
}
