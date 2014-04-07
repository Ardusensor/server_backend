package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
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
)

var (
	port          = flag.Int("port", 8090, "TCP upload port")
	portv2        = flag.Int("portv2", 18150, "TCP upload format V2 port")
	webserverPort = flag.Int("webserver_port", 8084, "HTTP port")
	environment   = flag.String("environment", "development", "environment")
	redisHost     = flag.String("redis", "127.0.0.1:6379", "host:ip of Redis instance")
	workdir       = flag.String("workdir", ".", "workdir of API, where log folder resides etc")
)

var redisPool *redis.Pool

const keyControllers = "osp:controllers"
const keyLogs = "osp:logs"
const keySensorToController = "osp:sensor_to_controller"

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
	Controller struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Token string
	}
	Sensor struct {
		ID           int64      `json:"id"`
		LastTick     *time.Time `json:"last_tick,omitempty"`
		ControllerID string     `json:"controller_id"`
		Lat          string     `json:"lat,omitempty"`
		Lng          string     `json:"lng,omitempty"`
	}
	Tick struct {
		Datetime        time.Time `json:"datetime"`
		SensorID        int64     `json:"sensor_id,omitempty"`
		NextDataSession string    `json:"next_data_session,omitempty"` // sec
		BatteryVoltage  float64   `json:"battery_voltage,omitempty"`   // mV
		Sensor1         int64     `json:"sensor1,omitempty"`           // encoded temperature
		Sensor2         int64     `json:"sensor2,omitempty"`           // humidity
		RadioQuality    int64     `json:"radio_quality,omitempty"`     // (LQI=0..255)
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

	serveTCP(*port, NewTick)
	serveTCP(*portv2, NewTickV2)

	log.Println("API started on port", *webserverPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *webserverPort), http.DefaultServeMux))
}

type tickParser func(input string) (*Tick, error)

func serveTCP(port int, parser tickParser) {
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
			go handleConnection(conn, parser)
		}
	}()
}

func handleConnection(conn net.Conn, parser tickParser) {
	defer conn.Close()
	log.Println("New connection")
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

	go func() {
		redisClient := redisPool.Get()
		defer redisClient.Close()
		if _, err := redisClient.Do("LPUSH", keyLogs, time.Now().String()+" "+buf.String()); err != nil {
			log.Println(err)
			return
		}
		if _, err := redisClient.Do("LTRIM", keyLogs, 0, 1000); err != nil {
			log.Println(err)
			return
		}
	}()

	start := time.Now()
	count, err := ProcessTicks(buf.String(), parser)
	if err != nil {
		log.Println("Error while processing ticks:", err)
		return
	}
	log.Println("Processed", count, "ticks in", time.Since(start))
}

func unmarshalTickJSON(b []byte) (*Tick, error) {
	var values map[string]interface{}
	if err := json.Unmarshal(b, &values); err != nil {
		return nil, err
	}
	var tick Tick
	var err error
	if tick.Datetime, err = time.Parse(time.RFC3339, values["datetime"].(string)); err != nil {
		return nil, err
	}
	if tick.SensorID, err = parseInt(values["sensor_id"]); err != nil {
		return nil, fmt.Errorf("Invalid or missing sensor_id in JSON: %s", err.Error())
	}
	tick.NextDataSession = values["next_data_session"].(string)
	if tick.BatteryVoltage, err = parseFloat(values["battery_voltage"]); err != nil {
		log.Println("Warning: Invalid or missing battery_voltage in JSON", err.Error())
	}
	if tick.Sensor1, err = parseInt(values["sensor1"]); err != nil {
		log.Println("Warning: Invalid or missing sensor1 in JSON", err.Error())
	}
	if tick.Sensor2, err = parseInt(values["sensor2"]); err != nil {
		log.Println("Warning: Invalid or missing sensor2 in JSON", err.Error())
	}
	if tick.RadioQuality, err = parseInt(values["radio_quality"]); err != nil {
		log.Println("Warning: Invalid or missing radio_quality in JSON", err.Error())
	}
	return &tick, nil
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

func findAverages(ticks []*Tick, dotsPerDay int, start int, end int) []*Tick {
	startTime := time.Unix(int64(start), 0)
	endTime := time.Unix(int64(end), 0)
	// 6 dots means: 24h / 6 = 4 hour increment
	// 12 dots means: 24h / 12 = 2 hour increment
	hours := 24 / dotsPerDay
	increment := time.Duration(hours) * time.Hour
	var result []*Tick
	for startTime.Before(endTime) {
		next := startTime.Add(increment)
		dot := averageMatching(ticks, startTime, next)
		dot.decodeForVisual()
		result = append(result, &dot)
		startTime = next
	}
	return result
}

func averageMatching(ticks []*Tick, start time.Time, end time.Time) Tick {
	var matching int64
	var avgBatteryVoltage float64
	var avgSensor1 int64
	var avgSensor2 int64
	var avgRadioQuality int64
	for _, tick := range ticks {
		if tick.Datetime.Before(start) || tick.Datetime.After(end) {
			continue
		}
		avgBatteryVoltage += tick.BatteryVoltage
		avgRadioQuality += tick.RadioQuality
		avgSensor1 += tick.Sensor1
		avgSensor2 += tick.Sensor2
		matching += 1
	}
	if matching > 0 {
		avgBatteryVoltage /= float64(matching)
		avgRadioQuality /= matching
		avgSensor1 /= matching
		avgSensor2 /= matching
	}
	return Tick{
		Datetime:       start,
		BatteryVoltage: avgBatteryVoltage,
		RadioQuality:   avgRadioQuality,
		Sensor1:        avgSensor1,
		Sensor2:        avgSensor2,
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

func NewTick(input string) (*Tick, error) {
	log.Println("NewTick, input: ", input)
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
	tick := &Tick{
		Datetime:        datetime,
		SensorID:        sensorID,
		NextDataSession: parts[2],
	}
	tick.BatteryVoltage, err = strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return nil, err
	}
	tick.Sensor1, err = strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return nil, err
	}
	tick.Sensor2, err = strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return nil, err
	}
	tick.RadioQuality, err = strconv.ParseInt(parts[6], 10, 64)
	if err != nil {
		return nil, err
	}
	return tick, err
}

func NewTickV2(input string) (*Tick, error) {
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
	tick := &Tick{
		Datetime:        datetime,
		SensorID:        sensorID,
		NextDataSession: parts[2],
	}
	tick.BatteryVoltage, err = strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return nil, err
	}
	tick.Sensor1, err = strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return nil, err
	}
	tick.Sensor2, err = strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return nil, err
	}
	tick.RadioQuality, err = strconv.ParseInt(parts[6], 10, 64)
	if err != nil {
		return nil, err
	}
	return tick, err
}

func FindTicksByRange(sensorID int64, startIndex, stopIndex int) ([]*Tick, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("ZREVRANGE", keyOfSensorTicks(sensorID), startIndex, stopIndex)
	if err != nil {
		return nil, err
	}

	var ticks []*Tick
	for _, value := range bb.([]interface{}) {
		tick, err := unmarshalTickJSON(value.([]byte))
		if err != nil {
			return nil, err
		}
		tick.decodeForVisual()
		ticks = append(ticks, tick)
	}

	return ticks, nil
}

func FindTicksByScore(sensorID int64, start, end int) ([]*Tick, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("ZRANGEBYSCORE", keyOfSensorTicks(sensorID), start, end)
	if err != nil {
		return nil, err
	}

	var ticks []*Tick
	for _, value := range bb.([]interface{}) {
		tick, err := unmarshalTickJSON(value.([]byte))
		if err != nil {
			return nil, err
		}
		tick.decodeForVisual()
		ticks = append(ticks, tick)
	}
	return ticks, nil
}

// FIXME: do this when saving
func (tick *Tick) decodeForVisual() {
	tick.TemperatureVisual = decodeTemperature(int32(tick.Sensor1))
	tick.BatteryVoltageVisual = tick.BatteryVoltage / 1000.0
}

func (tick Tick) Save() error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	b, err := json.Marshal(tick)
	if err != nil {
		return err
	}

	_, err = redisClient.Do("ZADD", tick.key(), tick.score(), b)
	return err
}

func (tick Tick) score() float64 {
	return float64(tick.Datetime.Unix())
}

func (tick Tick) key() string {
	return keyOfSensorTicks(tick.SensorID)
}

func (tick Tick) String() string {
	return fmt.Sprintf("datetime: %v, sensor ID: %d, next: %s, battery: %f, sensor1: %d, sensor2: %d, radio: %d",
		tick.Datetime, tick.SensorID, tick.NextDataSession, tick.BatteryVoltage, tick.Sensor1, tick.Sensor2, tick.RadioQuality)
}

func (controller Controller) key() string {
	return keyOfController(controller.ID)
}

func (controller Controller) generateToken() string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("OPEN%dSENSOR%dPLATFORM", controller.ID, controller.ID)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func ProcessTicks(tickList string, parser tickParser) (int, error) {
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

func processTick(redisClient redis.Conn, s string, parser tickParser) error {
	tick, err := parser(s)
	if err != nil {
		return err
	}
	if err := tick.Save(); err != nil {
		return err
	}
	log.Println("Saved:", tick)

	if tick.controllerID == "" {
		id, err := redis.String(redisClient.Do("HGET", keySensorToController, tick.SensorID))
		if err != nil && err != redis.ErrNil {
			return err
		}
		tick.controllerID = id
	}

	if tick.controllerID == "" {
		log.Println("Achtung! Controller ID not found by sensor ID", tick.SensorID, "saving tick to controller 1")
		tick.controllerID = "1"
	}

	controller := &Controller{ID: tick.controllerID}
	if _, err := redisClient.Do("SADD", keyControllers, controller.ID); err != nil {
		return err
	}
	if _, err := redisClient.Do("HSET", controller.key(), "token", controller.generateToken()); err != nil {
		return err
	}
	if _, err := redisClient.Do("HSET", keySensorToController, tick.SensorID, controller.ID); err != nil {
		return err
	}
	if _, err := redisClient.Do("SADD",
		keyOfControllerSensors(tick.controllerID), fmt.Sprintf("%d", tick.SensorID)); err != nil {
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
