package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
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

	"github.com/toggl/bugsnag"
)

var (
	jsonPort      = flag.Int("json_port", 18150, "TCP upload port, JSON format")
	webserverPort = flag.Int("webserver_port", 8084, "HTTP port")
	environment   = flag.String("environment", "development", "environment")
	redisHost     = flag.String("redis", "127.0.0.1:6379", "host:ip of Redis instance")
	workdir       = flag.String("workdir", ".", "workdir of API, where log folder resides etc")
	bugsnagAPIKey = flag.String("bugsnag_apikey", "", "")
	adminUsername = flag.String("admin_username", "foo", "Admin API username")
	adminPassword = flag.String("admin_password", "bar", "Admin API password")
)

const socketTimeoutSeconds = 30
const defaultCoordinatorID = "1"

func main() {
	flag.Parse()

	bugsnag.APIKey = *bugsnagAPIKey

	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()

	defineRoutes()

	if err := os.Mkdir(filepath.Join(*workdir, "log"), 0755); err != nil {
		log.Println(err)
	}
	if *environment == "production" || *environment == "staging" {
		f, err := os.OpenFile(filepath.Join(*workdir, "log", *environment+".log"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	serveTCP("JSON", *jsonPort, handleJSONUpload)

	log.Println("API started on port", *webserverPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *webserverPort), http.DefaultServeMux))
}

type upload struct {
	ticks    []*tick
	cr       controllerReading
	debugLog string
}

type uploadHandler func(buf *bytes.Buffer) (*upload, error)

func serveTCP(name string, port int, handler uploadHandler) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Println(name, "server started on port", port)
		for {
			conn, err := ln.Accept()
			if err != nil {
				bugsnag.Notify(err)
				continue
			}
			go handleConnection(conn, port, handler)
		}
	}()
}

func handleConnection(conn net.Conn, port int, handler uploadHandler) {
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
				bugsnag.Notify(err)
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

	start := time.Now()
	_, err := handler(buf)
	if err != nil {
		bugsnag.Notify(err)
		return
	}

	log.Println("Upload processed in", time.Since(start))
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

func parseJSONTick(coordinatorID string, input string) (*tick, error) {
	t := &tick{
		Datetime: time.Now(),
		Version:  2,
	}

	parts := strings.Split(input, ";")
	if len(parts) != 5 {
		return nil, fmt.Errorf("%d fields expected, got %d", 5, len(parts))
	}

	t.SensorID = parts[0]
	if len(t.SensorID) == 0 {
		return nil, errors.New("Missing sensor ID")
	}

	sensor, err := loadSensor(coordinatorID, t.SensorID)
	if err != nil {
		return nil, err
	}

	sensorReading, err := parseFloat(parts[1])
	if err != nil {
		return nil, err
	}
	t.setTemperatureFromSensorReading(sensorReading, sensor)

	sensorReading, err = parseFloat(parts[2])
	if err != nil {
		return nil, err
	}
	t.setBatteryVoltageFromSensorReading(sensorReading)

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

func handleJSONUpload(buf *bytes.Buffer) (*upload, error) {
	log.Println("handleJSONUpload", buf.String())

	go func(b *bytes.Buffer) {
		if err := saveLog(buf, loggingKeyJSON); err != nil {
			bugsnag.Notify(err)
		}
	}(buf)

	var pl payload
	if err := json.Unmarshal(buf.Bytes(), &pl); err != nil {
		return nil, err
	}

	if err := saveCoordinatorReading(pl.Coordinator); err != nil {
		return nil, err
	}

	// FIXME: save ticks without converting to old format.
	// instead convert all old format to new format and delete
	// old format support afterwards
	ticks, err := pl.convertToOldFormat()
	if err != nil {
		return nil, err
	}

	if err := saveTicks(ticks); err != nil {
		return nil, err
	}

	// FIXME: delete, it's used in testing only
	return &upload{
		ticks: ticks,
	}, nil
}

func (t *tick) Save() error {
	log.Println("Saving tick", t)

	b, err := json.Marshal(t)
	if err != nil {
		return err
	}

	if err := saveReading(keyOfSensorTicks(t.SensorID), float64(t.Datetime.Unix()), b); err != nil {
		return err
	}

	if t.coordinatorID == "" {
		id, err := findCoordinatorIDBySensorID(t.SensorID)
		if err != nil {
			return err
		}
		t.coordinatorID = id
	}

	if t.coordinatorID == "" {
		log.Println("Coordinator ID not found by sensor ID", t.SensorID, "saving tick to coordinator", defaultCoordinatorID)
		t.coordinatorID = defaultCoordinatorID
	}

	if err := setCoordinatorToken(t.coordinatorID); err != nil {
		return err
	}

	if err := addSensorToCoordinator(t.SensorID, t.coordinatorID); err != nil {
		return err
	}

	return nil
}

func (t tick) String() string {
	return fmt.Sprintf("coordinatorID: %v, datetime: %v, sensor ID: %v, next: %v, battery: %v, sensor1: %v, humidity: %v, radio: %v",
		t.coordinatorID, t.Datetime, t.SensorID, t.NextDataSession, t.BatteryVoltage, t.Temperature, t.Humidity, t.RadioQuality)
}

func tokenForCoordinator(coordinatorID string) string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("OPEN%sSENSOR%sPLATFORM", coordinatorID, coordinatorID)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

type tickParser func(coordinatorID string, input string) (*tick, error)

func saveTicks(ticks []*tick) error {
	for _, t := range ticks {
		if err := t.Save(); err != nil {
			return err
		}
	}
	return nil
}

func (t tick) calculateTemperatureFromRaw() float64 {
	return ((t.RawTemperature * 0.001292) - 0.6) / 0.01
}

func (t *tick) setTemperatureFromSensorReading(sensorReading float64, s *sensor) {
	t.RawTemperature = sensorReading

	log.Println("[CALCULATE TEMP]", t.SensorID, "sensorReading", sensorReading)

	t.Temperature = t.calculateTemperatureFromRaw()

	log.Println("[CALCULATE TEMP]", t.SensorID, "Temperature = ((t.RawTemperature * 0.001292) - 0.6) / 0.01", t.Temperature)

	if s.CalibrationConstant != nil {
		log.Println("[CALCULATE TEMP]", t.SensorID, "CalibrationConstant", *s.CalibrationConstant)
		t.Temperature += *s.CalibrationConstant
		log.Println("[CALCULATE TEMP]", t.SensorID, "Temperature = t.Temperature += *s.CalibrationConstant", t.Temperature)
	}
}

func (t *tick) setBatteryVoltageFromSensorReading(sensorReading float64) {
	t.BatteryVoltage = sensorReading * 0.00384
}

type authData struct {
	Username string
	Password string
}

func parseToken(r *http.Request) (*authData, error) {
	auth := r.Header.Get("Authorization")
	if 0 == len(auth) {
		return nil, nil
	}
	if !strings.Contains(auth, "Basic ") {
		// Unsupported auth scheme, for example, NTLM
		return nil, nil
	}
	encodedToken := strings.Replace(auth, "Basic ", "", -1)
	b, err := base64.StdEncoding.DecodeString(encodedToken)
	if err != nil {
		return nil, err
	}
	pair := strings.SplitN(bytes.NewBuffer(b).String(), ":", 2)
	if len(pair) != 2 {
		return nil, nil
	}
	username := strings.TrimSpace(pair[0])
	password := strings.TrimSpace(pair[1])
	if len(username) == 0 || len(password) == 0 {
		return nil, nil
	}
	return &authData{Username: username, Password: password}, nil
}
