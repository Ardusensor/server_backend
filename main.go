package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
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
	port          = flag.Int("port", 8090, "TCP port to listen on")
	webserverPort = flag.Int("webserver_port", 8084, "TCP port to listen on")
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
		ID   string `json:"id"`
		Name string `json:"name"`
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

	r := mux.NewRouter()
	r.HandleFunc("/api/controllers/{controller_id}/sensors", getControllerSensors).Methods("GET")
	r.HandleFunc("/api/controllers/{controller_id}", putController).Methods("POST", "PUT")
	r.HandleFunc("/api/controllers/{controller_id}", getController).Methods("GET")
	r.HandleFunc("/api/controllers", getControllers).Methods("GET")
	r.HandleFunc("/api/sensors/{sensor_id}", putSensor).Methods("POST", "PUT")
	r.HandleFunc("/api/sensors/{sensor_id}/ticks", getSensorTicks).Methods("GET")
	r.HandleFunc("/api/sensors/{sensor_id}/dots", getSensorDots).Methods("GET")
	r.HandleFunc("/api/log", getLogs).Methods("GET")
	r.HandleFunc("/api/logs", getLogs).Methods("GET")
	http.Handle("/", r)

	if *environment == "production" || *environment == "staging" {
		f, err := os.OpenFile(filepath.Join(*workdir, "log", *environment+".log"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Println("Upload server started on port", *port)
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println("Error while accepting connection:", err)
				continue
			}
			go handleConnection(conn)
		}
	}()

	log.Println("API started on port", *webserverPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *webserverPort), http.DefaultServeMux))
}

func handleConnection(conn net.Conn) {
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
	count, err := ProcessTicks(buf.String())
	if err != nil {
		log.Println("Error while processing ticks:", err)
		return
	}
	log.Println("Processed", count, "ticks in", time.Since(start))
}

func getLogs(w http.ResponseWriter, r *http.Request) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("LRANGE", keyLogs, 0, 1000)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	for _, b := range bb.([][]byte) {
		w.Write(b)
		w.Write([]byte("\n\r"))
	}
}

func getControllers(w http.ResponseWriter, r *http.Request) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	ids, err := redis.Strings(redisClient.Do("SMEMBERS", keyControllers))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	controllers := make([]*Controller, 0)
	for _, controllerID := range ids {
		controller := &Controller{ID: controllerID}
		controllerName, err := redis.String(redisClient.Do("HGET", controller.key(), "name"))
		if err != nil {
			if err != redis.ErrNil {
				log.Println(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			controllerName = controller.ID
		}
		controller.Name = controllerName
		controllers = append(controllers, controller)
	}

	b, err := json.Marshal(controllers)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func getController(w http.ResponseWriter, r *http.Request) {
	controllerID, ok := mux.Vars(r)["controller_id"]
	if !ok {
		http.Error(w, "Missing controller_id", http.StatusBadRequest)
		return
	}

	redisClient := redisPool.Get()
	defer redisClient.Close()

	controller := &Controller{ID: controllerID}
	controllerName, err := redis.String(redisClient.Do("HGET", controller.key(), "name"))
	if err != nil {
		if err != redis.ErrNil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		controllerName = controller.ID
	}
	controller.Name = controllerName

	b, err := json.Marshal(controller)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func putController(w http.ResponseWriter, r *http.Request) {
	controllerID, ok := mux.Vars(r)["controller_id"]
	if !ok {
		http.Error(w, "Missing controller_id", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var controller Controller
	if err := json.Unmarshal(b, &controller); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	controller.ID = controllerID

	redisClient := redisPool.Get()
	defer redisClient.Close()

	_, err = redisClient.Do("HSET", controller.key(), "name", controller.Name)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func putSensor(w http.ResponseWriter, r *http.Request) {
	sensorID, err := strconv.ParseInt(mux.Vars(r)["sensor_id"], 10, 64)
	if err != nil {
		http.Error(w, "Missing or invalid sensor_id", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var sensor Sensor
	if err := json.Unmarshal(b, &sensor); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	redisClient := redisPool.Get()
	defer redisClient.Close()

	_, err = redisClient.Do("HMSET", keyOfSensor(sensorID), "lat", sensor.Lat, "lng", sensor.Lng)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getControllerSensors(w http.ResponseWriter, r *http.Request) {
	controllerID, ok := mux.Vars(r)["controller_id"]
	if !ok {
		http.Error(w, "Missing controller_id", http.StatusBadRequest)
		return
	}

	redisClient := redisPool.Get()
	defer redisClient.Close()

	ids, err := redis.Strings(redisClient.Do("SMEMBERS", keyOfControllerSensors(controllerID)))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sensors := make([]*Sensor, 0)
	for _, sensorID := range ids {
		sensorID, err := strconv.ParseInt(sensorID, 10, 64)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sensor := &Sensor{ID: sensorID, ControllerID: controllerID}

		// Get lat, lng of sensor
		bb, err := redisClient.Do("HMGET", keyOfSensor(sensorID), "lat", "lng")
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if bb != nil {
			list := bb.([]interface{})
			if len(list) > 0 {
				if list[0] != nil {
					sensor.Lat = string(list[0].([]byte))
				}
				if list[1] != nil {
					sensor.Lng = string(list[1].([]byte))
				}
			}
		}

		// Get last tick of sensor
		ticks, err := FindTicksByRange(sensorID, 0, 0)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(ticks) > 0 {
			sensor.LastTick = &ticks[0].Datetime

		}

		sensors = append(sensors, sensor)
	}

	b, err := json.Marshal(sensors)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func unmarshalTick(b []byte) (*Tick, error) {
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
		return nil, err
	}
	tick.NextDataSession = values["next_data_session"].(string)
	if tick.BatteryVoltage, err = parseFloat(values["battery_voltage"]); err != nil {
		return nil, err
	}
	if tick.Sensor1, err = parseInt(values["sensor1"]); err != nil {
		return nil, err
	}
	if tick.Sensor2, err = parseInt(values["sensor2"]); err != nil {
		return nil, err
	}
	if tick.RadioQuality, err = parseInt(values["radio_quality"]); err != nil {
		return nil, err
	}
	return &tick, nil
}

func parseFloat(value interface{}) (float64, error) {
	if s, isString := value.(string); isString {
		return strconv.ParseFloat(s, 64)
	} else if n, isNumeric := value.(float64); isNumeric {
		return float64(n), nil
	}
	return 0, nil
}

func parseInt(value interface{}) (int64, error) {
	if s, isString := value.(string); isString {
		return strconv.ParseInt(s, 10, 64)
	} else if n, isNumeric := value.(float64); isNumeric {
		return int64(n), nil
	}
	return 0, nil
}

func getSensorDots(w http.ResponseWriter, r *http.Request) {
	sensorID, err := strconv.ParseInt(mux.Vars(r)["sensor_id"], 10, 64)
	if err != nil {
		http.Error(w, "Invalid sensor_id", http.StatusBadRequest)
		return
	}

	start, err := strconv.Atoi(r.FormValue("start"))
	if err != nil {
		http.Error(w, "Invalid start", http.StatusBadRequest)
		return
	}

	end, err := strconv.Atoi(r.FormValue("end"))
	if err != nil {
		http.Error(w, "Invalid end", http.StatusBadRequest)
		return
	}

	dotsPerDay, err := strconv.Atoi(r.FormValue("dots_per_day"))
	if err != nil {
		http.Error(w, "Invalid dots_per_day", http.StatusBadRequest)
		return
	}
	if dotsPerDay < 0 || dotsPerDay > 24 {
		http.Error(w, "dots_per_day must be in range 0-24", http.StatusBadRequest)
		return
	}

	redisClient := redisPool.Get()
	defer redisClient.Close()

	ticks, err := FindTicksByScore(sensorID, start, end)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var dots []*Tick
	if dotsPerDay > 0 {
		dots = findAverages(ticks, dotsPerDay, start, end)
	} else {
		dots = ticks
	}

	b, err := json.Marshal(dots)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
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

func getSensorTicks(w http.ResponseWriter, r *http.Request) {
	// Parse sensor ID
	sensorID, err := strconv.ParseInt(mux.Vars(r)["sensor_id"], 10, 64)
	if err != nil {
		http.Error(w, "Missing or invalid sensor_id", http.StatusBadRequest)
		return
	}

	// Parse start index of tick range
	start, err := strconv.Atoi(r.FormValue("start"))
	if err != nil {
		http.Error(w, "Missing or invalid start", http.StatusBadRequest)
		return
	}

	// Parse stop index of tick range
	end, err := strconv.Atoi(r.FormValue("end"))
	if err != nil {
		http.Error(w, "Missing or invalid end", http.StatusBadRequest)
		return
	}

	result, err := FindTicksByScore(sensorID, start, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := json.Marshal(result)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
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
	if len(parts) >= 8 {
		tick.controllerID = parts[7]
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
		tick, err := unmarshalTick(value.([]byte))
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
		tick, err := unmarshalTick(value.([]byte))
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

func ProcessTicks(tickList string) (int, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	tickList = strings.Replace(tickList, "\r", "\n", -1)
	processedCount := 0
	for _, s := range strings.Split(tickList, "\n") {
		if len(s) == 0 {
			continue
		}
		err := processTick(redisClient, s)
		if err != nil {
			return 0, err
		}
		processedCount += 1
	}

	return processedCount, nil
}

func processTick(redisClient redis.Conn, s string) error {
	tick, err := NewTick(s)
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

	if _, err := redisClient.Do("SADD", keyControllers, tick.controllerID); err != nil {
		return err
	}
	if _, err := redisClient.Do("HSET", keySensorToController, tick.SensorID, tick.controllerID); err != nil {
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
