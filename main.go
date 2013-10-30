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
)

var redisPool *redis.Pool

const keyControllers = "osp:controllers"
const keyLogs = "osp:logs"

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
		ID int64 `json:"id"`
	}
	Tick struct {
		Datetime        time.Time `json:"datetime"`
		SensorID        int64     `json:"sensor_id"`
		NextDataSession string    `json:"next_data_session,omitempty"` // sec
		BatteryVoltage  string    `json:"battery_voltage,omitempty"`   // mV
		Sensor1         string    `json:"sensor1,omitempty"`
		Sensor2         string    `json:"sensor2,omitempty"`
		RadioQuality    string    `json:"radio_quality,omitempty"` // (LQI=0..255)
	}
	PaginatedTicks struct {
		Ticks []*Tick `json:"ticks"`
		Total int     `json:"total"`
	}
)

func main() {
	flag.Parse()

	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()

	http.HandleFunc("/api/controllers", getControllers)
	http.HandleFunc("/api/controllers/{controller_id}", putController)
	http.HandleFunc("/api/controllers/{controller_id}/sensors", getControllerSensors)
	http.HandleFunc("/api/sensors/{sensor_id}/ticks", getSensorTicks)
	http.HandleFunc("/api/log", getLogs)
	http.HandleFunc("/api/logs", getLogs)

	if *environment == "production" || *environment == "test" {
		f, err := os.OpenFile(filepath.Join("log", *environment+".log"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
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

	log.Println("HTTP server started on port", *webserverPort)
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
			log.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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

func getControllerSensors(w http.ResponseWriter, r *http.Request) {
	controllerID, ok := mux.Vars(r)["controller_id"]
	if !ok {
		http.Error(w, "Missing controller_id", http.StatusBadRequest)
		return
	}

	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("SMEMBERS", keyOfControllerSensors(controllerID))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sensors := make([]*Sensor, 0)
	for _, b := range bb.([][]byte) {
		sensorID, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sensor := &Sensor{ID: sensorID}
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

func getSensorTicks(w http.ResponseWriter, r *http.Request) {
	// Parse sensor ID
	s, ok := mux.Vars(r)["sensor_id"]
	if !ok {
		http.Error(w, "Missing sensor_id", http.StatusBadRequest)
		return
	}
	sensorID, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse start index of tick range
	startIndex, err := strconv.Atoi(r.FormValue("start_index"))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse stop index of tick range
	stopIndex, err := strconv.Atoi(r.FormValue("stop_index"))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Find ticks in the given start index - stop index range
	result, err := FindTicks(sensorID, startIndex, stopIndex)
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
		BatteryVoltage:  parts[3],
		Sensor1:         parts[4],
		Sensor2:         parts[5],
		RadioQuality:    parts[6],
	}
	return tick, err
}

func FindTicks(sensorID int64, startIndex, stopIndex int) (*PaginatedTicks, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	total, err := redis.Int(redisClient.Do("ZCARD", keyOfSensorTicks(sensorID)))
	if err != nil {
		return nil, err
	}

	bb, err := redisClient.Do("ZREVRANGE", keyOfSensorTicks(sensorID), startIndex, stopIndex)
	if err != nil {
		return nil, err
	}

	result := PaginatedTicks{Total: total}
	for _, b := range bb.([][]byte) {
		var tick Tick
		if err := json.Unmarshal(b, &tick); err != nil {
			return nil, err
		}
		result.Ticks = append(result.Ticks, &tick)
	}

	return &result, nil
}

func (tick Tick) Save() error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	b, err := json.Marshal(tick)
	if err != nil {
		return err
	}

	_, err = redisClient.Do("ZADD", tick.key(), tick.rank(), b)
	return err
}

func (tick Tick) rank() float64 {
	return float64(tick.Datetime.Unix())
}

func (tick Tick) key() string {
	return keyOfSensorTicks(tick.SensorID)
}

func (tick Tick) String() string {
	return fmt.Sprintf("datetime: %v, sensor ID: %d, next: %s, battery: %s, sensor1: %s, sensor2: %s, radio: %s",
		tick.Datetime, tick.SensorID, tick.NextDataSession, tick.BatteryVoltage, tick.Sensor1, tick.Sensor2, tick.RadioQuality)
}

func (controller Controller) key() string {
	return keyOfController(controller.ID)
}

func ProcessTicks(tickList string) (int, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	tickList = strings.Replace(tickList, "\r", "\n", -1)
	registeredSensorIds := make(map[string]map[int64]bool)
	processedCount := 0
	for _, s := range strings.Split(tickList, "\n") {
		if len(s) == 0 {
			continue
		}
		tick, err := NewTick(s)
		if err != nil {
			return 0, err
		}
		if err := tick.Save(); err != nil {
			return 0, err
		}
		log.Println("Saved:", tick)
		processedCount += 1

		// controller ID will be sent 
		controllerID := "myfancycontroller"

		// Register sensor for later lookup
		if _, exists := registeredSensorIds[controllerID]; !exists {
			registeredSensorIds[controllerID] = make(map[int64]bool)
			if _, err := redisClient.Do("SADD", keyControllers, controllerID); err != nil {
				return 0, err
			}
		}
		sensorMap := registeredSensorIds[controllerID]
		if _, exists := sensorMap[tick.SensorID]; !exists {
			if _, err := redisClient.Do("SADD",
				keyOfControllerSensors(controllerID), fmt.Sprintf("%d", tick.SensorID)); err != nil {
				return 0, err
			}
			sensorMap[tick.SensorID] = true
		}
	}
	return processedCount, nil
}
