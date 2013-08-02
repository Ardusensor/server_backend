package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
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
	logtofile     = flag.Bool("logtofile", false, "write log to server.log")
	port          = flag.Int("port", 8090, "TCP port to listen on")
	webserverPort = flag.Int("webserver_port", 8084, "TCP port to listen on")
	environment   = flag.String("environment", "development", "environment")
	redisHost     = flag.String("redis", "127.0.0.1:6379", "host:ip of Redis instance")
)

var redisPool *redis.Pool

type (
	// Sensor is a unit on the ground, sending us measurements
	Sensor struct {
		ID int64 `json:"id"`
	}
	// Entry is a measurement collected from a sensor
	Entry struct {
		Datetime        time.Time `json:"datetime"`
		SensorID        int64     `json:"sensor_id"`
		NextDataSession string    `json:"next_data_session,omitempty"` // sec
		BatteryVoltage  string    `json:"battery_voltage,omitempty"`   // mV
		Sensor1         string    `json:"sensor1,omitempty"`
		Sensor2         string    `json:"sensor2,omitempty"`
		RadioQuality    string    `json:"radio_quality,omitempty"` // (LQI=0..255)
	}
	// PaginatedEntries is for paginating entries
	PaginatedEntries struct {
		Entries []*Entry `json:"entries"`
		Total   int      `json:"total"`
	}
)

func main() {
	flag.Parse()

	redisPool = getRedisPool(*redisHost)
	defer redisPool.Close()

	r := mux.NewRouter()

	r.HandleFunc("/api/v1/sensors", handleGetSensors).Methods("GET")
	r.HandleFunc("/api/v1/sensors/{sensor_id}/entries", handleGetSensorEntries).Methods("GET")
	r.HandleFunc("/log", handleGetLogs).Methods("GET")
	r.HandleFunc("/logs", handleGetLogs).Methods("GET")
	http.Handle("/", r)

	if *logtofile {
		f, err := os.OpenFile(filepath.Join("log", "sensor_server.log"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
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
		if _, err := redisClient.Do("LPUSH", "sensor_server:logs", time.Now().String()+" "+buf.String()); err != nil {
			log.Println(err)
			return
		}
		if _, err := redisClient.Do("LTRIM", "sensor_server:logs", 0, 1000); err != nil {
			log.Println(err)
			return
		}
	}()

	start := time.Now()
	count, err := ProcessEntries(buf.String())
	if err != nil {
		log.Println("Error while processing entries:", err)
		return
	}
	log.Println("Processed", count, "entries in", time.Since(start))
}

func handleGetLogs(w http.ResponseWriter, r *http.Request) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("LRANGE", "sensor_server:logs", 0, 1000)
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

func handleGetSensors(w http.ResponseWriter, r *http.Request) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("SMEMBERS", "known_sensors")
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

func handleGetSensorEntries(w http.ResponseWriter, r *http.Request) {
	// Parse sensor ID
	s, ok := mux.Vars(r)["sensor_id"]
	if !ok {
		http.Error(w, "Missing sensor_id", http.StatusInternalServerError)
		return
	}
	sensorID, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Parse start index of entry range
	startIndex, err := strconv.Atoi(r.FormValue("start_index"))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Parse stop index of entry range
	stopIndex, err := strconv.Atoi(r.FormValue("stop_index"))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Find entries in the given start index - stop index range
	result, err := FindEntries(sensorID, startIndex, stopIndex)
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

// NewEntry parses input bytes and returns an Entry or error, if parse failed
func NewEntry(input string) (*Entry, error) {
	log.Println("NewEntry, input: ", input)
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
	entry := &Entry{
		Datetime:        datetime,
		SensorID:        sensorID,
		NextDataSession: parts[2],
		BatteryVoltage:  parts[3],
		Sensor1:         parts[4],
		Sensor2:         parts[5],
		RadioQuality:    parts[6],
	}
	return entry, err
}

// FindEntries finds sensor entries. Result is paginated.
func FindEntries(sensorID int64, startIndex, stopIndex int) (*PaginatedEntries, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	total, err := redis.Int(redisClient.Do("ZCARD", setKey(sensorID)))
	if err != nil {
		return nil, err
	}

	bb, err := redisClient.Do("ZREVRANGE", setKey(sensorID), startIndex, stopIndex)
	if err != nil {
		return nil, err
	}

	result := PaginatedEntries{Total: total}
	for _, b := range bb.([][]byte) {
		entry := &Entry{}
		if err := json.Unmarshal(b, &entry); err != nil {
			return nil, err
		}
		result.Entries = append(result.Entries, entry)
	}

	return &result, nil
}

// Save saves JSON-serialized entry to Redis
func (entry Entry) Save() error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = redisClient.Do("ZADD", entry.setKey(), entry.rank(), b)
	return err
}

func (entry Entry) rank() float64 {
	return float64(entry.Datetime.Unix())
}

func (entry Entry) setKey() string {
	return setKey(entry.SensorID)
}

func setKey(sensorID int64) string {
	return fmt.Sprintf("sensor:%d:entries", sensorID)
}

func (entry Entry) String() string {
	return fmt.Sprintf("datetime: %v, sensor ID: %d, next: %s, battery: %s, sensor1: %s, sensor2: %s, radio: %s",
		entry.Datetime, entry.SensorID, entry.NextDataSession, entry.BatteryVoltage, entry.Sensor1, entry.Sensor2, entry.RadioQuality)
}

// ProcessEntries parses
func ProcessEntries(entryList string) (int, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	entryList = strings.Replace(entryList, "\r", "\n", -1)
	registeredSensorIds := make(map[int64]bool)
	processedCount := 0
	for _, s := range strings.Split(entryList, "\n") {
		if len(s) == 0 {
			continue
		}
		entry, err := NewEntry(s)
		if err != nil {
			return 0, err
		}
		if err := entry.Save(); err != nil {
			return 0, err
		}
		log.Println("Saved:", entry)
		processedCount += 1
		// Register sensor for later lookup
		_, sensorRegistered := registeredSensorIds[entry.SensorID]
		if sensorRegistered {
			continue
		}
		if _, err := redisClient.Do("SADD", "known_sensors", fmt.Sprintf("%d", entry.SensorID)); err != nil {
			return 0, err
		}
		registeredSensorIds[entry.SensorID] = true
	}
	return processedCount, nil
}
