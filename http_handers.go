package main

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

func defineRoutes() {
	r := mux.NewRouter()
	r.HandleFunc("/api/controllers/{controller_id}/sensors", getControllerSensors).Methods("GET")
	r.HandleFunc("/api/controllers/{controller_id}", putController).Methods("POST", "PUT")
	r.HandleFunc("/api/controllers/{controller_id}/{hash}", getController).Methods("GET")
	// r.HandleFunc("/api/controllers", getControllers).Methods("GET")
	r.HandleFunc("/api/sensors/{sensor_id}", putSensor).Methods("POST", "PUT")
	r.HandleFunc("/api/sensors/{sensor_id}/ticks", getSensorTicks).Methods("GET")
	r.HandleFunc("/api/sensors/{sensor_id}/dots", getSensorDots).Methods("GET")
	r.HandleFunc("/api/log", getLogs).Methods("GET")
	r.HandleFunc("/api/logs", getLogs).Methods("GET")
	http.Handle("/", r)
}

func getController(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

	controllerID, ok := mux.Vars(r)["controller_id"]
	if !ok {
		http.Error(w, "Missing controller_id", http.StatusBadRequest)
		return
	}
	hashToken, ok := mux.Vars(r)["hash"]
	if !ok {
		http.Error(w, "Missing token hash", http.StatusBadRequest)
		return
	}

	redisClient := redisPool.Get()
	defer redisClient.Close()

	controller := &Controller{ID: controllerID, Token: hashToken}

	controllerHash, err := redis.String(redisClient.Do("HGET", controller.key(), "token"))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if controllerHash != controller.Token {
		http.Error(w, "Incorrect hash for this controller", http.StatusUnauthorized)
	}

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
	log.Println(r)

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
	log.Println(r)

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
	log.Println(r)

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
			http.Error(w, "Invalid or missing sensor ID", http.StatusInternalServerError)
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

func getSensorDots(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

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

func getSensorTicks(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

	sensorID, err := strconv.ParseInt(mux.Vars(r)["sensor_id"], 10, 64)
	if err != nil {
		http.Error(w, "Missing or invalid sensor_id", http.StatusBadRequest)
		return
	}

	start, err := strconv.Atoi(r.FormValue("start"))
	if err != nil {
		http.Error(w, "Missing or invalid start", http.StatusBadRequest)
		return
	}

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

func getLogs(w http.ResponseWriter, r *http.Request) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("LRANGE", keyLogs, 0, 1000)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, item := range bb.([]interface{}) {
		s := string(item.([]byte))
		w.Write([]byte(s))
		w.Write([]byte("\n\r"))
	}
}

func getControllers(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

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
