package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/toggl/bugsnag"
)

func defineRoutes() {
	r := mux.NewRouter()

	api := r.PathPrefix("/api").Subrouter()

	coordinators := api.PathPrefix("/coordinators").Subrouter()
	coordinators.HandleFunc("/{coordinator_id}/sensors", getCoordinatorSensors).Methods("GET")
	coordinators.HandleFunc("/{coordinator_id}/readings", getCoordinatorReadings).Methods("GET")
	coordinators.HandleFunc("/{coordinator_id}/log", getCoordinatorLog).Methods("GET")
	coordinators.HandleFunc("/{coordinator_id}", putCoordinator).Methods("POST", "PUT")
	coordinators.HandleFunc("/{coordinator_id}/{hash}", getCoordinator).Methods("GET")

	sensors := api.PathPrefix("/sensors").Subrouter()
	sensors.HandleFunc("/{sensor_id}", putSensor).Methods("POST", "PUT")
	sensors.HandleFunc("/{sensor_id}/ticks", getSensorTicks).Methods("GET")
	sensors.HandleFunc("/{sensor_id}/dots", getSensorDots).Methods("GET")

	api.HandleFunc("/admin/coordinators", getAdminCoordinators).Methods("GET")

	api.HandleFunc("/v2/log", getJSONLogs).Methods("GET")
	api.HandleFunc("/v2/logs", getJSONLogs).Methods("GET")

	http.Handle("/", r)
}

func getAdminCoordinators(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

	auth, err := parseToken(r)
	if err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if auth == nil {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"Ardusensor admin\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if auth.Username != *adminUsername || auth.Password != *adminPassword {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"Ardusensor admin\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	coordinators, err := coordinators()
	if err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := json.MarshalIndent(coordinators, "", "\t")
	if err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func getCoordinatorLog(w http.ResponseWriter, r *http.Request) {
	coordinatorID, err := strconv.Atoi(mux.Vars(r)["coordinator_id"])
	if err != nil {
		http.Error(w, "Missing or invalid coordinator_id", http.StatusBadRequest)
		return
	}
	writeLogs(w, r, loggingKeyJSON, coordinatorID)
}

func getCoordinator(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

	coordinatorID, ok := mux.Vars(r)["coordinator_id"]
	if !ok {
		http.Error(w, "Missing coordinator_id", http.StatusBadRequest)
		return
	}
	hashToken, ok := mux.Vars(r)["hash"]
	if !ok {
		http.Error(w, "Missing token hash", http.StatusBadRequest)
		return
	}

	c, err := loadCoordinator(coordinatorID)
	if err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if hashToken != c.Token {
		http.Error(w, "Incorrect token for this coordinator", http.StatusUnauthorized)
		return
	}

	b, err := json.Marshal(c)
	if err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func putCoordinator(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

	coordinatorID, ok := mux.Vars(r)["coordinator_id"]
	if !ok {
		http.Error(w, "Missing coordinator_id", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var c coordinator
	if err := json.Unmarshal(b, &c); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := setCoordinatorLabel(coordinatorID, c.Label); err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func putSensor(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

	sensorID, exists := mux.Vars(r)["sensor_id"]
	if !exists {
		http.Error(w, "Missing or invalid sensor_id", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var s sensor
	if err := json.Unmarshal(b, &s); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.ID = sensorID

	if err := s.save(); err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, err = json.Marshal(s)
	if err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func getCoordinatorSensors(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

	coordinatorID, ok := mux.Vars(r)["coordinator_id"]
	if !ok {
		http.Error(w, "Missing coordinator_id", http.StatusBadRequest)
		return
	}

	sensors, err := sensorsOfCoordinator(coordinatorID)
	if err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := json.Marshal(sensors)
	if err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func getSensorDots(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

	sensorID, exists := mux.Vars(r)["sensor_id"]
	if !exists {
		http.Error(w, "Missing sensor_id", http.StatusBadRequest)
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

	ticks, err := findTicksByScore(sensorID, start, end)
	if err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var dots []*tick
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

	sensorID, exists := mux.Vars(r)["sensor_id"]
	if !exists {
		http.Error(w, "Missing sensor_id", http.StatusBadRequest)
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

	result, err := findTicksByScore(sensorID, start, end)
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

func getCoordinatorReadings(w http.ResponseWriter, r *http.Request) {
	log.Println(r)

	s, exists := mux.Vars(r)["coordinator_id"]
	if !exists {
		http.Error(w, "Missing coordinator_id", http.StatusBadRequest)
		return
	}
	coordinatorID, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		http.Error(w, "Invalid coordinator_id", http.StatusBadRequest)
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

	result, err := coordinatorReadings(coordinatorID, start, end)
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

func getJSONLogs(w http.ResponseWriter, r *http.Request) {
	writeLogs(w, r, loggingKeyJSON, 0)
}

func writeLogs(w http.ResponseWriter, r *http.Request, key string, coordinatorID int) {
	b, err := getLogs(key, coordinatorID)
	if err != nil {
		bugsnag.Notify(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Write(b)
}
