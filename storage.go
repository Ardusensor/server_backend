package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

var redisPool *redis.Pool

const keyCoordinators = "osp:controllers"
const keySensorToController = "osp:sensor_to_controller"
const loggingKeyCSV = "osp:logs"
const loggingKeyJSON = "osp:logs:v2"

func keyOfSensor(sensorID string) string {
	return fmt.Sprintf("osp:sensor:%s:fields", sensorID)
}

func keyOfCoordinator(coordinatorID string) string {
	return "osp:controller:" + coordinatorID + ":fields"
}

func keyOfCoordinatorSensors(coordinatorID string) string {
	return "osp:controller:" + coordinatorID + ":sensors"
}

func keyOfSensorTicks(sensorID string) string {
	return fmt.Sprintf("osp:sensor:%s:ticks", sensorID)
}

func keyOfCoordinatorReadings(coordinatorID int64) string {
	return fmt.Sprintf("osp:coordinator:%v:readings", coordinatorID)
}

func saveLog(buf *bytes.Buffer, loggingKey string) error {
	redisClient := redisPool.Get()
	defer redisClient.Close()
	if _, err := redisClient.Do("LPUSH", loggingKey, time.Now().String()+" "+buf.String()); err != nil {
		return err
	}
	if _, err := redisClient.Do("LTRIM", loggingKey, 0, 1000); err != nil {
		return err
	}
	return nil
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

func findCoordinatorIDBySensorID(sensorID string) (string, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()
	id, err := redis.String(redisClient.Do("HGET", keySensorToController, sensorID))
	if err != nil && err != redis.ErrNil {
		return "", err
	}
	return id, nil
}

func findTicksByRange(sensorID string, startIndex, stopIndex int) ([]*tick, error) {
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

func coordinatorReadings(coordinatorID int64, startIndex, stopIndex int) ([]*coordinatorReading, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("ZREVRANGE", keyOfCoordinatorReadings(coordinatorID), startIndex, stopIndex)
	if err != nil {
		return nil, err
	}

	var result []*coordinatorReading
	for _, value := range bb.([]interface{}) {
		b := value.([]byte)
		var cr coordinatorReading
		if err := json.Unmarshal(b, &cr); err != nil {
			return nil, err
		}
		result = append(result, &cr)
	}

	return result, nil
}

func findTicksByScore(sensorID string, start, end int) ([]*tick, error) {
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

func saveReading(key string, score float64, b []byte) error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	_, err := redisClient.Do("ZADD", key, score, b)
	return err
}

func coordinators() ([]*coordinator, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	ids, err := redis.Strings(redisClient.Do("SMEMBERS", keyCoordinators))
	if err != nil {
		if err == redis.ErrNil {
			return nil, nil
		}
		return nil, err
	}

	var result []*coordinator
	for _, id := range ids {
		c, err := loadCoordinator(id)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}

	return result, nil
}

func setCoordinatorToken(coordinatorID string) error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	if _, err := redisClient.Do("SADD", keyCoordinators, coordinatorID); err != nil {
		return err
	}
	if _, err := redisClient.Do("HSET", keyOfCoordinator(coordinatorID), "token", tokenForCoordinator(coordinatorID)); err != nil {
		return err
	}
	return nil
}

func loadCoordinator(coordinatorID string) (*coordinator, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	fields, err := redis.Strings(redisClient.Do("HGETALL", keyOfCoordinator(coordinatorID)))
	if err != nil {
		if redis.ErrNil == err {
			return nil, nil
		}
		return nil, err
	}

	c := &coordinator{}
	var fieldName string
	for i, field := range fields {
		if 0 == i%2 {
			fieldName = field
			continue
		}
		switch fieldName {
		case "token":
			c.Token = field
		case "label":
			c.Label = field
		}
	}

	c.ID = coordinatorID
	c.URL = fmt.Sprintf("http://ardusensor.com/index.html#/%s/%s", coordinatorID, c.Token)
	c.LogURL = fmt.Sprintf("http://ardusensor.com/api/coordinators/%s/log", coordinatorID)

	return c, nil
}

func setCoordinatorLabel(coordinatorID, label string) error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	if _, err := redisClient.Do("SADD", keyCoordinators, coordinatorID); err != nil {
		return err
	}
	_, err := redisClient.Do("HSET", keyOfCoordinator(coordinatorID), "label", label)
	if err != nil {
		return err
	}

	return nil
}

func addSensorToCoordinator(sensorID, coordinatorID string) error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	if _, err := redisClient.Do("HSET", keySensorToController, sensorID, coordinatorID); err != nil {
		return err
	}
	if _, err := redisClient.Do("SADD", keyOfCoordinatorSensors(coordinatorID), sensorID); err != nil {
		return err
	}
	return nil
}

func (s sensor) save() error {
	if len(s.ID) == 0 {
		return errors.New("missing sensor ID")
	}
	redisClient := redisPool.Get()
	defer redisClient.Close()

	_, err := redisClient.Do("HMSET", keyOfSensor(s.ID), "lat", s.Lat, "lng", s.Lng, "label", s.Label)
	if err != nil {
		return err
	}
	return err
}

func sensorsOfCoordinator(coordinatorID string) ([]*sensor, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	ids, err := redis.Strings(redisClient.Do("SMEMBERS", keyOfCoordinatorSensors(coordinatorID)))
	if err != nil {
		if err == redis.ErrNil {
			return nil, nil
		}
		return nil, err
	}

	sensors := make([]*sensor, 0)
	for _, sensorID := range ids {
		if len(sensorID) == 0 {
			return nil, errors.New("Invalid or missing sensor ID")
		}

		// Get lat, lng of sensor
		list, err := redis.Strings(redisClient.Do("HMGET", keyOfSensor(sensorID), "lat", "lng", "label"))
		if err != nil {
			return nil, err
		}
		s := &sensor{
			ID:           sensorID,
			ControllerID: coordinatorID,
			Lat:          list[0],
			Lng:          list[1],
			Label:        list[2],
		}

		// Get last tick of sensor
		ticks, err := findTicksByRange(sensorID, 0, 0)
		if err != nil {
			return nil, err
		}
		if len(ticks) > 0 {
			s.LastTick = &ticks[0].Datetime

		}

		sensors = append(sensors, s)
	}
	return sensors, nil
}

func getLogs(key string, coordinatorID int) ([]byte, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	bb, err := redisClient.Do("LRANGE", key, 0, 1000)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)
	for _, item := range bb.([]interface{}) {
		s := string(item.([]byte))
		if coordinatorID == 0 || strings.Contains(s, fmt.Sprintf("coordinator_id\":%d", coordinatorID)) {
			s = strconv.Quote(s)
			buf.WriteString(s)
			buf.WriteString("\n\r")
		}
	}

	return buf.Bytes(), nil
}
