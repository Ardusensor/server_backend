package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

var redisPool *redis.Pool

const keyControllers = "osp:controllers"
const keySensorToController = "osp:sensor_to_controller"
const loggingKeyV1 = "osp:logs"
const loggingKeyV2 = "osp:logs:v2"
const debugLogKey = "osp:debug_logs"

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

func saveTick(key string, score float64, b []byte) error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	_, err := redisClient.Do("ZADD", key, score, b)
	return err
}

func saveCoordinatorToken(coordinatorID string) error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	if _, err := redisClient.Do("SADD", keyControllers, coordinatorID); err != nil {
		return err
	}
	if _, err := redisClient.Do("HSET", keyOfCoordinator(coordinatorID), "token", tokenForCoordinator(coordinatorID)); err != nil {
		return err
	}
	return nil
}

func coordinatorToken(coordinatorID string) (string, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	return redis.String(redisClient.Do("HGET", keyOfCoordinator(coordinatorID), "token"))
}

func saveCoordinatorName(coordinatorID, name string) error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	if _, err := redisClient.Do("SADD", keyControllers, coordinatorID); err != nil {
		return err
	}
	_, err := redisClient.Do("HSET", keyOfCoordinator(coordinatorID), "name", name)
	if err != nil {
		return err
	}

	return nil
}

func coordinatorName(coordinatorID string) (string, error) {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	name, err := redis.String(redisClient.Do("HGET", keyOfCoordinator(coordinatorID), "name"))
	if err != nil {
		if redis.ErrNil == err {
			return coordinatorID, nil
		}
		return "", err
	}
	return name, nil
}

func addSensorToCoordinator(sensorID, coordinatorID string) error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	if _, err := redisClient.Do("HSET", keySensorToController, sensorID, coordinatorID); err != nil {
		return err
	}
	if _, err := redisClient.Do("SADD",
		keyOfCoordinatorSensors(coordinatorID), sensorID); err != nil {
		return err
	}
	return nil
}

func saveSensorCoordinates(sensorID, latitude, longitude string) error {
	redisClient := redisPool.Get()
	defer redisClient.Close()

	_, err := redisClient.Do("HMSET", keyOfSensor(sensorID), "lat", latitude, "lng", longitude)
	if err != nil {
		return err
	}
	return err
}
