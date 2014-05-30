package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

type payload struct {
	Coordinator coordinatorReading `json:"coordinator"`
}

type sensorReading struct {
	SensorID          string `json:"sensor_id"`
	BatteryVoltage    int64  `json:"battery_voltage"`
	CPUTemperature    int64  `json:"cpu_temperature"`
	SensorTemperature int64  `json:"sensor_temperature"`
	Moisture          int64  `json:"moisture"`
	SendCounter       int64  `json:"sendcounter"`
}

type coordinatorReading struct {
	CoordinatorID  int64           `json:"coordinator_id"`
	GSMCoverage    int64           `json:"gsm_coverage"`
	BatteryVoltage int64           `json:"battery_voltage"`
	Uptime         int64           `json:"uptime"`
	FirstOverflow  int64           `json:"first_overflow"`
	Tries          int64           `json:"tries"`
	Successes      int64           `json:"successes"`
	SensorReadings []sensorReading `json:"sensor_readings,omitempty"`
	CreatedAt      *time.Time      `json:"created_at,omitempty"`
}

func (pl payload) convertToOldFormat() ([]*tick, error) {
	var ticks []*tick
	for _, sensorReading := range pl.Coordinator.SensorReadings {
		t := &tick{
			coordinatorID: fmt.Sprintf("%d", pl.Coordinator.CoordinatorID),
			Datetime:      time.Now(),
			Version:       3,
			SensorID:      sensorReading.SensorID,
			Humidity:      sensorReading.Moisture,
			Sendcounter:   sensorReading.SendCounter,
		}
		sensor, err := loadSensor(t.coordinatorID, sensorReading.SensorID)
		if err != nil {
			return nil, err
		}
		t.setTemperatureFromSensorReading(float64(sensorReading.SensorTemperature), sensor)
		t.setBatteryVoltageFromSensorReading(float64(sensorReading.BatteryVoltage))
		ticks = append(ticks, t)
	}
	return ticks, nil
}

func saveCoordinatorReading(cr coordinatorReading) error {
	log.Println("Saving coordinator reading", cr)

	if !(cr.CoordinatorID > 0) {
		return errors.New("Cannot save coordinator reading without coordinator ID")
	}

	if cr.CreatedAt == nil {
		now := time.Now()
		cr.CreatedAt = &now
	}

	b, err := json.Marshal(cr)
	if err != nil {
		return err
	}

	if err := saveReading(keyOfCoordinatorReadings(cr.CoordinatorID), float64(cr.CreatedAt.Unix()), b); err != nil {
		return err
	}

	return nil
}
