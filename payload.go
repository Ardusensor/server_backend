package main

import (
	"fmt"
	"time"
)

type payload struct {
	Coordinator coordinator `json:"coordinator"`
}

type sensorReading struct {
	SensorID          string `json:"sensor_id"`
	BatteryVoltage    int64  `json:"battery_voltage"`
	CPUTemperature    int64  `json:"cpu_temperature"`
	SensorTemperature int64  `json:"sensor_temperature"`
	Moisture          int64  `json:"moisture"`
	SendCounter       int64  `json:"sendcounter"`
}

type coordinator struct {
	CoordinatorID  int64           `json:"coordinator_id"`
	GSMCoverage    int64           `json:"gsm_coverage"`
	BatteryVoltage int64           `json:"battery_voltage"`
	Uptime         int64           `json:"uptime"`
	FirstOverflow  int64           `json:"first_overflow"`
	Tries          int64           `json:"tries"`
	Successes      int64           `json:"successes"`
	SensorReadings []sensorReading `json:"sensor_readings"`
}

func (pl payload) convertToOldFormat() (*controllerReading, []*tick) {
	cr := &controllerReading{
		Datetime:       time.Now(),
		ControllerID:   fmt.Sprintf("%d", pl.Coordinator.CoordinatorID),
		GSMCoverage:    pl.Coordinator.GSMCoverage,
		BatteryVoltage: float64(pl.Coordinator.BatteryVoltage),
	}
	var ticks []*tick
	for _, sensorReading := range pl.Coordinator.SensorReadings {
		t := &tick{
			controllerID: cr.ControllerID,
			Datetime:     time.Now(),
			Version:      3,
			SensorID:     sensorReading.SensorID,
			Humidity:     sensorReading.Moisture,
			Sendcounter:  sensorReading.SendCounter,
		}
		t.setTemperatureFromSensorReading(float64(sensorReading.SensorTemperature))
		t.setBatteryVoltageFromSensorReading(float64(sensorReading.BatteryVoltage))
		ticks = append(ticks, t)
	}
	return cr, ticks
}
