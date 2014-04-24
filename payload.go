package main

// Payload is sent from coordinator; contains sensor readings
type Payload struct {
	Coordinator struct {
		CoordinatorID  int64 `json:"coordinator_id"`
		GSMCoverage    int64 `json:"gsm_coverage"`
		BatteryVoltage int64 `json:"battery_voltage"`
		Uptime         int64 `json:"uptime"`
		FirstOverflow  int64 `json:"first_overflow"`
		Tries          int64 `json:"tries"`
		Successes      int64 `json:"successes"`
		SensorReadings []struct {
			SensorID          string `json:"sensor_id"`
			BatteryVoltage    int64  `json:"battery_voltage"`
			CPUTemperature    int64  `json:"cpu_temperature"`
			SensorTemperature int64  `json:"sensor_temperature"`
			Moisture          int64  `json:"moisture"`
			SendCounter       int64  `json:"sendcounter"`
		} `json:"sensor_readings"`
	} `json:"coordinator"`
}
