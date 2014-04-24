package main

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
