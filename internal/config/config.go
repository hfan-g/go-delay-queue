package config

import (
	"fmt"
	"os"
	"time"

	"feng/delay-queue/internal/wheel"

	"go.yaml.in/yaml/v2"
)

type Config struct {
	HTTP      HTTPConfig      `yaml:"http"`
	Redis     RedisConfig     `yaml:"redis"`
	Executor  ExecutorConfig  `yaml:"executor"`
	Wheel     WheelConfig     `yaml:"wheel"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Logger    LoggerConfig    `yaml:"logger"`
}

type HTTPConfig struct {
	Addr         string        `yaml:"addr"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
}

type RedisConfig struct {
	Addr         string `yaml:"addr"`
	Password     string `yaml:"password"`
	DB           int    `yaml:"db"`
	PoolSize     int    `yaml:"pool_size"`
	MinIdleConns int    `yaml:"minIdle_conns"`
}

type ExecutorConfig struct {
	PoolNum int `yaml:"pool_num"`
}

type WheelConfig struct {
	Layers []wheel.LayerConfig `yaml:"layers"`
}

type SchedulerConfig struct {
	RetryInterval time.Duration `yaml:"retry_interval"`
}

type LoggerConfig struct {
	Level      string `yaml:"level"`
	Path       string `yaml:"path"`
	MaxSize    int    `yaml:"max_size"`
	MaxAge     int    `yaml:"max_age"`
	MaxBackups int    `yaml:"max_backups"`
}

var config *Config

func InitConfig() error {
	data, err := os.ReadFile("conf.yaml")
	if err != nil {
		return fmt.Errorf("读取配置文件失败: %v", err.Error())
	}
	if err = yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("读取配置文件失败: %v", err.Error())
	}
	return nil
}

func GetConfig() *Config {
	return config
}
