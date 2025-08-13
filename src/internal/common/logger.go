package common

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.SugaredLogger

func init() {
	config := zap.NewDevelopmentConfig()
	if viper.Get("loglevel") != nil {
		// if it is not set, by default will be 0 - info
		config.Level.SetLevel(zapcore.Level(viper.GetInt("loglevel")))
	}
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.Level.SetLevel(zapcore.InfoLevel)
    zapLogger, err := config.Build()
    defer func() { _ = zapLogger.Sync() }()
	if err != nil {
		panic(err)
	}
	Logger = zapLogger.Sugar()
}

// Logs an error and panics
func ReportError(err error, msg string) {
	if err != nil {
		Logger.Error(msg, " error: ", err)
	}
}
