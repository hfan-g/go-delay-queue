package logger

import (
	"io"
	"log/slog"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)


var logger *slog.Logger

func Init(level string, path string, maxSize int, maxAge int, maxBackups int) {
	var slogLevel slog.Level
	switch level {
	case "debug":
		slogLevel = slog.LevelDebug
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	wirter := io.MultiWriter(
		os.Stdout,
		&lumberjack.Logger{
			Filename:   path,
			MaxSize:    maxSize,
			MaxAge:     maxAge,
			MaxBackups: maxBackups,
			Compress:   true,
		},
	)

	logger = slog.New(slog.NewJSONHandler(wirter, &slog.HandlerOptions{
		Level: slogLevel,
	}))
}

func Get() *slog.Logger {
	return logger
}
