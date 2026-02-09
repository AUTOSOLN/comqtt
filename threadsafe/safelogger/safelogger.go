package safelogger

import (
	"log/slog"
	"sync"
)

type SafeLogger struct {
	mu        sync.RWMutex
	logger    *slog.Logger
	logcloser func()
}

type LogOpener func(args ...any) (*slog.Logger, func(), error)

func NewSafeLogger(logger *slog.Logger, logcloser func()) *SafeLogger {
	return &SafeLogger{
		logger:    logger,
		logcloser: logcloser,
	}
}

func (sl *SafeLogger) Swap(openerCb LogOpener, args ...any) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if sl.logcloser != nil {
		sl.logcloser()
	}

	newlyOpenedLog, newLogCloser, err := openerCb(args)
	if err != nil {
		return err
	}

	sl.logger = newlyOpenedLog
	sl.logcloser = newLogCloser

	return nil
}

func (sl *SafeLogger) Info(msg string, args ...any) {
	sl.mu.RLock()
	defer sl.mu.Unlock()
	if sl.logger != nil {
		sl.logger.Info(msg, args...)
	}
}

func (sl *SafeLogger) Error(msg string, args ...any) {
	sl.mu.RLock()
	defer sl.mu.Unlock()
	if sl.logger != nil {
		sl.logger.Error(msg, args...)
	}
}

func (sl *SafeLogger) Debug(msg string, args ...any) {
	sl.mu.RLock()
	defer sl.mu.Unlock()
	if sl.logger != nil {
		sl.logger.Debug(msg, args...)
	}
}

func (sl *SafeLogger) Warn(msg string, args ...any) {
	sl.mu.RLock()
	defer sl.mu.Unlock()
	if sl.logger != nil {
		sl.logger.Warn(msg, args...)
	}
}

/*
Note: I deliberately left this function commented out, because it makes a copy of the logger.
The issue with the copy is that we are unable to swap of the logger when the user makes
a configuration change without a more complicated design.

func (sl *SafeLogger) With(args ...any) *SafeLogger {
	sl.mu.RLock()
	defer sl.mu.Unlock()
	if sl.logger != nil {
		return NewSafeLogger(sl.logger.With(args...))
	}
	return nil
}
*/
