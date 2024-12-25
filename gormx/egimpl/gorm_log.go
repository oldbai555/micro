package egimpl

import (
	"context"
	"errors"
	"fmt"
	"github.com/oldbai555/lbtool/log"
	"gorm.io/gorm/logger"
	"time"
)

// Colors
const (
	Reset       = "\033[0m"
	Red         = "\033[31m"
	Green       = "\033[32m"
	Yellow      = "\033[33m"
	Blue        = "\033[34m"
	Magenta     = "\033[35m"
	Cyan        = "\033[36m"
	White       = "\033[37m"
	BlueBold    = "\033[34;1m"
	MagentaBold = "\033[35;1m"
	RedBold     = "\033[31;1m"
	YellowBold  = "\033[33;1m"
)

const (
	traceStr     = "%s [%.3fms] [rows:%v]"
	traceWarnStr = "%s %s [%.3fms] [rows:%v]"
	traceErrStr  = "%s %s [%.3fms] [rows:%v]"
)

// NewOrmLog initialize logger
func NewOrmLog(slowThreshold time.Duration) *ormlog {
	return &ormlog{
		slowThreshold: slowThreshold,
	}
}

type ormlog struct {
	slowThreshold time.Duration
	skipCall      int
}

func (l *ormlog) LogMode(level logger.LogLevel) logger.Interface {
	return l
}

func (l ormlog) Info(ctx context.Context, msg string, data ...interface{}) {
	log.GetLogger().SetSkipCall(l.skipCall)
	defer func() {
		log.GetLogger().SetSkipCall(log.DefaultSkipCall)
	}()
	log.Debugf(msg, data...)
}

func (l ormlog) Warn(ctx context.Context, msg string, data ...interface{}) {
	log.GetLogger().SetSkipCall(l.skipCall)
	defer func() {
		log.GetLogger().SetSkipCall(log.DefaultSkipCall)
	}()
	log.Warnf(msg, data...)
}

func (l ormlog) Error(ctx context.Context, msg string, data ...interface{}) {
	log.GetLogger().SetSkipCall(l.skipCall)
	defer func() {
		log.GetLogger().SetSkipCall(log.DefaultSkipCall)
	}()
	log.Errorf(msg, data...)
}

func (l ormlog) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	log.GetLogger().SetSkipCall(l.skipCall)
	defer func() {
		log.GetLogger().SetSkipCall(log.DefaultSkipCall)
	}()
	elapsed := time.Since(begin)
	switch {
	case err != nil && (!errors.Is(err, logger.ErrRecordNotFound)):
		sql, rows := fc()
		//utils.FileWithLineNum()
		if rows == -1 {
			l.Error(ctx, traceErrStr, err, sql, float64(elapsed.Nanoseconds())/1e6, "-")
		} else {
			l.Error(ctx, traceErrStr, err, sql, float64(elapsed.Nanoseconds())/1e6, rows, sql)
		}
	case elapsed > l.slowThreshold && l.slowThreshold != 0:
		sql, rows := fc()
		slowLog := fmt.Sprintf("SLOW SQL >= %v", l.slowThreshold)
		if rows == -1 {
			l.Warn(ctx, traceWarnStr, slowLog, sql, float64(elapsed.Nanoseconds())/1e6, "-")
		} else {
			l.Warn(ctx, traceWarnStr, slowLog, sql, float64(elapsed.Nanoseconds())/1e6, rows)
		}
	default:
		sql, rows := fc()
		if rows == -1 {
			l.Info(ctx, traceStr, sql, float64(elapsed.Nanoseconds())/1e6, "-")
		} else {
			l.Info(ctx, traceStr, sql, float64(elapsed.Nanoseconds())/1e6, rows)
		}
	}
}
