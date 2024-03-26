package blimiter

import (
	"github.com/didip/tollbooth/limiter"
	"time"
)

const Max = 100

func DefaultExpiredAbleOptions() *limiter.ExpirableOptions {
	return &limiter.ExpirableOptions{
		DefaultExpirationTTL: time.Second,
		ExpireJobInterval:    time.Second * 5,
	}
}
