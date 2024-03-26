package bprometheus

import (
	"context"
	"fmt"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/signal"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"
)

const PrometheusUrl = "/metrics"

func StartPrometheusMonitor(ip string, port uint32) error {
	srv := http.NewServeMux()
	srv.Handle(PrometheusUrl, promhttp.Handler())
	s := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", ip, port),
		Handler: srv,
	}

	signal.RegV2(func(signal os.Signal) error {
		log.Warnf("exit: close prometheus monitor, signal[%v]", signal)
		err := s.Shutdown(context.Background())
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	})

	log.Infof("====== start prometheus monitor, port is %d ======", port)
	err := s.ListenAndServe()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}
