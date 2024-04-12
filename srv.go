package micro

import (
	"context"
	"fmt"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/routine"
	"github.com/oldbai555/micro/bcmd"
	"github.com/oldbai555/micro/bgin/gate"
	"github.com/oldbai555/micro/bprometheus"
	"github.com/oldbai555/micro/brpc"
	"github.com/oldbai555/micro/brpc/reg"
	"google.golang.org/grpc"
)

// 聚合 grpc server 和 网关功能
// 可以单独拎出去 自己组装

type GrpcWithGateSrv struct {
	ip             string
	name           string
	port           uint32
	gatePort       uint32
	prometheusPort uint32

	rf            brpc.RegisterFunc
	checkAuthFunc gate.CheckAuthFunc
	cmdList       []*bcmd.Cmd
	interceptors  []grpc.UnaryServerInterceptor

	useDefaultSrvReg bool
}

func NewGrpcWithGateSrv(name, ip string, port uint32, opts ...Option) *GrpcWithGateSrv {
	s := &GrpcWithGateSrv{name: name, ip: ip, port: port}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type Option func(*GrpcWithGateSrv)

func WithCheckAuthFunc(checkAuthFunc gate.CheckAuthFunc) Option {
	return func(gateSrv *GrpcWithGateSrv) {
		gateSrv.checkAuthFunc = checkAuthFunc
	}
}

func WithCmdList(cmdList []*bcmd.Cmd) Option {
	return func(gateSrv *GrpcWithGateSrv) {
		gateSrv.cmdList = cmdList
	}
}

func WithRegisterFunc(rf brpc.RegisterFunc) Option {
	return func(gateSrv *GrpcWithGateSrv) {
		gateSrv.rf = rf
	}
}

func WithUnaryServerInterceptors(list ...grpc.UnaryServerInterceptor) Option {
	return func(gateSrv *GrpcWithGateSrv) {
		gateSrv.interceptors = list
	}
}

func WithUseDefaultSrvReg() Option {
	return func(gateSrv *GrpcWithGateSrv) {
		gateSrv.useDefaultSrvReg = true
	}
}

func WithGatePort(gatePort uint32) Option {
	return func(gateSrv *GrpcWithGateSrv) {
		gateSrv.gatePort = gatePort
	}
}

func WithPrometheusPort(prometheusPort uint32) Option {
	return func(gateSrv *GrpcWithGateSrv) {
		gateSrv.prometheusPort = prometheusPort
	}
}

func (s *GrpcWithGateSrv) Start(ctx context.Context) error {
	grpcSrv := brpc.NewSvr(s.name, s.port, s.rf, s.interceptors...)
	gateSrv := gate.NewSvr(s.name, s.genGatePort(), s.cmdList, s.checkAuthFunc)
	defer func() {
		grpcSrv.Stop()
		gateSrv.Stop()
	}()

	// 启动 grpc
	routine.GoV2(func() error {
		err := grpcSrv.StartGrpcSrv(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	})

	if s.useDefaultSrvReg {
		// 服务注册
		err := reg.V2(ctx, s.ip, s.name, int(s.port), fmt.Sprintf("%d", s.genGatePort()))
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	// 启动监控
	routine.GoV2(func() error {
		err := bprometheus.StartPrometheusMonitor("", s.genPrometheusPort())
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	})

	// 启动 网关
	err := gateSrv.StartSrv(ctx)
	if err != nil {
		log.Warnf("err:%v", err)
		return err
	}

	return nil
}

func (s *GrpcWithGateSrv) genGatePort() uint32 {
	return s.port + 100
}

func (s *GrpcWithGateSrv) genPrometheusPort() uint32 {
	return s.port + 1000
}
