package brpc

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/signal"
	"github.com/oldbai555/micro/bgin"
	"github.com/oldbai555/micro/brpc/middleware"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"os"
	"strings"
)

func StartH2CGrpcSrv(ctx context.Context, port uint32, registerFunc func(server *grpc.Server), interceptors ...grpc.UnaryServerInterceptor) error {
	interceptors = append(interceptors, middleware.Recover())
	interceptors = append(interceptors, middleware.AutoValidate())

	// 新建gRPC服务器实例
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(interceptors...),
	)

	registerFunc(grpcServer)

	// 初始化 http
	gin.DefaultWriter = log.GetWriter()
	router := gin.Default()
	router.Any("/*path", bgin.NotFoundGrpcRouter())

	// http + grpc 模式
	h2cHandler := h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 判断协议是否为 http/2 && 是 grpc
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			router.ServeHTTP(w, r)
		}
	}), &http2.Server{})
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: h2cHandler,
	}

	// 监听结束信号
	signal.RegV2(func(signal os.Signal) error {
		log.Infof("exit: close server connect, signal [%v]", signal)
		grpcServer.GracefulStop()
		err := srv.Shutdown(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
		}
		log.Infof("exit: close server ok")
		return nil
	})

	// 启动服务
	err := srv.ListenAndServe()
	if err != nil {
		log.Warnf("err is %v", err)
		return err
	}
	return nil
}

type RegisterFunc func(server *grpc.Server) error

type Svr struct {
	name         string
	port         uint32
	rf           RegisterFunc
	interceptors []grpc.UnaryServerInterceptor

	grpcServer *grpc.Server
}

func NewSvr(name string, port uint32, rf RegisterFunc, interceptors ...grpc.UnaryServerInterceptor) *Svr {
	return &Svr{name: name, port: port, rf: rf, interceptors: interceptors}
}

func (s *Svr) StartGrpcSrv(_ context.Context) error {
	// 单grpc模式-监听端口
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		log.Errorf("net.Listen err: %v", err)
		return err
	}

	var defaultInterceptors = []grpc.UnaryServerInterceptor{middleware.Recover(), middleware.AutoValidate()}
	defaultInterceptors = append(defaultInterceptors, s.interceptors...)

	// 新建gRPC服务器实例
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(defaultInterceptors...),
	)
	s.grpcServer = grpcServer

	// 注册方法
	if s.rf != nil {
		err = s.rf(grpcServer)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	// 监听结束信号
	signal.RegV2(func(signal os.Signal) error {
		log.Infof("exit: close %s server connect, signal [%v]", s.name, signal)
		grpcServer.GracefulStop()
		log.Infof("exit: close %s server ok", s.name)
		return nil
	})

	log.Infof("====== start grpc server %s , port is %d ======", s.name, s.port)

	// 单grpc模式-启动 grpc 服务
	if err := grpcServer.Serve(listener); err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (s *Svr) Stop() {
	if s.grpcServer == nil {
		return
	}
	s.grpcServer.GracefulStop()
}
