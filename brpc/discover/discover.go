package discover

import (
	"context"
	"fmt"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/etcdcfg"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/lbtool/pkg/signal"
	"github.com/oldbai555/micro/brpc/bresolver"
	"github.com/oldbai555/micro/brpc/middleware"
	eclient "go.etcd.io/etcd/client/v3"
	eresolver "go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"os"
	"time"
)

type InitGrpcClientFunc func(conn *grpc.ClientConn)

// V2 自定义服务发现
func V2(serverName string, f InitGrpcClientFunc) error {
	etcdTarget := fmt.Sprintf("%s:///%s", bresolver.ResolveSchema, serverName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 创建 grpc 连接代理
	conn, err := grpc.DialContext(ctx, etcdTarget, middleware.RoundRobinDialOpts...)
	if err != nil {
		log.Errorf("dial %s failed , etcd target is %s , err:%v", serverName, etcdTarget, err)
		return err
	}

	if f == nil {
		return lberr.NewInvalidArg("init client func is nil")
	}
	f(conn)

	signal.RegV2(func(signal os.Signal) error {
		log.Warnf("exit: close %s client connect, signal [%v]", serverName, signal)
		if err = conn.Close(); err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	})
	return nil
}

// V1 grpc 自带的服务发现
func V1(serverName string, f InitGrpcClientFunc) error {
	// 创建 etcd 客户端
	config := etcdcfg.GetConfig()
	etcdClient, err := eclient.New(eclient.Config{
		Endpoints:   config.GetEndpointList(),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 创建 etcd 实现的 grpc 服务注册发现模块 resolver
	etcdResolverBuilder, err := eresolver.NewBuilder(etcdClient)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 拼接服务名称，需要固定义 etcd:/// 作为前缀
	etcdTarget := fmt.Sprintf("etcd:///%s", serverName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 检查 etcd 是否链接成功
	if len(config.GetEndpointList()) == 0 {
		return lberr.NewInvalidArg("not etcd configured end point list")
	}
	for _, url := range config.GetEndpointList() {
		_, err = etcdClient.Status(ctx, url)
		if err != nil {
			log.Errorf("err:%v , url is %s", err, url)
			return err
		}
	}

	// 创建 grpc 连接代理
	conn, err := grpc.DialContext(
		ctx,
		// 服务名称
		etcdTarget,
		// 注入 etcd bresolver
		grpc.WithResolvers(etcdResolverBuilder),
		// 声明使用的负载均衡策略为 round robin
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, roundrobin.Name)),
		//grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Errorf("dial %s failed , etcd target is %s , err:%v", serverName, etcdTarget, err)
		return err
	}

	if f == nil {
		return lberr.NewInvalidArg("init client func is nil")
	}
	f(conn)

	signal.RegV2(func(signal os.Signal) error {
		log.Warnf("exit: close %s client connect, signal [%v]", serverName, signal)
		if err = conn.Close(); err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	})
	return nil
}
