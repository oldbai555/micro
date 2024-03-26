package reg

import (
	"context"
	"fmt"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/dispatch"
	"github.com/oldbai555/lbtool/pkg/etcdcfg"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/lbtool/pkg/signal"
	dispatch2 "github.com/oldbai555/micro/brpc/dispatchimpl"
	eclient "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"os"
	"time"
)

// EndPointToEtcd V1 grpc 自带的服务注册
func EndPointToEtcd(ctx context.Context, addr, svrName string, regOkChan chan struct{}) error {
	log.Infof("reg svr is %s, address: %s", svrName, addr)
	// 创建 etcd 客户端
	config := etcdcfg.GetConfig()
	etcdClient, _ := eclient.New(eclient.Config{
		Endpoints:   config.GetEndpointList(),
		DialTimeout: 5 * time.Second,
	})

	// 检查 etcd 是否链接成功
	if len(config.GetEndpointList()) == 0 {
		return lberr.NewInvalidArg("not etcd configured end point list")
	}
	newCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, url := range config.GetEndpointList() {
		_, err := etcdClient.Status(newCtx, url)
		if err != nil {
			log.Errorf("err:%v , url is %s", err, url)
			return err
		}
	}

	etcdManager, _ := endpoints.NewManager(etcdClient, svrName)

	// 创建一个租约，每隔 10s 需要向 etcd 汇报一次心跳，证明当前节点仍然存活
	var ttl int64 = 10
	lease, _ := etcdClient.Grant(ctx, ttl)

	// 添加注册节点到 etcd 中，并且携带上租约 id
	err := etcdManager.AddEndpoint(ctx, fmt.Sprintf("%s/%s", svrName, addr), endpoints.Endpoint{Addr: addr}, eclient.WithLease(lease.ID))
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if regOkChan != nil {
		regOkChan <- struct{}{}
	}
	log.Debugf("registered endpoint ok, key is %s", fmt.Sprintf("%s/%s", svrName, addr))

	// 每隔 5 s进行一次延续租约的动作
	for {
		select {
		case <-time.After(5 * time.Second):
			// 续约操作
			resp, err := etcdClient.KeepAliveOnce(ctx, lease.ID)
			if err != nil {
				log.Errorf("err:%v", err)
				continue
			}
			log.Debugf("keep alive resp: %+v", resp)
		case <-signal.GetSignalChan():
			log.Infof("stop EndPointToEtcd")
			return nil
		}
	}
}

// V2 自定义实现服务注册
func V2(ctx context.Context, ip, svrName string, port int, extra string) error {
	iDispatch, err := dispatch2.New()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	node := dispatch.NewNode(ip, port)
	node.Extra = extra
	signal.RegV2(func(signal os.Signal) error {
		err := iDispatch.UnRegister(ctx, svrName, node, true)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	})
	err = iDispatch.Register(ctx, svrName, node)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}
