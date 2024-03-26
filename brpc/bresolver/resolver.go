package bresolver

import (
	"context"
	"fmt"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/dispatch"
	"google.golang.org/grpc/resolver"
)

var _ resolver.Resolver = (*Resolver)(nil)

type Resolver struct {
	srvName string
	cc      resolver.ClientConn
	*Builder
}

func NewResolver(srvName string, cc resolver.ClientConn, builder *Builder) *Resolver {
	return &Resolver{
		srvName: srvName,
		cc:      cc,
		Builder: builder,
	}
}

func (r *Resolver) ResolveNow(_ resolver.ResolveNowOptions) {
	srv, err := r.Builder.discover.Discover(context.Background(), r.srvName)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	r.UpdateSrvCfg(srv)
}

func (r *Resolver) Close() {
	r.Builder.OnResolverClosed(r)
}

func (r *Resolver) UpdateSrvCfg(srv *dispatch.Service) {
	if srv.SrvName != r.srvName {
		return
	}

	state := resolver.State{}
	for _, node := range srv.Nodes {
		state.Addresses = append(state.Addresses, resolver.Address{
			Addr: fmt.Sprintf("%s:%d", node.Host, node.Port),
		})
	}

	err := r.cc.UpdateState(state)
	if err != nil {
		log.Errorf("err:%v", err)
	}
}
