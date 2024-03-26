package bresolver

import (
	"context"
	"github.com/emirpasic/gods/lists/doublylinkedlist"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/dispatch"
	dispatch2 "github.com/oldbai555/micro/brpc/dispatchimpl"
	"google.golang.org/grpc/resolver"
	"sync"
)

var _ resolver.Builder = (*Builder)(nil)

const (
	ResolveSchema = "baix"
)

type Builder struct {
	srvNameToResolversMap map[string]*doublylinkedlist.List
	discover              dispatch.IDispatch
	mu                    sync.RWMutex
}

func NewBuilder(ctx context.Context) (resolver.Builder, error) {
	iDispatch, err := dispatch2.New()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	builder := &Builder{
		srvNameToResolversMap: map[string]*doublylinkedlist.List{},
	}

	iDispatch.OnSrvUpdated(func(ctx context.Context, evt dispatch.Evt, srv *dispatch.Service) {
		builder.mu.RLock()
		defer builder.mu.RUnlock()

		resolvers, ok := builder.srvNameToResolversMap[srv.SrvName]
		if !ok {
			return
		}

		resolvers.Each(func(_ int, node interface{}) {
			node.(*Resolver).UpdateSrvCfg(srv)
		})
	})

	_, err = iDispatch.LoadAll(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	builder.discover = iDispatch

	return builder, nil
}

func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	srvName := target.Endpoint

	srv, err := b.discover.Discover(context.Background(), srvName)
	if err != nil {
		return nil, err
	}

	resolve := NewResolver(srvName, cc, b)
	resolve.UpdateSrvCfg(srv)

	b.mu.Lock()
	defer b.mu.Unlock()

	resolvers, ok := b.srvNameToResolversMap[srvName]
	if !ok {
		resolvers = doublylinkedlist.New()
		b.srvNameToResolversMap[srvName] = resolvers
	}
	resolvers.Append(resolve)

	return resolve, nil
}

func (b *Builder) OnResolverClosed(resolve *Resolver) {
	b.mu.Lock()
	defer b.mu.Unlock()

	resolvers := b.srvNameToResolversMap[resolve.srvName]
	resolvers.Remove(resolvers.IndexOf(resolve))
	if resolvers.Size() == 0 {
		delete(b.srvNameToResolversMap, resolve.srvName)
	}

	return
}

func (b *Builder) Scheme() string {
	return ResolveSchema
}
