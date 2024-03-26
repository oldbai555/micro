package middleware

import (
	"context"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/routine"
	"google.golang.org/grpc"
)

func Recover() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		defer routine.CatchPanic(func(err interface{}) {
			if err != nil {
				log.Errorf("err:%v", err)
				return
			}
		})
		return handler(ctx, req)
	}
}

func AutoValidate() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if validator, ok := req.(Validator); ok {
			err := validator.Validate()
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}
