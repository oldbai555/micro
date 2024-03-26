package uctx

import (
	"context"
	"google.golang.org/grpc/status"
)

type IUCtx interface {
	AuthType() string
	SetAuthType(authType string)
	Sid() string
	SetSid(sid string)
	DeviceId() string
	SetDeviceId(deviceId string)
	TraceId() string
	SetTraceId(traceId string)
	ExtInfo() interface{}
	SetExtInfo(interface{})
	ProtocolType() string
	SetProtocolType(authType string)
}

func ToUCtx(ctx context.Context) (IUCtx, error) {
	iuCtx, ok := ctx.(IUCtx)
	if !ok {
		return nil, convertErr
	}
	return iuCtx, nil
}

var convertErr = status.Error(99999, "convert u ctx failed")
