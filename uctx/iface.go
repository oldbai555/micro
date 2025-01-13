package uctx

import (
	"context"
	"github.com/oldbai555/lbtool/pkg/lberr"
)

type IUCtx interface {
	context.Context
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

var convertErr = lberr.NewCustomErr("convert ctx failed")
