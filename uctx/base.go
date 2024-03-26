package uctx

type BaseUCtx struct {
	sid          string
	deviceId     string
	traceId      string
	authType     string
	protocolType string
	extInfo      interface{}
}

func NewBaseUCtx() *BaseUCtx {
	return &BaseUCtx{}
}

func (U *BaseUCtx) ProtocolType() string {
	return U.protocolType
}

func (U *BaseUCtx) SetProtocolType(protoType string) {
	U.protocolType = protoType
}

func (U *BaseUCtx) ExtInfo() interface{} {
	return U.extInfo
}

func (U *BaseUCtx) SetExtInfo(i interface{}) {
	U.extInfo = i
}

func (U *BaseUCtx) AuthType() string {
	return U.authType
}

func (U *BaseUCtx) SetAuthType(authType string) {
	U.authType = authType
}

func (U *BaseUCtx) Sid() string {
	return U.sid
}

func (U *BaseUCtx) SetSid(sid string) {
	U.sid = sid
}

func (U *BaseUCtx) DeviceId() string {
	return U.deviceId
}

func (U *BaseUCtx) SetDeviceId(deviceId string) {
	U.deviceId = deviceId
}

func (U *BaseUCtx) TraceId() string {
	return U.traceId
}

func (U *BaseUCtx) SetTraceId(traceId string) {
	U.traceId = traceId
}
