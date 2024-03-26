package bconst

import "strings"

var (
	LogWithHint = strings.ToUpper("hint")

	GinHeaderTraceId  = strings.ToUpper("X-LB-TRACE-ID")
	GinHeaderDeviceId = strings.ToUpper("X-LB-DEVICE-ID")
	GinHeaderSid      = strings.ToUpper("X-LB-SID")
	GinHeaderAuthType = strings.ToUpper("X-LB-AUTH-TYPE")
)

var (
	GrpcHeaderTraceId  = strings.ToUpper("X-GRPC-TRACE-ID")
	GrpcHeaderDeviceId = strings.ToUpper("X-GRPC-DEVICE-ID")
	GrpcHeaderSid      = strings.ToUpper("X-GRPC-SID")
	GrpcHeaderAuthType = strings.ToUpper("X-GRPC-AUTH-TYPE")
)

var (
	ProtocolType = strings.ToUpper("X-LB-PROTO-TYPE")
)

const (
	PROTO_TYPE_PROTO3   = "proto"
	PROTO_TYPE_API_JSON = "apijson"
)

const (
	KSystemError = -1
	// KErrRequestBodyReadFail 服务端读取请求数据异常
	KErrRequestBodyReadFail = -2002
	// KErrResponseMarshalFail 服务返回数据序列化失败
	KErrResponseMarshalFail = -2003
	// KProcessPanic 业务处理异常
	KProcessPanic       = -2004
	KExceedMaxCallDepth = -2005
)

const (
	HeaderAccessControlAllowOrigin      = "Access-Control-Allow-Origin"
	HeaderAccessControlAllowHeaders     = "Access-Control-Allow-Headers"
	HeaderAccessControlAllowMethods     = "Access-Control-Allow-Methods"
	HeaderAccessControlExposeHeaders    = "Access-Control-Expose-Headers"
	HeaderAccessControlAllowCredentials = "Access-Control-Allow-Credentials"
)

const (
	HttpHeaderContentType       = "Content-Type"
	HttpHeaderContentTypeByJson = "application/json"
	DefaultRspMsg               = "ok"
)
