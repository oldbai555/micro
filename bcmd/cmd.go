package bcmd

import (
	"net/http"
	"strings"
)

const (
	ApiMethod = "ApiMethod"
	AuthType  = "AuthType"
)

const (
	AuthTypeUser   = "user"
	AuthTypePublic = "public"
	AuthTypeSystem = "system"
)

type Cmd struct {
	Server    string // 所在服务
	Path      string // api 请求路径
	FuncName  string // 方法名
	OptionMap map[string]string
	GRpcFunc  interface{}
}

func (c *Cmd) GetApiMethod() string {
	if c.OptionMap == nil {
		return http.MethodPost
	}
	method, ok := c.OptionMap[ApiMethod]
	if !ok {
		method = http.MethodPost
	}
	return strings.ToUpper(method)
}

func (c *Cmd) GetAuthType() string {
	if c.OptionMap == nil {
		return AuthTypeUser
	}
	authType, ok := c.OptionMap[AuthType]
	if !ok {
		authType = AuthTypeUser
	}
	return authType
}

func (c *Cmd) IsUserAuthType() bool {
	return strings.ToUpper(c.GetAuthType()) == strings.ToUpper(AuthTypeUser)
}

func (c *Cmd) IsPublicAuthType() bool {
	return strings.ToUpper(c.GetAuthType()) == strings.ToUpper(AuthTypePublic)
}

func (c *Cmd) IsSystemAuthType() bool {
	return strings.ToUpper(c.GetAuthType()) == strings.ToUpper(AuthTypeSystem)
}
