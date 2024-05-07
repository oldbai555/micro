package bgin

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/utils"
	"github.com/oldbai555/micro/bconst"
	"net/http"
	"time"
)

// Cors 跨域配制
func Cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		if v := c.GetHeader(bconst.HeaderAccessControlAllowOrigin); v == "" {
			c.Header(bconst.HeaderAccessControlAllowOrigin, "*")
		}

		if v := c.GetHeader(bconst.HeaderAccessControlAllowHeaders); v == "" {
			c.Header("Access-Control-Allow-Headers", "Origin,Content-Type,AccessToken,X-CSRF-Token,Authorization,Token,X-Requested-With")
		}

		if v := c.GetHeader(bconst.HeaderAccessControlAllowMethods); v == "" {
			c.Header("Access-Control-Allow-Methods", "POST, GET") // 只放行 POST GET
		}

		if v := c.GetHeader(bconst.HeaderAccessControlExposeHeaders); v == "" {
			c.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Content-Type")
		}

		if v := c.GetHeader(bconst.HeaderAccessControlAllowCredentials); v == "" {
			c.Header("Access-Control-Allow-Credentials", "true")
		}

		method := c.Request.Method
		if method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
		}
	}
}

// RegisterUuidTrace 注册一个链路Id进入日志中
func RegisterUuidTrace() gin.HandlerFunc {
	return func(c *gin.Context) {
		var traceId string
		hint := c.Value(bconst.LogWithHint)
		if hint == nil || fmt.Sprintf("%v", hint) == "" {
			hint = c.GetHeader(bconst.GinHeaderTraceId)
		}

		if hint == "" {
			traceId = utils.GenUUID()
		} else {
			traceId = fmt.Sprintf("%v.%s", hint, utils.GenUUID())
		}
		log.SetLogHint(traceId)

		c.Set(bconst.LogWithHint, traceId)
		c.Request.Header.Add(bconst.GinHeaderTraceId, traceId)

		log.Infof("hint: %s , RemoteIP: %s , ClientIP: %s", traceId, c.RemoteIP(), c.ClientIP())

		c.Next()
	}
}

func NotFoundGrpcRouter() gin.HandlerFunc {
	return func(context *gin.Context) {
		handler := NewHandler(context)
		// 404
		handler.RespByJson(http.StatusNotFound, http.StatusNotFound, "", fmt.Sprintf("%s not found", context.Request.RequestURI))
	}
}

func NewLogFormatter(svr string) func(param gin.LogFormatterParams) string {
	return func(param gin.LogFormatterParams) string {
		var statusColor, methodColor, resetColor string
		if param.IsOutputColor() {
			statusColor = param.StatusCodeColor()
			methodColor = param.MethodColor()
			resetColor = param.ResetColor()
		}

		if param.Latency > time.Minute {
			param.Latency = param.Latency.Truncate(time.Second)
		}
		hint := param.Keys[bconst.LogWithHint]
		v := fmt.Sprintf("[%s] [GIN] <%s> %v |%s %3d %s| %13v | %15s |%s %-7s %s %#v\n%s",
			svr,
			hint,
			param.TimeStamp.Format("2006/01/02 - 15:04:05"),
			statusColor, param.StatusCode, resetColor,
			param.Latency,
			param.ClientIP,
			methodColor, param.Method, resetColor,
			param.Path,
			param.ErrorMessage,
		)

		return v
	}
}
