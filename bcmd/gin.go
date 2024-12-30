/**
 * @Author: zjj
 * @Date: 2024/12/30
 * @Desc:
**/

package bcmd

import (
	"github.com/gin-gonic/gin"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/jsonpb"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/micro/brpc/middleware"
	"github.com/oldbai555/micro/uctx"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"reflect"
)

func (c *Cmd) WithGenIUCtx(genIUCtxF func(ctx *gin.Context) uctx.IUCtx) *Cmd {
	c.genIUCtxF = genIUCtxF
	return c
}

func (c *Cmd) WithCheckAuthF(checkF func(nCtx uctx.IUCtx) (extInfo interface{}, err error)) *Cmd {
	c.checkAuthF = checkF
	return c
}

func (c *Cmd) WithHandleError(handleF func(ctx *gin.Context, err error)) *Cmd {
	c.errF = handleF
	return c
}

func (c *Cmd) WithHandleResult(handleF func(ctx *gin.Context, result proto.Message)) *Cmd {
	c.resultF = handleF
	return c
}

func (c *Cmd) GinPost(ctx *gin.Context) {
	// func (a *Server)  RpcFunc(ctx context.Context, req *RpcReq) (*RpcRsp, error)

	// call
	h := c.GRpcFunc
	v := reflect.ValueOf(h)
	t := v.Type()

	// 拼装 request
	reqT := t.In(1).Elem()
	reqV := reflect.New(reqT)
	msg := reqV.Interface().(proto.Message)
	buff, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		log.Errorf("read err:%v", err)
		return
	}

	err = jsonpb.Unmarshal(buff, msg)
	if err != nil {
		log.Errorf("err:%v", err)
		c.errF(ctx, err)
		return
	}

	if validator, ok := reqV.Interface().(middleware.Validator); ok {
		err := validator.Validate()
		if err != nil {
			log.Errorf("err:%v", err)
			c.errF(ctx, err)
			return
		}
	}

	if c.genIUCtxF == nil {
		c.errF(ctx, lberr.NewErr(http.StatusInternalServerError, "internal server error"))
		return
	}

	nCtx := c.genIUCtxF(ctx)

	// 需要校验
	if c.IsUserAuthType() {
		if c.checkAuthF == nil {
			c.errF(ctx, lberr.NewErr(http.StatusUnauthorized, "unauthorized"))
			return
		}
		extInfo, err := c.checkAuthF(nCtx)
		if err != nil {
			log.Errorf("err:%v", err)
			c.errF(ctx, lberr.NewErr(http.StatusUnauthorized, "unauthorized"))
			return
		}
		nCtx.SetExtInfo(extInfo)
	}

	log.Infof("req:[%s]", string(buff))

	handlerRet := v.Call([]reflect.Value{reflect.ValueOf(nCtx), reqV})

	// 检查是否有误
	var callRes error
	if !handlerRet[1].IsNil() {
		callRes = handlerRet[1].Interface().(error)
	}

	if callRes != nil {
		log.Errorf("err:%v", callRes)
		c.errF(ctx, callRes)
		return
	}

	// 检查返回值
	if handlerRet[0].IsValid() && !handlerRet[0].IsNil() {
		rspBody, ok := handlerRet[0].Interface().(proto.Message)
		if !ok {
			log.Errorf("proto convert failed")
			c.errF(ctx, lberr.NewErr(http.StatusInternalServerError, "internal server error"))
			return
		}

		c.resultF(ctx, rspBody)
		return
	}

	// 走到这里说明走不动了
	c.errF(ctx, lberr.NewErr(http.StatusInternalServerError, "Internal error"))
}
