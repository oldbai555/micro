package gate

import (
	"context"
	"fmt"
	"github.com/didip/tollbooth"
	"github.com/didip/tollbooth_gin"
	"github.com/gin-gonic/gin"
	"github.com/golang/protobuf/proto"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/lbtool/pkg/signal"
	"github.com/oldbai555/lbtool/utils"
	"github.com/oldbai555/micro/bcmd"
	"github.com/oldbai555/micro/bconst"
	"github.com/oldbai555/micro/bgin"
	"github.com/oldbai555/micro/blimiter"
	"net/http"
	"os"
	"reflect"
)

type CheckAuthFunc func(ctx context.Context, sid string) (interface{}, error)

type Svr struct {
	name          string
	port          uint32
	cmdList       []*bcmd.Cmd
	checkAuthFunc CheckAuthFunc

	httpSrv *http.Server
}

func NewSvr(name string, port uint32, cmdList []*bcmd.Cmd, checkAuthFunc CheckAuthFunc) *Svr {
	return &Svr{name: name, port: port, cmdList: cmdList, checkAuthFunc: checkAuthFunc}
}

func (s *Svr) StartSrv(ctx context.Context) error {
	gin.DefaultWriter = log.GetWriter()
	gin.DebugPrintRouteFunc = func(httpMethod, absolutePath, handlerName string, nuHandlers int) {
		log.Infof("%-6s %-25s --> %s (%d handlers)", httpMethod, absolutePath, handlerName, nuHandlers)
	}
	router := gin.Default()

	// Create a limiter struct.
	limiter := tollbooth.NewLimiter(blimiter.Max, blimiter.DefaultExpiredAbleOptions())

	router.Use(
		gin.Recovery(),
		gin.LoggerWithFormatter(bgin.NewLogFormatter(s.name)),
		bgin.Cors(),
		bgin.RegisterUuidTrace(),
		tollbooth_gin.LimitHandler(limiter),
	)

	checkCmdList(s.cmdList)

	for _, cmd := range s.cmdList {
		registerCmd(router, cmd, s.checkAuthFunc)
	}

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: router,
	}

	s.httpSrv = srv

	signal.RegV2(func(signal os.Signal) error {
		log.Warnf("exit: close %s gateway server connect , signal[%v]", s.name, signal)
		err := srv.Shutdown(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	})

	log.Infof("====== start grpc %s gate , port is %d ======", s.name, s.port)

	// 启动服务
	err := srv.ListenAndServe()
	if err != nil {
		log.Warnf("err is %v", err)
		return err
	}
	return nil
}

func (s *Svr) Stop() {
	if s.httpSrv == nil {
		return
	}
	err := s.httpSrv.Shutdown(context.Background())
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
}

func checkCmdList(cmdList []*bcmd.Cmd) {
	for _, cmd := range cmdList {
		h := cmd.GRpcFunc
		v := reflect.ValueOf(h)
		t := v.Type()
		if !t.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
			panic("XX(context.Context, proto.Message)(proto.Message, error): first in arg must be context.Context")
		}
		if !t.In(1).Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
			panic("XX(context.Context, proto.Message)(proto.Message, error): first out arg must be proto.Message")
		}
		if !t.Out(0).Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
			panic("XX(context.Context, proto.Message)(proto.Message, error): first out arg must be proto.Message")
		}
		if t.Out(1).String() != "error" {
			panic("XX(context.Context, proto.Message)(proto.Message, error): second out arg must be error")
		}
	}
}

func registerCmd(router *gin.Engine, cmd *bcmd.Cmd, checkF CheckAuthFunc) {
	router.POST(cmd.Path, func(c *gin.Context) {
		handler := bgin.NewHandler(c)

		nCtx := NewGinUCtx(c)

		// 组装 nCtx
		{
			val := c.GetHeader(bconst.ProtocolType)
			if val != "" {
				nCtx.SetProtocolType(val)
			} else {
				nCtx.SetProtocolType(bconst.PROTO_TYPE_PROTO3) // 默认pb
			}

			val = c.GetHeader(bconst.GinHeaderTraceId)
			if val != "" {
				val = fmt.Sprintf("%s.%s", val, utils.GenRandomStr())
			} else {
				val = utils.GenRandomStr()
			}
			nCtx.SetTraceId(val)
			log.SetLogHint(val)

			val = c.GetHeader(bconst.GinHeaderDeviceId)
			if val != "" {
				nCtx.SetDeviceId(val)
			}

			val = c.GetHeader(bconst.GinHeaderSid)
			if val != "" {
				nCtx.SetSid(val)
			}

			val = c.GetHeader(bconst.GinHeaderAuthType)
			if val != "" {
				nCtx.SetAuthType(val)
			} else {
				nCtx.SetAuthType(cmd.GetAuthType())
			}

			// 需要校验
			if cmd.IsUserAuthType() {
				if checkF == nil {
					panic("check auth func is nil")
				}
				info, err := checkF(nCtx, nCtx.Sid())
				if err != nil {
					log.Errorf("err:%v", err)
					handler.Error(err)
					return
				}
				nCtx.SetExtInfo(info)
			}
		}

		// call
		h := cmd.GRpcFunc
		v := reflect.ValueOf(h)
		t := v.Type()

		// 拼装 request
		reqT := t.In(1).Elem()
		reqV := reflect.New(reqT)
		msg := reqV.Interface().(proto.Message)

		// 根据协议来
		err := handler.UnmarshalerByProtocol(c.Request.Body, msg, nCtx.ProtocolType())
		if err != nil {
			log.Errorf("err:%v", err)
			handler.Error(err)
			return
		}
		log.Infof("req is %v", msg.String())

		handlerRet := v.Call([]reflect.Value{reflect.ValueOf(nCtx), reqV})

		// 检查是否有误
		var callRes error
		if !handlerRet[1].IsNil() {
			callRes = handlerRet[1].Interface().(error)
		}

		if callRes != nil {
			log.Errorf("err:%v", callRes)
			handler.Error(callRes)
			return
		}

		// 检查返回值
		if handlerRet[0].IsValid() && !handlerRet[0].IsNil() {
			rspBody, ok := handlerRet[0].Interface().(proto.Message)
			if !ok {
				log.Errorf("proto.Marshal err %v", err)
				handler.Error(lberr.NewErr(500, "not proto.Message"))
				return
			}

			handler.RespByProtocol(rspBody, nCtx.ProtocolType())
			return
		}

		// 走到这里说明走不动了
		handler.Error(lberr.NewInvalidArg("un ok"))
	})
}
