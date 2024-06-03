package bgin

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/render"
	"github.com/golang/protobuf/proto"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/json"
	"github.com/oldbai555/lbtool/pkg/jsonpb"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/micro/bconst"
	"github.com/pkg/errors"
	"google.golang.org/grpc/status"
	"io"
	"net/http"
)

type Handler struct {
	C *gin.Context
}

func NewHandler(c *gin.Context) *Handler {
	handler := &Handler{
		C: c,
	}
	return handler
}

// BindAndValidateReq 绑定并校验请求参数 - 请求体
// req 必须是指针
func (r *Handler) BindAndValidateReq(req interface{}) error {
	err := r.C.ShouldBindJSON(req)
	if err != nil {
		return err
	}
	return nil
}

func (r *Handler) GetHeader(key string) string {
	return r.C.GetHeader(key)
}

func (r *Handler) GetQuery(key string) (string, bool) {
	return r.C.GetQuery(key)
}

func (r *Handler) RespByJson(httpCode int, errCode int32, data string, errMsg string) {
	r.C.Header(bconst.HttpHeaderContentType, bconst.HttpHeaderContentTypeByJson)
	hint := r.C.Value(bconst.LogWithHint)
	if data == "" {
		data = "{}"
	}
	template := apiRspTemplate(data, errCode, errMsg, fmt.Sprintf("%s", hint))
	log.Infof("jsonRsp:%v", template)

	w := r.C.Writer
	r.C.Status(httpCode)

	if !bodyAllowedForStatus(httpCode) {
		j := render.JSON{Data: data}
		j.WriteContentType(w)
		w.WriteHeaderNow()
		return
	}

	header := w.Header()
	if val := header["Content-Type"]; len(val) == 0 {
		header["Content-Type"] = []string{"application/json; charset=utf-8"}
	}
	header[bconst.ProtocolType] = []string{bconst.PROTO_TYPE_API_JSON}

	_, err := w.Write([]byte(template))
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func (r *Handler) RespByPb(pb proto.Message) {
	marshal, err := proto.Marshal(pb)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	w := r.C.Writer

	header := w.Header()
	header[bconst.ProtocolType] = []string{bconst.PROTO_TYPE_PROTO3}

	_, err = r.C.Writer.Write(marshal)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
}

func (r *Handler) RespByProtocol(pb proto.Message, protocolType string) {
	switch protocolType {
	case bconst.PROTO_TYPE_API_JSON:
		r.Success(pb)
	case bconst.PROTO_TYPE_PROTO3:
		r.RespByPb(pb)
	}
}

func (r *Handler) UnmarshalerByProtocol(reader io.ReadCloser, pb proto.Message, protocolType string) error {
	var err error
	switch protocolType {
	case bconst.PROTO_TYPE_API_JSON:
		err = jsonpb.Unmarshal(reader, pb)
	case bconst.PROTO_TYPE_PROTO3:
		var buf bytes.Buffer
		_, err := buf.ReadFrom(reader)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if err == nil {
			err = proto.Unmarshal(buf.Bytes(), pb)
		}
	}
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// Success 响应数据
func (r *Handler) Success(data proto.Message) {
	var tmp string
	tmp, err := jsonpb.MarshalToString(data)
	if err != nil {
		log.Errorf("MarshalToString err %v", err)
	}
	// 封一层
	if tmp == "" {
		tmp = "{}"
	}
	log.Infof("rsp: %s", tmp)
	r.RespByJson(http.StatusOK, 0, tmp, bconst.DefaultRspMsg)
}

// 响应错误
func (r *Handler) Error(err error) {
	// 获取根错误
	rootErr := errors.Cause(err)

	if e, ok := rootErr.(*lberr.LbErr); ok {
		r.RespByJson(http.StatusOK, e.Code(), "", e.Message())
		return
	}

	if e, ok := status.FromError(err); ok {
		r.RespByJson(http.StatusOK, int32(e.Code()), "", e.Message())
		return
	}

	r.RespByJson(http.StatusOK, http.StatusInternalServerError, "", err.Error())
}

func (r *Handler) HttpJson(val interface{}) {
	b, _ := json.Marshal(val)
	r.RespByJson(http.StatusOK, 0, string(b), bconst.DefaultRspMsg)
}

func apiRspTemplate(data string, errCode int32, errMsg string, hint string) string {
	var rsp = map[string]interface{}{
		"data":    data,
		"errcode": errCode,
		"errmsg":  errMsg,
		"hint":    hint,
	}
	marshal, err := json.Marshal(rsp)
	if err != nil {
		return ""
	}
	return string(marshal)
}

func bodyAllowedForStatus(status int) bool {
	switch {
	case status >= 100 && status <= 199:
		return false
	case status == http.StatusNoContent:
		return false
	case status == http.StatusNotModified:
		return false
	}
	return true
}
