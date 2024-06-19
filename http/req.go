package http

import (
	"context"
	"fmt"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/dispatch"
	"github.com/oldbai555/lbtool/pkg/json"
	"github.com/oldbai555/lbtool/pkg/jsonpb"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/lbtool/pkg/restysdk"
	"github.com/oldbai555/micro/bconst"
	"github.com/oldbai555/micro/brpc/dispatchimpl"
	"google.golang.org/protobuf/proto"
	"net/url"
)

type Resp struct {
	Data    string `json:"data"`
	ErrCode int32  `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
	Hint    string `json:"hint"`
}

func DoRequest(ctx context.Context, srv, path, method string, protocolType string, req, out proto.Message) error {
	d, err := dispatchimpl.New()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	node, err := dispatch.Route(ctx, d, srv)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var body []byte
	switch protocolType {
	case bconst.PROTO_TYPE_API_JSON:
		var val string
		val, err = jsonpb.MarshalToString(req)
		body = []byte(val)
	case bconst.PROTO_TYPE_PROTO3:
		body, err = proto.Marshal(req)
	default:
		err = lberr.NewInvalidArg("req not found protocol type , val is %s", protocolType)
	}
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var headers = make(map[string]string)
	headers[bconst.ProtocolType] = bconst.PROTO_TYPE_PROTO3

	var target = fmt.Sprintf("%s://%s:%s", "http", node.Host, node.Extra)
	result, err := url.JoinPath(target, path)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	//log.Infof("do http request request: %v", req.ProtoReflect())
	resp, err := restysdk.NewRequest().SetHeaders(headers).SetBody(body).Execute(method, result)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	val := resp.Header().Get(bconst.ProtocolType)
	switch val {
	case bconst.PROTO_TYPE_API_JSON:
		log.Infof("do http resp is %s", string(resp.Body()))
		var respBody Resp
		err := json.Unmarshal(resp.Body(), &respBody)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if respBody.ErrCode > 0 {
			return lberr.NewErr(respBody.ErrCode, respBody.ErrMsg)
		}
		err = jsonpb.Unmarshal([]byte(respBody.Data), out)
	case bconst.PROTO_TYPE_PROTO3:
		err = proto.Unmarshal(resp.Body(), out)
		//log.Infof("do http resp is %s", out.String())
	default:
		err = lberr.NewInvalidArg("resp not found protocol type , val is %s", val)
	}
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}
