package gormx

import (
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/oldbai555/lbtool/log"
	"path/filepath"
	"strings"

	"github.com/bytedance/sonic"
)

const (
	DbCorpId = "corp_id"
)

const (
	ErrModelUniqueIndexConflict = 10001
)

func rowsJsonToPb(rowsJson string, obj interface{}) error {
	if rowsJson != "" {
		j := rowsJson
		if len(j) >= 2 && j[0] == '[' && j[len(j)-1] == ']' {
			j = j[1 : len(j)-1]
		}
		err := sonic.Unmarshal([]byte(j), obj)
		if err != nil {
			log.Errorf("err:%s", err)
			return err
		}
	}
	return nil
}

func toJonSkipZeroValueField(obj interface{}) (string, error) {
	if msg, ok := obj.(proto.Message); ok {
		var m = jsonpb.Marshaler{
			OrigName: true,
		}
		j, err := m.MarshalToString(msg)
		if err != nil {
			log.Errorf("proto MarshalToString err:v", err)
			return "", err
		}
		return j, nil
	}
	buf, err := sonic.Marshal(obj)
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}
	return string(buf), nil
}

func map2Interface(m map[string]interface{}, i interface{}) error {
	if nm, ok := i.(*map[string]interface{}); ok {
		p := *nm
		for k, v := range m {
			p[k] = v
		}
	} else {
		b, err := sonic.Marshal(m)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		err = sonic.Unmarshal(b, i)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

func getModelType(m interface{}) string {
	if v, ok := m.(proto.Message); ok {
		return proto.MessageName(v)
	}
	// 如果不是proto，就用table name代替
	return TryGetTableName(m)
}

func getPackageName(callerFile, callerName string) (string, string) {
	slashIndex := strings.LastIndex(callerName, "/")
	if slashIndex > 0 {
		idx := strings.Index(callerName[slashIndex:], ".") + slashIndex
		return filepath.Join(callerName[:idx], callerFile), callerName[idx+1:]
	}

	return filepath.Join(callerName, callerFile), ""
}

type tabler interface {
	TableName() string
}

func TryGetTableName(i interface{}) string {
	if p, ok := i.(tabler); ok {
		return p.TableName()
	}
	return ""
}
