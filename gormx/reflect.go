package gormx

import (
	"fmt"
	"reflect"
	"strings"
)

func ReflectGet(obj interface{}, path string, failHint *string) (res interface{}, success bool) {
	setFailHint := func(hint string) {
		if failHint != nil {
			*failHint = hint
		}
	}
	for obj != nil && path != "" {
		v := reflect.ValueOf(obj)
		for v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			setFailHint("not struct type")
			break
		}
		var fieldName string
		pos := strings.IndexByte(path, '.')
		if pos < 0 {
			fieldName = path
			path = ""
		} else {
			fieldName = path[:pos]
			path = path[pos+1:]
		}
		f := v.FieldByName(fieldName)
		if !f.IsValid() {
			setFailHint(fmt.Sprintf("%s not found", fieldName))
			break
		}
		if path == "" {
			res = f.Interface()
			success = true
			break
		}
		obj = f.Interface()
	}
	return
}

func ReflectGetInt(obj interface{}, path string, failHint *string) (res int, success bool) {
	setFailHint := func(hint string) {
		if failHint != nil {
			*failHint = hint
		}
	}
	var i interface{}
	i, success = ReflectGet(obj, path, failHint)
	if !success {
		return
	}
	switch v := i.(type) {
	case int:
		res = v
		success = true
	case int32:
		res = int(v)
		success = true
	default:
		setFailHint(fmt.Sprintf("type not match: %s", reflect.TypeOf(i).String()))
	}
	return
}

func ReflectGetStr(obj interface{}, path string, failHint *string) (res string, success bool) {
	setFailHint := func(hint string) {
		if failHint != nil {
			*failHint = hint
		}
	}
	var i interface{}
	i, success = ReflectGet(obj, path, failHint)
	if !success {
		return
	}
	switch v := i.(type) {
	case string:
		res = v
		success = true
	case []byte:
		res = string(v)
		success = true
	default:
		setFailHint(fmt.Sprintf("type not match: %s", reflect.TypeOf(i).String()))
	}
	return
}

func Interface2Int(i interface{}) int {
	switch x := i.(type) {
	case bool:
		if x {
			return 1
		}
		return 0
	case int:
		return x
	case int8:
		return int(x)
	case int16:
		return int(x)
	case int32:
		return int(x)
	case int64:
		return int(x)
	case uint:
		return int(x)
	case uint8:
		return int(x)
	case uint16:
		return int(x)
	case uint32:
		return int(x)
	case uint64:
		return int(x)
	case float32:
		return int(x)
	case float64:
		return int(x)
	}
	return 0
}

func Interface2String(i interface{}) string {
	switch x := i.(type) {
	case string:
		return x
	}
	panic("not string")
}

func EnsureIsSliceOrArray(obj interface{}) (res reflect.Value) {
	vo := reflect.ValueOf(obj)
	for vo.Kind() == reflect.Ptr || vo.Kind() == reflect.Interface {
		vo = vo.Elem()
	}
	k := vo.Kind()
	if k != reflect.Slice && k != reflect.Array {
		panic(fmt.Sprintf("obj required slice or array type, but got %v", vo.Type()))
	}
	res = vo
	return
}

func EnsureIsMapType(m reflect.Value, keyType, valType reflect.Type) {
	if m.Kind() != reflect.Map {
		panic(fmt.Sprintf("required map type, but got %v", m.Type()))
	}
	t := m.Type()
	if t.Key() != keyType {
		panic(fmt.Sprintf("map key type not equal, %v != %v", t.Key(), keyType))
	}
	if t.Elem() != valType {
		panic(fmt.Sprintf("map val type not equal, %v != %v", t.Elem(), valType))
	}
}

func ClearSlice(ptr interface{}) {
	if ptr == nil {
		return
	}
	vo := reflect.ValueOf(ptr)
	if vo.Kind() != reflect.Ptr {
		panic("required ptr to slice type")
	}
	for vo.Kind() == reflect.Ptr {
		vo = vo.Elem()
	}
	if vo.Kind() != reflect.Slice {
		panic("required ptr to slice type")
	}
	vo.Set(reflect.MakeSlice(vo.Type(), 0, 0))
}

func GetSliceLen(i interface{}) int {
	vo := reflect.ValueOf(i)
	for vo.Kind() == reflect.Ptr {
		vo = vo.Elem()
	}
	if vo.Kind() != reflect.Slice && vo.Kind() != reflect.Array {
		panic("required slice or array type")
	}
	return vo.Len()
}
