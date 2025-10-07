package clickhousecache

import (
	"encoding"
	"reflect"
	"time"
)

func Struct2Map(model interface{}) map[string]interface{} {
	ret := make(map[string]interface{})
	modelReflect := reflect.ValueOf(model)
	if modelReflect.Kind() == reflect.Ptr {
		modelReflect = modelReflect.Elem()
	}
	modelRefType := modelReflect.Type()
	fieldsCount := modelReflect.NumField()

	for i := 0; i < fieldsCount; i++ {
		field := modelReflect.Field(i)
		key := modelRefType.Field(i).Tag.Get("db")
		if key == "-" {
			continue
		}
		if key == "" {
			key = modelRefType.Field(i).Name
		}
		var fieldData interface{}

		switch field.Kind() {
		case reflect.Struct:
			if t, ok := field.Interface().(time.Time); ok {
				fieldData = t.Format("2006-01-02")
			} else if t, ok := field.Interface().(encoding.TextMarshaler); ok {
				b, err := t.MarshalText()
				if err == nil {
					fieldData = string(b)
				} else {
					fieldData = ""
				}
			} else {
				// fallback: nested struct
				fieldData = Struct2Map(field.Interface())
			}
		case reflect.Ptr:
			if field.IsNil() {
				fieldData = nil
			} else if t, ok := field.Interface().(encoding.TextMarshaler); ok {
				b, err := t.MarshalText()
				if err == nil {
					fieldData = string(b)
				} else {
					fieldData = ""
				}
			} else {
				fieldData = Struct2Map(field.Interface())
			}
		default:
			fieldData = field.Interface()
		}
		ret[key] = fieldData
	}
	return ret
}
