package clickhouseconn

import (
	"reflect"
	"time"
)

// Struct2Map converts struct to map.
// result Key is:
// db tag - if tag "db" is provided or
// field name otherwise
func Struct2Map(model interface{}) map[string]interface{} {
	ret := make(map[string]interface{})

	modelReflect := reflect.ValueOf(model)

	if modelReflect.Kind() == reflect.Ptr {
		modelReflect = modelReflect.Elem()
	}

	modelRefType := modelReflect.Type()
	fieldsCount := modelReflect.NumField()

	var fieldData interface{}

	for i := 0; i < fieldsCount; i++ {
		field := modelReflect.Field(i)

		key := modelRefType.Field(i).Tag.Get("db")
		if key == "-" {
			// ignore
			continue
		}

		switch field.Kind() {
		case reflect.Struct:
			if t, ok := field.Interface().(time.Time); ok {
				fieldData = t.Format("2006-01-02") // Format as string for ClickHouse
			} else {
				fieldData = Struct2Map(field.Interface())
			}
		case reflect.Ptr:
			fieldData = Struct2Map(field.Interface())
		default:
			fieldData = field.Interface()
		}

		if key == "" {
			key = modelRefType.Field(i).Name
		}
		ret[key] = fieldData
	}

	return ret
}
