package fluxline

// DCSO fluxline
// Copyright (c) 2017, 2018, DCSO GmbH

import (
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"
)

// Encoder represents a component that encapsulates a target environment for
// measurement submissions, as given by hostname and receiving writer.
type Encoder struct {
	host string
	io.Writer
}

func escapeSpecialChars(in string) string {
	str := strings.Replace(in, ",", `\,`, -1)
	str = strings.Replace(str, "=", `\=`, -1)
	str = strings.Replace(str, " ", `\ `, -1)
	return str
}

func toInfluxRepr(tag string, val interface{}, nostatictypes bool) (string, error) {
	switch v := val.(type) {
	case string:
		if len(v) > 64000 {
			return "", fmt.Errorf("%s: string too long (%d characters, max. 64K)", tag, len(v))
		}
		return fmt.Sprintf("%q", v), nil
	case int32, int64, int16, int8, int, uint32, uint64, uint16, uint8, uint:
		if nostatictypes {
			return fmt.Sprintf("%d", v), nil
		}
		return fmt.Sprintf("%di", v), nil
	case float64, float32:
		return fmt.Sprintf("%g", v), nil
	case bool:
		return fmt.Sprintf("%t", v), nil
	case time.Time:
		return fmt.Sprintf("%d", uint64(v.UnixNano())), nil
	default:
		return "", fmt.Errorf("%s: unsupported type for Influx Line Protocol", tag)
	}
}

func recordFields(val interface{}, fieldSet map[string]string, nostatictypes bool) (map[string]string, error) {
	t := reflect.TypeOf(val)
	v := reflect.ValueOf(val)

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("val needs to be a struct")
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("influx")
		if tag == "" {
			continue
		}
		repr, err := toInfluxRepr(tag, v.Field(i).Interface(), nostatictypes)
		if err != nil {
			return nil, err
		}
		fieldSet[tag] = repr
	}
	return fieldSet, nil
}

func (a *Encoder) formatLineProtocol(prefix string, tags map[string]string, fieldSet map[string]string, ts time.Time) string {
	out := ""
	tagstr := ""

	// sort by key to obtain stable output order
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// serialize tags
	for _, k := range keys {
		tagstr += ","
		tagstr += fmt.Sprintf("%s=%s", escapeSpecialChars(k), escapeSpecialChars(tags[k]))
	}

	// sort by key to obtain stable output order
	keys = make([]string, 0, len(fieldSet))
	for key := range fieldSet {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// serialize fields
	first := true
	for _, k := range keys {
		if !first {
			out += ","
		} else {
			first = false
		}
		out += fmt.Sprintf("%s=%s", escapeSpecialChars(k), fieldSet[k])
	}
	if out == "" {
		return ""
	}

	t := ts
	if ts.IsZero() {
		t = time.Now()
	}

	// construct line protocol string
	return fmt.Sprintf("%s,host=%s%s %s %d\n", prefix, a.host,
		tagstr, out, uint64(t.UnixNano()))
}

// Encode writes the line protocol representation for a given measurement
// name, data struct and tag map to the io.Writer specified on encoder creation.
func (a *Encoder) encodeGeneric(prefix string, val interface{}, tags map[string]string, nostatictypes bool, opts ...Option) error {
	var ops options
	for _, o := range opts {
		o(&ops)
	}

	fieldSet := make(map[string]string)
	fieldSet, err := recordFields(val, fieldSet, nostatictypes)
	if err != nil {
		return err
	}
	_, err = a.Write([]byte(a.formatLineProtocol(prefix, tags, fieldSet, ops.time)))
	return err
}

// Encode writes the line protocol representation for a given measurement
// name, data struct and tag map to the io.Writer specified on encoder creation.
func (a *Encoder) Encode(prefix string, val interface{}, tags map[string]string, opts ...Option) error {
	return a.encodeGeneric(prefix, val, tags, false, opts...)
}

// EncodeWithoutTypes writes the line protocol representation for a given measurement
// name, data struct and tag map to the io.Writer specified on encoder creation.
// In contrast to Encode(), this method never appends type suffixes to values.
func (a *Encoder) EncodeWithoutTypes(prefix string, val interface{}, tags map[string]string, opts ...Option) error {
	return a.encodeGeneric(prefix, val, tags, true, opts...)
}

// EncodeMap writes the line protocol representation for a given measurement
// name, field value map and tag map to the io.Writer specified on encoder
// creation.
func (a *Encoder) EncodeMap(prefix string, val map[string]interface{}, tags map[string]string, opts ...Option) error {
	var ops options
	for _, o := range opts {
		o(&ops)
	}

	var err error
	fieldSet := make(map[string]string, len(val))
	for k, v := range val {
		fieldSet[k], err = toInfluxRepr(prefix, v, false)
		if err != nil {
			return err
		}
	}

	_, err = a.Write([]byte(a.formatLineProtocol(prefix, tags, fieldSet, ops.time)))
	return err
}

// NewEncoder creates a new encoder that writes to the given io.Writer.
func NewEncoder(w io.Writer) *Encoder {
	a := &Encoder{
		host:   getFQDN(),
		Writer: w,
	}
	return a
}

// NewEncoderWithHostname creates a new encoder that writes to the given
// io.Writer with an overridden hostname
func NewEncoderWithHostname(w io.Writer, host string) *Encoder {
	a := &Encoder{
		host:   host,
		Writer: w,
	}
	return a
}
