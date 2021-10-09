package es

import (
	"sync"

	"github.com/go-kit/log"
)

type (
	//Wrapper 实现log interface.封装了elastic search字段
	//(避免常用字段设置名称不一致)便于统一处理分析
	Wrapper struct {
		mu     sync.Mutex
		fields map[string]interface{}
		logger log.Logger
	}
)

var (
	ESField = "es"
	//ESOn 设置的"es" field值, 设置为public供其他逻辑判断"es"是否设置为ESOn
	ESOn = "{}"
)

func NewWrapper(logger log.Logger) *Wrapper {
	return &Wrapper{
		fields: make(map[string]interface{}),
		logger: logger,
	}
}

//Log 根据args以及添加的fields记录日志, 并清空fields
func (w *Wrapper) Log(args ...interface{}) error {
	fields := w.toFields()
	args = append(args, fields...)
	return w.logger.Log(args...)
}

//New 根据添加的fields，以及logger已有的fields 创建新的logger, 并清空wrapper fields
func (w *Wrapper) New() *Wrapper {
	fields := w.toFields()
	newLogger := log.With(w.logger, fields...)
	return NewWrapper(newLogger)
}

//Exchange add "exchange" field
func (w *Wrapper) Exchange(ex string) *Wrapper {
	return w.addFields("exchange", ex)
}

//Event add "event" field, 用于唯一确定一个event类型
func (w *Wrapper) Event(ev string) *Wrapper {
	return w.addFields("event", ev)
}

//Programme add "programme" field 用于区分不同程序
func (w *Wrapper) Programme(p string) *Wrapper {
	return w.addFields("programme", p)
}

//Symbol add "symbol" field 交易对
func (w *Wrapper) Symbol(sym string) *Wrapper {
	return w.addFields("symbol", sym)
}

//Error add "error" field 记录的错误信息
func (w *Wrapper) Error(err error) *Wrapper {
	return w.addFields("error", err.Error())
}

//ES add "es" field 是否发送到elasticsearch
func (w *Wrapper) ES() *Wrapper {
	return w.addFields(ESField, ESOn)
}

//Message 添加些细节信息
func (w *Wrapper) Message(msg string) *Wrapper {
	return w.addFields("message", msg)
}

//ID 添加id字段唯一标志一系列日志
func (w *Wrapper) ID(id interface{}) *Wrapper {
	return w.addFields("id", id)
}

func (w *Wrapper) addFields(key string, val interface{}) *Wrapper {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fields[key] = val
	return w
}

func (w *Wrapper) toFields() []interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	ret := []interface{}{}
	for k, v := range w.fields {
		ret = append(ret, k, v)
	}
	w.fields = make(map[string]interface{})
	return ret
}
