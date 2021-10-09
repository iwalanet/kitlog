package es

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
)

//es indexTemplate 初始化

const (
	TypeString  = "string"
	TypeText    = "text"
	TypeKeyWord = "keyword"
	TypeDate    = "date"
	TypeObject  = "object"

	staticTemplateName = "go-common-datastreams"
	indexTemplateName  = "go-common-logs"
)

type (
	dataStream struct {
		Hidden bool `json:"hidden"`
	}

	indexTemplate struct {
		ComposedOf      []string               `json:"composed_of"`
		IndexPatterns   []string               `json:"index_patterns"`
		Poriority       int                    `json:"priority"`
		Version         int                    `json:"version"`
		Meta            map[string]interface{} `json:"_meta,omitempty"`
		DataStream      dataStream             `json:"data_stream"`
		AllowAutoCreate bool                   `json:"allow_auto_create"`
	}
)

var (
	dataStreamMapping = map[string]interface{}{
		"template": map[string]interface{}{
			"mappings": map[string]interface{}{
				"date_detection": false,
				"dynamic":        false,

				"properties": map[string]map[string]interface{}{
					"@timestamp": {
						"type": TypeDate,
					},
					"programme": {
						"type": TypeKeyWord,
					},
					"event": {
						"type": TypeKeyWord,
					},
					"level": {
						"type": TypeKeyWord,
					},
					"exchange": {
						"type": TypeKeyWord,
					},
					"symbol": {
						"type": TypeKeyWord,
					},
					"error": {
						"type": TypeText,
					},
					"message": {
						"type": TypeText,
					},
					"id": {
						"type": TypeKeyWord,
					},
					"es": { //"es"字段用于logrouter区分log是否写入到es，并没有其他用途
						"type":    TypeObject,
						"enabled": false,
					},
				},
			},
		},
		"version": 1,
		"_meta": map[string]interface{}{
			"description": "mapping conventions for go.common kitlog",
		},
	}

	esAddr        string
	esCredentials string
)

//Create 初始化indexTemplate.用于创建datastream, dataStream名称可以通过DataStream方法构建
//确保符合indexTemplate pattern
//
// addr elastic 服务地址(ip:port)
// user 用户名Authorization用 ""不进行Authorization
// pass 同user
func Create(addr string, user string, pass string) error {
	esAddr = addr

	if len(user) != 0 {
		esCredentials = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", user, pass)))
	}
	if err := initDynamicTemplate(); err != nil {
		return errors.WithMessage(err, "init component template fail")
	}

	if err := initIndexTemplate(); err != nil {
		return errors.WithMessage(err, "init index template fail")
	}
	return nil
}

//DataStreamName 根据name构建符合 indexTemplateName的 streamName
func DataStreamName(name string) string {
	return fmt.Sprintf("go-common-%s", name)
}

//initDynamicTemplate 初始化component template用于字段映射，具体映射规则查看dataStreamMappings
func initDynamicTemplate() error {
	url := fmt.Sprintf("http://%s/_component_template/%s", esAddr, staticTemplateName)
	return putInternal(url, dataStreamMapping)
}

//initIndexTemplate 初始化创建go-common datastreams的index template
func initIndexTemplate() error {
	it := indexTemplate{
		ComposedOf:    []string{staticTemplateName, "logs-mappings", "logs-settings"},
		IndexPatterns: []string{"go-common-*"},
		Poriority:     200,
		Version:       1,
		Meta: map[string]interface{}{
			"description": "index template which used to gocommon datastreams",
		},
		DataStream: dataStream{
			Hidden: false,
		},
		AllowAutoCreate: true,
	}

	return putInternal(fmt.Sprintf("http://%s/_index_template/%s", esAddr, indexTemplateName), &it)
}

func putInternal(url string, obj interface{}) error {
	resp, err := http.Get(url)
	if err != nil {
		return errors.WithMessage(err, "read existing data fail")
	}
	if resp.StatusCode == 200 {
		return nil
	}

	raw, _ := json.Marshal(obj)
	buf := bytes.NewBuffer(raw)
	req, err := http.NewRequest(http.MethodPut, url, buf)
	if err != nil {
		return errors.WithMessage(err, "build request fail")
	}
	req.Header.Add("Content-Type", "application/json")
	if len(esCredentials) != 0 {
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", esCredentials))
	}

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return errors.WithMessage(err, "put data failed")
	}

	if resp.StatusCode > 299 {
		raw, _ := ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		return errors.Errorf("bad response code %d resp '%s'", resp.StatusCode, string(raw))
	}

	return nil
}
