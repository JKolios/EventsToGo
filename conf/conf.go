package conf

import (
	"encoding/json"
	"io/ioutil"
)

type Configuration struct {
	Producers    []string                     `json:"Producers"`
	Consumers    []string                     `json:"Consumers"`
	ProducerConf map[string]map[string]string `json:"ProducerConf"`
	ConsumerConf map[string]map[string]string `json:"ProducerConf"`
}

func ParseJSONFile(filename string) (Configuration, error) {

	var confObject Configuration
	confFile, err := ioutil.ReadFile(filename)

	err = json.Unmarshal(confFile, &confObject)
	return confObject, err
}
