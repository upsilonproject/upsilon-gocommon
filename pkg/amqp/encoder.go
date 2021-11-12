package amqp

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
)

func EncodeMessage(in interface{}) []byte {
	jsonBytes, err := json.Marshal(in)

	if err != nil {
		log.Warnf("Could not encode message %v", err)
	}

	return jsonBytes
}
