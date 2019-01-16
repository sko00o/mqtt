package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/sko00o/mqtt"
)

const (
	mqttURL  = "mqtt://usr:pwd@localhost:1883"
	clientID = "test_consumer"
	topic    = "test/topic"
)

func main() {

	url := flag.String("url", mqttURL, "set mqtt broker url schema here")
	tpc := flag.String("tpc", topic, "set topics here")
	cid := flag.String("cid", clientID, "set clientID here")
	flag.Parse()

	bkd := mqtt.NewMqttBroker(*url, *cid)
	if err := bkd.Connect(); err != nil {
		log.Fatal(err)
	}

	bkd.Handle(
		strings.Split(*tpc, ","),
		func(msg []byte) {
			log.Infof("receive: %s", msg)
		},
	)

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	log.Warn("exit with signal: ", <-sig)
}
