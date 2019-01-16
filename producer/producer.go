package main

import (
	"flag"
	"os"
	"os/signal"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/sko00o/mqtt"
)

const (
	mqttURL  = "mqtt://logs:logs123@192.168.20.47:1883"
	clientID = "test_client"
	topic    = "test_topic"
)

func main() {

	url := flag.String("url", mqttURL, "set mqtt broker url schema here")
	tpc := flag.String("tpc", topic, "set one topic here")
	cid := flag.String("cid", clientID, "set clientID here")
	flag.Parse()

	bkd := mqtt.NewMqttBroker(*url, *cid)
	if err := bkd.Connect(); err != nil {
		log.Fatal(err)
	}

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	for {
		select {
		case <-sig:
			return
		case <-time.After(time.Second * 2):
			msg := time.Now().String()
			log.Infoln("send: ", msg)
			bkd.Publish(*tpc, 2, true, []byte(msg))
		}
	}

}
