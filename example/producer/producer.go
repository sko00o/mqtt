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
	mqttURL  = "mqtt://usr:pwd@localhost:1883"
	clientID = "test_producer"
	topic    = "test/topic"
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
	defer bkd.Disconnect()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	for {
		select {
		case <-sig:
			return
		case curTime := <-time.After(time.Second * 2):
			msg := curTime.String()
			log.Infoln("send: ", msg)
			if err := bkd.Publish(*tpc, 2, true, []byte(msg)); err != nil {
				log.Error("publish failed:", err)
			}
		}
	}

}
