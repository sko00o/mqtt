package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/sko00o/mqtt"
)

/*const (
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
	defer bkd.Disconnect()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	log.Warn("exit with signal: ", <-sig)
}*/

const (
	host     = "mqtt.domain:1883"
	username = "usr"
	password = "pwd"

	clientID = "test-consumer"
	topic    = "test/topic"
)

func main() {

	hst := flag.String("host", host, "set mqtt broker host:port")
	usr := flag.String("usr", username, "set mqtt username")
	pwd := flag.String("pwd", password, "set mqtt password")

	tpc := flag.String("tpc", topic, "set topics here")
	cid := flag.String("cid", clientID, "set clientID here")
	flag.Parse()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	bkd := mqtt.MQTTBroker(&mqtt.ConnInfo{
		Host:     *hst,
		Username: *usr,
		Password: *pwd,
	}, *cid)
	if err := bkd.Connect(); err != nil {
		log.Fatal(err)
	}
	defer bkd.Disconnect()

	// consumer process
	if err := bkd.Handle(
		strings.Split(*tpc, ","),
		func(msg []byte) {
			log.Infof("receive: %s", msg)
		},
	); err != nil {
		log.Fatal(err)
	}
	log.Warn("exit with signal: ", <-sig)
}
