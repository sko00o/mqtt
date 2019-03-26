package main

import (
	"flag"
	"os"
	"os/signal"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/sko00o/mqtt"
)

/*const (
	mqttURL  = "mqtt://usr:pwd@localhost:1883"
	clientID = "test_producer"
	topic    = "test/topic"
)

func main() {

	url := flag.String("url", mqttURL, "set mqtt broker url schema here")
	tpc := flag.String("tpc", topic, "set one topic here")
	cid := flag.String("cid", clientID, "set clientID here")
	flag.Parse()

	bkd := mqtt.MQTTBroker(*url, *cid)
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
*/

const (
	host     = "localhost:1883"
	username = "usr"
	password = "pwd"

	clientID = "test-producer"
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

	// producer process
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
