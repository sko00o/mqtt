package main

import (
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/surgemq/message"
	"github.com/surgemq/surgemq/service"
)

var (
	clientCmd = &cobra.Command{
		Use:   "client",
		Short: "Simple client demo with SurgeMQ",
	}
	c *service.Client

	done chan struct{}
)

var (
	host     string
	username string
	password string
	topics   []string
)

const (
	retryInterval = 2 * time.Second
)

func init() {
	clientCmd.Flags().StringVarP(&host, "server", "s", "tcp://mqtt.domain:1883", "server host:port")
	clientCmd.Flags().StringVarP(&username, "username", "u", "usr", "client username")
	clientCmd.Flags().StringVarP(&password, "password", "p", "pwd", "client password")
	clientCmd.Flags().StringSliceVarP(&topics, "topic", "t", []string{"test/topic"}, "comma separated list of topics to subscribe to")
	clientCmd.Run = client
}

func client(_ *cobra.Command, _ []string) {
	// Instantiates a new Client
	c = &service.Client{}

	// Creates a new MQTT CONNECT message and sets the proper parameters
	opts := message.NewConnectMessage()
	opts.SetVersion(4)
	opts.SetCleanSession(true)
	opts.SetUsername([]byte(username))
	opts.SetPassword([]byte(password))
	opts.SetClientId([]byte(fmt.Sprintf("surge_client%d%d", os.Getpid(), time.Now().Unix())))
	opts.SetKeepAlive(300)

	// Connects to the server
	for {
		if err := c.Connect(host, opts); err != nil {
			log.Warnf("connect error: %s, will retry in %s", err, retryInterval)
			time.Sleep(retryInterval)
			continue
		}
		break
	}
	log.Info("connect success")

	// Creates a new SUBSCRIBE message to subscribe to topics
	subOpts := message.NewSubscribeMessage()
	for _, t := range topics {
		_ = subOpts.AddTopic([]byte(t), 0)
	}

	for {
		if err := c.Subscribe(subOpts, nil, subHandle); err != nil {
			log.Warnf("subscribe error: %s, will retry in %s", err, retryInterval)
			time.Sleep(retryInterval)
			continue
		}
		break
	}
	log.Info("subscribe success")

	<-done
}

func subHandle(msg *message.PublishMessage) error {
	log.Infof("receive: %s", msg.Payload())
	return nil
}

func main() {
	if err := clientCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
