package mqtt

import (
	"fmt"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/sirupsen/logrus"
)

type Mqtt interface {
	Connect() error
	Disconnect()
	Handle(topic []string, handler func(message []byte)) error
	Publish(topic string, qos byte, retailed bool, message []byte) error
	GetAddress() string
}

const (
	waitTimeout   = 5 * time.Second
	retryInterval = 2 * time.Second
)

var logMqtt = logrus.WithField("logger", "tools/mqtt")

type sub struct {
	filters map[string]byte
	handle  func(message []byte)
}

type mqttImpl struct {
	url    string
	client mqtt.Client
	tasks  []sub
}

type ConnInfo struct {
	Host     string
	Username string
	Password string
}

func MQTTBroker(c *ConnInfo, clientID string) Mqtt {
	m := &mqttImpl{
		tasks: make([]sub, 0),
	}

	m.client = client(c, clientID, m)

	return m
}

func client(c *ConnInfo, clientID string, m *mqttImpl) mqtt.Client {
	connOpts := mqtt.NewClientOptions().
		AddBroker(c.Host). // can be url schema here, e.g tcp://usr:pwd@mqtt.domain:1883
		SetClientID(clientID).
		SetUsername(c.Username).SetPassword(c.Password).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			logMqtt.WithError(reason).Warn("mqtt connection lost")
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			logger := logMqtt.WithFields(logrus.Fields{
				"broker":   c.Host,
				"ClientID": clientID,
			})
			logger.Info("mqtt connected")

			subscribeLoop(client, m.tasks...)
		})

	client := mqtt.NewClient(connOpts)
	return client
}

func (m *mqttImpl) GetAddress() string {
	return m.url
}

func (m *mqttImpl) Connect() error {
	/*if t := m.client.Connect(); t.Wait() && t.Error() != nil {
		return t.Error()
	}*/
	connectLoop(m.client)
	return nil
}

func connectLoop(client mqtt.Client) {
	for {
		if t := client.Connect(); t.WaitTimeout(waitTimeout) && t.Error() != nil {
			logMqtt.Warnf("connect error: %s, will retry in %s", t.Error(), retryInterval)
			time.Sleep(retryInterval)
			continue
		}
		return
	}
}

func (m *mqttImpl) Disconnect() {
	m.client.Disconnect(0)
}

func (m *mqttImpl) Handle(topics []string, handle func(message []byte)) error {

	filters := make(map[string]byte)
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		filters[topic] = byte(0)
	}

	if len(filters) == 0 {
		return fmt.Errorf("no topics")
	}

	task := sub{filters, handle}

	subscribeLoop(m.client, task)

	m.tasks = append(m.tasks, task)

	return nil
}

func subscribeLoop(client mqtt.Client, tasks ...sub) {
	for _, task := range tasks {
		filters, handle := task.filters, task.handle
		for {
			t := client.SubscribeMultiple(filters, func(client mqtt.Client, msg mqtt.Message) {
				handle(msg.Payload())
			})
			if t.WaitTimeout(waitTimeout) && t.Error() != nil {
				logMqtt.WithField("topics", filters).
					Warnf("subscribe error: %s, will retry in %s", t.Error(), retryInterval)
				time.Sleep(retryInterval)
				continue
			}
			if err := qosCheck(t.(*mqtt.SubscribeToken).Result()); err != nil {
				logMqtt.WithField("topics", filters).
					Warnf("subscribe error: %s, will retry in %s", err, retryInterval)
				time.Sleep(retryInterval)
				continue
			}

			logMqtt.WithField("topics", filters).
				Info("subscribe success")
			break
		}
	}
}

func qosCheck(rst map[string]byte) error {
	for topic, qos := range rst {
		if qos < 0 || qos > 2 {
			return fmt.Errorf("invalid qos for topic %s", topic)
		}
	}
	return nil
}

func (m *mqttImpl) Publish(topic string, qos byte, retailed bool, message []byte) error {
	if t := m.client.Publish(topic, qos, retailed, message); t.Wait() && t.Error() != nil {
		return t.Error()
	}
	return nil
}
