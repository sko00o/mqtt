package mqtt

import (
	"net/url"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sirupsen/logrus"
)

type Mqtt interface {
	Connect() error
	Disconnect()
	Handle(topic []string, handler func(message []byte)) error
	Publish(topic string, qos byte, retailed bool, message []byte) error
	GetAddress() string
}

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

func NewMqttBroker(rawURL string, clientID string) Mqtt {
	url, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}

	m := &mqttImpl{
		url:   url.Host,
		tasks: make([]sub, 0),
	}

	m.client = client(url, clientID, m)

	return m
}

func client(url *url.URL, clientID string, m *mqttImpl) mqtt.Client {
	connOpts := mqtt.NewClientOptions().
		AddBroker("tcp://" + url.Host).
		SetClientID(clientID).
		SetUsername(url.User.Username()).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			logMqtt.WithError(reason).Warn("mqtt connection lost")
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			logger := logMqtt.WithFields(logrus.Fields{
				"broker":   url.Host,
				"ClientID": clientID,
			})
			logger.Info("mqtt connected")
			for _, task := range m.tasks {
				filters, handle := task.filters, task.handle
				for {
					if t := client.SubscribeMultiple(filters, func(client mqtt.Client, msg mqtt.Message) {
						handle(msg.Payload())
					}); t.Wait() && t.Error() != nil {
						logger.Error("subscribe error", t.Error(), ",will retry in 2s")
						time.Sleep(2 * time.Second)
					} else {
						break
					}
				}
			}
		})
	if password, isSet := url.User.Password(); isSet {
		connOpts = connOpts.SetPassword(password)
	}

	client := mqtt.NewClient(connOpts)
	return client
}

func (m *mqttImpl) GetAddress() string {
	return m.url
}

func (m *mqttImpl) Connect() error {
	if t := m.client.Connect(); t.Wait() && t.Error() != nil {
		return t.Error()
	}
	return nil
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
		logMqtt.Warn("no topics")
		return nil
	}

	if t := m.client.SubscribeMultiple(filters, func(client mqtt.Client, msg mqtt.Message) {
		handle(msg.Payload())
	}); t.Wait() && t.Error() != nil {
		return t.Error()
	}

	m.tasks = append(m.tasks, sub{filters, handle})

	return nil
}

func (m *mqttImpl) Publish(topic string, qos byte, retailed bool, message []byte) error {
	if t := m.client.Publish(topic, qos, retailed, message); t.Wait() && t.Error() != nil {
		return t.Error()
	}
	return nil
}
