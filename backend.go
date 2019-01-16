package mqtt

import (
	"net/url"

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

type mqttImpl struct {
	url    string
	client mqtt.Client
}

func NewMqttBroker(rawURL string, clientID string) Mqtt {
	url, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}

	return &mqttImpl{
		url:    url.Host,
		client: client(url, clientID),
	}
}

func client(url *url.URL, clientID string) mqtt.Client {

	connOpts := mqtt.NewClientOptions().
		AddBroker("tcp://" + url.Host).
		SetClientID(clientID).
		SetUsername(url.User.Username()).
		SetOnConnectHandler(func(client mqtt.Client) {
			logMqtt.WithFields(logrus.Fields{
				"broker":   url.Host,
				"ClientID": clientID,
			}).Info("mqtt connected")
		}).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			logMqtt.WithError(reason).Warn("mqtt connection lost")
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

	return nil
}

func (m *mqttImpl) Publish(topic string, qos byte, retailed bool, message []byte) error {
	if t := m.client.Publish(topic, qos, retailed, message); t.Wait() && t.Error() != nil {
		return t.Error()
	}
	return nil
}
