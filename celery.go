package celery

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type BrokerBackend interface {
	Publish(ctx context.Context, message []byte, rawMessage string, queue string) error
	Get(ctx context.Context, taskID string) *redis.StringCmd
}

type ICeleryClient interface {
	Delay(ctx context.Context, queue string, taskName string, args ...interface{}) (string, error)
	GetResult(ctx context.Context, taskID string) (*ResultMessage, error)
}

type CeleryClient struct {
	brokerBackend BrokerBackend
}

// GetResult queries redis backend to get asynchronous result
func (cc *CeleryClient) GetResult(ctx context.Context, taskID string) (*ResultMessage, error) {
	val, err := cc.brokerBackend.Get(ctx, taskID).Bytes()
	if err != nil {
		return nil, err
	}
	var resultMessage ResultMessage
	err = json.Unmarshal(val, &resultMessage)
	if err != nil {
		return nil, err
	}

	return &resultMessage, nil
}

func (cc *CeleryClient) Delay(ctx context.Context, queue string, taskName string, args ...interface{}) (messageId string, err error) {
	messageId = uuid.NewString()
	var encodedMessage string
	encodedMessage, err = encodeMessage(taskName, messageId, args)
	if err != nil {
		return
	}

	celeryMessage := CeleryMessage{
		Body:            encodedMessage,
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		Properties: CeleryProperties{
			BodyEncoding:  "base64",
			CorrelationID: uuid.NewString(),
			ReplyTo:       uuid.NewString(),
			DeliveryInfo: CeleryDeliveryInfo{
				Priority:   0,
				RoutingKey: queue,
				Exchange:   queue,
			},
			DeliveryMode: 2,
			DeliveryTag:  uuid.NewString(),
		},
	}
	var encodedCeleryMessage []byte
	encodedCeleryMessage, err = json.Marshal(celeryMessage)

	if err != nil {
		return
	}

	err = cc.brokerBackend.Publish(ctx, encodedCeleryMessage, string(encodedCeleryMessage), queue)
	if err != nil {
		return
	}

	return
}

type celery struct {
	client ICeleryClient
}

type CeleryMessage struct {
	Body            string                 `json:"body"`
	Headers         map[string]interface{} `json:"headers,omitempty"`
	ContentType     string                 `json:"content-type"`
	Properties      CeleryProperties       `json:"properties"`
	ContentEncoding string                 `json:"content-encoding"`
}

type CeleryProperties struct {
	BodyEncoding  string             `json:"body_encoding"`
	CorrelationID string             `json:"correlation_id"`
	ReplyTo       string             `json:"reply_to"`
	DeliveryInfo  CeleryDeliveryInfo `json:"delivery_info"`
	DeliveryMode  int                `json:"delivery_mode"`
	DeliveryTag   string             `json:"delivery_tag"`
}

type CeleryDeliveryInfo struct {
	Priority   int    `json:"priority"`
	RoutingKey string `json:"routing_key"`
	Exchange   string `json:"exchange"`
}

type TaskMessage struct {
	Task    string                 `json:"task"`
	ID      string                 `json:"id"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	ETA     *string                `json:"eta"`
	Retries int                    `json:"retries"`
}

// ResultMessage is return message received from broker
type ResultMessage struct {
	ID        string        `json:"task_id"`
	Status    string        `json:"status"`
	Traceback interface{}   `json:"traceback"`
	Result    interface{}   `json:"result"`
	Children  []interface{} `json:"children"`
}

func encodeMessage(taskName string, messageId string, args []interface{}) (string, error) {

	if args == nil {
		args = make([]interface{}, 0)
	}

	tm := TaskMessage{
		Task:   taskName,
		Args:   args,
		Kwargs: map[string]interface{}{},
		ID:     messageId,
		ETA:    nil,
	}

	message, err := json.Marshal(tm)
	if err != nil {
		return "", err
	}
	encoded := base64.StdEncoding.EncodeToString(message)
	return encoded, nil
}

func newCeleryClient(redisClient *redis.Client) (ICeleryClient, error) {
	brokerImpl := &RedisBroker{
		redisClient: redisClient,
	}

	return &CeleryClient{
		brokerBackend: brokerImpl,
	}, nil

}
