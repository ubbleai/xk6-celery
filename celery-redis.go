package celery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dop251/goja"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/celery", New())
}

// Duration is a wrapper of time.Duration enabling correct json marshalling
type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

type (
	// CeleryModule is the global module instance that will create Celery client
	// module instances for each VU.
	CeleryModule struct{}

	// CeleryInstance represents an instance of the JS module.
	CeleryInstance struct {
		// modules.VU provides some useful methods for accessing internal k6
		// objects like the global context, VU state and goja runtime.
		vu modules.VU
		// Celery is the exported module instance.
		*Celery
		logger logrus.FieldLogger
	}
)

// Ensure the interfaces are implemented correctly.
var (
	_ modules.Instance = &CeleryInstance{}
	_ modules.Module   = &CeleryModule{}
)

// New returns a pointer to a new RootModule instance.
func New() *CeleryModule {
	return &CeleryModule{}
}

// NewModuleInstance implements the modules.Module interface and returns
// a new instance for each VU.
func (*CeleryModule) NewModuleInstance(vu modules.VU) modules.Instance {

	logger := vu.InitEnv().Logger.WithField("component", "xk6-celery")
	return &CeleryInstance{vu: vu, Celery: &Celery{vu: vu}, logger: logger}
}

// Celery is the exported module instance.
type Celery struct {
	vu               modules.VU
	client           ICeleryClient
	backend          *redis.Client
	queue            string
	timeout          time.Duration
	getRetryInterval time.Duration
}

func (mi *CeleryInstance) NewCeleryRedis(call goja.ConstructorCall) *goja.Object {
	rt := mi.vu.Runtime()

	var optionsArg map[string]interface{}
	err := rt.ExportTo(call.Arguments[0], &optionsArg)
	if err != nil {
		common.Throw(rt, errors.New("unable to parse options object"))
	}

	opts, err := newOptionsFrom(optionsArg)
	if err != nil {
		common.Throw(rt, fmt.Errorf("invalid options; reason: %w", err))
	}

	opts.applyDefaults()
	err = opts.validate()
	if err != nil {
		common.Throw(rt, fmt.Errorf("invalid options; reason: %w", err))
	}

	mi.logger.Infof("configuration %+v", opts)

	redisClient := NewRedisClient(opts)
	client, err := newCeleryClient(redisClient)
	if err != nil {
		common.Throw(rt, fmt.Errorf("fail to innstanciate celery client; reason: %w", err))
	}

	CeleryClient := &Celery{
		vu:               mi.vu,
		client:           client,
		backend:          redisClient,
		queue:            opts.Queue,
		timeout:          opts.Timeout.Duration,
		getRetryInterval: opts.GetRetryInterval.Duration,
	}

	return rt.ToValue(CeleryClient).ToObject(rt)
}

type options struct {
	Url              string   `json:"url,omitempty"`
	SentinelAddrs    []string `json:"addrs,omitempty"`
	MasterName       string   `json:"mastername,omitempty"`
	Queue            string   `json:"queue,omitempty"`
	Timeout          Duration `json:"timeout,omitempty"`
	GetRetryInterval Duration `json:"getinterval,omitempty"`
}

func (o *options) applyDefaults() {
	if o.Url == "" {
		o.Url = "redis://127.0.0.1:6379"
	}

	if o.Queue == "" {
		o.Queue = "celery"
	}
	if o.MasterName == "" {
		o.MasterName = "default-master"
	}

	if o.Timeout.Duration == 0 {
		o.Timeout.Duration = 30 * time.Second
	}

	if o.GetRetryInterval.Duration == 0 {
		o.GetRetryInterval.Duration = 50 * time.Millisecond
	}
}

func (o *options) validate() error {
	if o.Timeout.Duration <= o.GetRetryInterval.Duration {
		return fmt.Errorf("celery backend timeout duration cannot be shorter than check interval")
	}

	if o.Queue == "" {
		return fmt.Errorf("celery target queue cannot be empty")
	}

	if o.Url == "" {
		return fmt.Errorf("celery endpoint URL cannot be empty")
	}
	if len(o.SentinelAddrs) >= 0 && o.MasterName == "" {
		return fmt.Errorf("celery endpoint redis MasterName cannot be empty")
	}

	return nil
}

// newOptionsFrom validates and instantiates an options struct from its map representation
// as obtained by calling a Goja's Runtime.ExportTo.
func newOptionsFrom(argument map[string]interface{}) (*options, error) {
	jsonStr, err := json.Marshal(argument)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize options to JSON %w", err)
	}

	// Instantiate a JSON decoder which will error on unknown
	// fields. As a result, if the input map contains an unknown
	// option, this function will produce an error.
	decoder := json.NewDecoder(bytes.NewReader(jsonStr))
	decoder.DisallowUnknownFields()

	var opts options
	err = decoder.Decode(&opts)
	if err != nil {
		return nil, fmt.Errorf("unable to decode options %w", err)
	}

	return &opts, nil
}

// Submits a new task to celery broker
// It only supports args (no kwargs)
func (c *Celery) Delay(taskName string, args ...interface{}) (string, error) {
	ctx := context.Background()
	taskId, err := c.client.Delay(ctx, taskName, c.queue, args)
	if err != nil {
		return "", err
	}
	return taskId, nil
}

// Check if task result is filled or still empty
// It's a sync call with instant result.
func (c *Celery) TaskCompleted(taskID string) (bool, error) {
	ctx := context.Background()
	result, err := c.client.GetResult(ctx, taskID)
	if err != nil {
		if err.Error() == "result not available" { // error message is hardcoded in client lib
			return false, nil
		}
		return false, err
	}

	return (result != nil), nil
}

// Wait for task to be completed until timeout is reached
// It's a blocking call that do a periodic check for any task result
// It returns true if task is processed, or false if timeout is reached.
func (c *Celery) WaitForTaskCompleted(taskID string) (bool, error) {
	ticker := time.NewTicker(c.getRetryInterval)
	timeoutChan := time.After(c.timeout)
	for {
		select {
		case <-timeoutChan:
			return false, nil
		case <-ticker.C:
			completed, _ := c.TaskCompleted(taskID)
			if completed != true {
				continue
			}
			return true, nil
		}
	}
}

// Exports implements the modules.Instance interface and returns the exports
// of the JS module.
func (mi *CeleryInstance) Exports() modules.Exports {
	return modules.Exports{Named: map[string]interface{}{
		"Redis": mi.NewCeleryRedis,
	}}
}
