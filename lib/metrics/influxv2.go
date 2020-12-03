package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	client "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/rcrowley/go-metrics"
)

func init() {
	Constructors[TypeInfluxV2] = TypeSpec{
		constructor: NewInfluxV2,
		Summary: `
Send metrics to InfluxDB 1.8+ using the /api/v2/write forward-compatible endpoint.`,
		Description: `description goes here`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "[http|udp]://host:port combination required to specify influx host."),
			docs.FieldCommon("token", "token to use for influx authentication. will override username/password"),

			docs.FieldAdvanced("username", "username for influx authentication. token is preferred."),
			docs.FieldAdvanced("password", "password for influx authentication. token is preferred."),

			docs.FieldAdvanced("batch_size", "maximum number of points sent to server in single request. default 5000."),
			docs.FieldAdvanced("flush_interval", "interval, in ms, in which is buffer flushed if it has not been already written (by reaching batch size) . default 1s."),
			docs.FieldAdvanced("max_retry_interval", "maximum retry interval, default 5m"),
			docs.FieldAdvanced("max_retries", "maximum count of retry attempts of failed writes. default 3."),
			docs.FieldAdvanced("precision", "[ns | us | ms | s] precision to use in writes for timestamp. default ns."),
			docs.FieldAdvanced("retry_buffer_limit", "maximum number of points to keep for retry. should be multiple of batch_size. default 50000."),
			docs.FieldAdvanced("retry_interval", "retry interval in ms, if not sent by server. default 5s."),
			docs.FieldAdvanced("use_gzip", "whether to use gzip compression in requests. default false"),

			docs.FieldAdvanced("tags", "tags added to each point during writing. If a point already has a tag with the same key, it is left unchanged."),

			docs.FieldAdvanced("prefix", "prefix all measurement names."),
			docs.FieldAdvanced("interval", "how often to send metrics to influx."),
			docs.FieldAdvanced("ping_interval", "how often to poll health of influx."),
			docs.FieldAdvanced("timeout", "how long to wait for response from influx for both ping and writing metrics."),
			docs.FieldAdvanced("tags", "tags to add to each metric sent to influx."),
			docs.FieldAdvanced("retention_policy", "sets the retention policy for each write."),
			docs.FieldAdvanced("write_consistency", "[any|one|quorum|all] sets write consistency when available."),
			pathMappingDocs(true),
		},
	}
}

// InfluxConfig is config for the influx v2 metrics type.
type InfluxV2Config struct {
	URL   string `json:"url" yaml:"url"`
	Token string `json:"token" yaml:"token"`

	Org    string `json:"org" yaml:"org"`
	Bucket string `json:"bucket" yaml:"bucket"`

	BatchSize        int    `json:"batch_size" yaml:"batch_size"'`
	FlushInterval    string `json:"flush_interval" yaml:"flush_interval"`
	MaxRetryInterval string `json:"max_retry_interval" yaml:"max_retry_interval"`
	MaxRetries       int    `json:"max_retries" yaml:"max_retries"`
	Precision        string `json:"precision" yaml:"precision"`
	RetryBufferLimit int    `json:"retry_buffer_limit" yaml:"retry_buffer_limit"`
	RetryInterval    string `json:"retry_interval" yaml:"retry_interval"`
	UseGZIP          bool   `json:"use_gzip" yaml:"use_gzip"`

	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`

	Prefix string            `json:"prefix" yaml:"prefix"`
	Tags   map[string]string `json:"tags" yaml:"tags"`
}

// NewInfluxConfig creates an InfluxConfig struct with default values.
func NewInfluxV2Config() InfluxV2Config {
	return InfluxV2Config{
		URL: "http://localhost:8086",

		Org:    "",
		Bucket: "db0",

		Token:    "",
		Username: "",
		Password: "",

		BatchSize:        5000,
		FlushInterval:    "1s",
		MaxRetryInterval: "5m",
		MaxRetries:       3,
		RetryInterval:    "5s",
		RetryBufferLimit: 50000,
		UseGZIP:          false,

		Prefix:    "benthos.",
		Precision: "ns",
	}
}

// InfluxV2 is the stats and client holder
type InfluxV2 struct {
	client client.Client

	interval time.Duration
	timeout  time.Duration

	ctx    context.Context
	cancel func()

	registry metrics.Registry
	config   InfluxV2Config
	log      log.Modular
}

// NewInfluxV2 creates and returns a new InfluxV2 object.
func NewInfluxV2(config Config, opts ...func(Type)) (Type, error) {
	i := &InfluxV2{
		config:   config.InfluxV2,
		registry: metrics.NewRegistry(),
		log:      log.Noop(),
	}

	i.ctx, i.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(i)
	}

	var err error

	i.interval = 5 * time.Second

	if err = i.makeClient(); err != nil {
		return nil, err
	}

	go i.loop()

	return i, nil
}

func (i *InfluxV2) makeClient() error {
	var token = i.config.Token
	if token == "" {
		token = fmt.Sprintf("%s:%s", i.config.Username, i.config.Password)
	}
	options := client.DefaultOptions()

	precision := time.Nanosecond
	switch p := i.config.Precision; p {
	case "ns":
		precision = time.Nanosecond
	case "us":
		precision = time.Microsecond
	case "ms":
		precision = time.Millisecond
	case "s":
		precision = time.Second
	default:
		return fmt.Errorf("bad precision format: %s", i.config.Precision)
	}

	options.SetPrecision(precision)

	flushInterval, err := time.ParseDuration(i.config.FlushInterval)
	if err != nil {
		return fmt.Errorf("failed to parse flush interval: %v", err)
	}
	options.SetFlushInterval(uint(flushInterval.Milliseconds()))

	maxRetryInterval, err := time.ParseDuration(i.config.MaxRetryInterval)
	if err != nil {
		return fmt.Errorf("failed to parse max retry interval: %v", err)
	}
	options.SetMaxRetryInterval(uint(maxRetryInterval.Milliseconds()))

	retryInterval, err := time.ParseDuration(i.config.RetryInterval)
	if err != nil {
		return fmt.Errorf("failed to parse retry interval: %v", err)
	}
	options.SetRetryInterval(uint(retryInterval.Milliseconds()))

	options.SetBatchSize(uint(i.config.BatchSize))
	options.SetMaxRetries(uint(i.config.MaxRetries))
	options.SetRetryBufferLimit(uint(i.config.RetryBufferLimit))
	options.SetUseGZip(i.config.UseGZIP)

	for k, v := range i.config.Tags {
		options.AddDefaultTag(k, v)
	}

	i.client = client.NewClientWithOptions(i.config.URL, token, options)
	return nil
}

func (i *InfluxV2) loop() {
	ticker := time.NewTicker(i.interval)
	defer ticker.Stop()
	for {
		select {
		case <-i.ctx.Done():
			i.log.Infoln("i.ctx.Done()")
			return
		case <-ticker.C:
			i.log.Infoln("i.publishRegistry()")
			if err := i.publishRegistry(); err != nil {
				i.log.Errorf("failed to send metrics data: %v", err)
			}
		}
	}
}

func (i *InfluxV2) publishRegistry() error {
	api := i.client.WriteAPIBlocking(i.config.Org, i.config.Bucket)

	now := time.Now()
	all := i.getAllMetrics()
	points := make([]*write.Point, len(all))

	idx := 0
	for k, v := range all {
		name, normalTags := decodeInfluxName(k)
		tags := make(map[string]string, len(i.config.Tags)+len(normalTags))
		for k, v := range normalTags {
			tags[k] = v
		}
		points[idx] = client.NewPoint(name, tags, v, now)
		idx++
	}
	return api.WritePoint(context.Background(), points...)
}

func (i *InfluxV2) getAllMetrics() map[string]map[string]interface{} {
	data := make(map[string]map[string]interface{})
	i.registry.Each(func(name string, i interface{}) {
		values := make(map[string]interface{})
		switch metric := i.(type) {
		case metrics.Counter:
			values["count"] = metric.Count()
		case metrics.Gauge:
			values["value"] = metric.Value()
		case metrics.GaugeFloat64:
			values["value"] = metric.Value()
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			values["count"] = t.Count()
			values["min"] = t.Min()
			values["max"] = t.Max()
			values["mean"] = t.Mean()
			values["stddev"] = t.StdDev()
			values["p50"] = ps[0]
			values["p75"] = ps[1]
			values["p95"] = ps[2]
			values["p99"] = ps[3]
			values["p999"] = ps[3]
			values["1m.rate"] = t.Rate1()
			values["5m.rate"] = t.Rate5()
			values["15m.rate"] = t.Rate15()
			values["mean.rate"] = t.RateMean()
		}
		data[name] = values
	})
	return data
}

func (i *InfluxV2) GetCounter(path string) StatCounter {
	if len(path) == 0 {
		return DudStat{}
	}
	return i.registry.GetOrRegister(i.config.Prefix+path, func() metrics.Counter {
		return NewCounter()
	}).(InfluxCounter)
}

func (i *InfluxV2) GetCounterVec(path string, n []string) StatCounterVec {
	if len(path) == 0 {
		return fakeCounterVec(func([]string) StatCounter {
			return DudStat{}
		})
	}
	return &fCounterVec{
		f: func(l []string) StatCounter {
			encodedName := encodeInfluxName(i.config.Prefix+path, n, l)
			return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
				return NewCounter()
			}).(InfluxCounter)
		},
	}
}

func (i *InfluxV2) GetTimer(path string) StatTimer {
	if len(path) == 0 {
		return DudStat{}
	}
	return i.registry.GetOrRegister(i.config.Prefix+path, func() metrics.Timer {
		return NewTimer()
	}).(InfluxTimer)
}

func (i *InfluxV2) GetTimerVec(path string, n []string) StatTimerVec {
	if len(path) == 0 {
		return fakeTimerVec(func([]string) StatTimer {
			return DudStat{}
		})
	}
	return &fTimerVec{
		f: func(l []string) StatTimer {
			encodedName := encodeInfluxName(i.config.Prefix+path, n, l)
			return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
				return NewTimer()
			}).(InfluxTimer)
		},
	}
}

func (i *InfluxV2) GetGauge(path string) StatGauge {
	var result = i.registry.GetOrRegister(i.config.Prefix+path, func() metrics.Gauge {
		return NewGauge()
	}).(InfluxGauge)
	return result
}

func (i *InfluxV2) GetGaugeVec(path string, n []string) StatGaugeVec {
	if len(path) == 0 {
		return fakeGaugeVec(func([]string) StatGauge {
			return DudStat{}
		})
	}
	return &fGaugeVec{
		f: func(l []string) StatGauge {
			encodedName := encodeInfluxName(i.config.Prefix+path, n, l)
			return i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
				return NewGauge()
			}).(InfluxGauge)
		},
	}
}

func (i *InfluxV2) SetLogger(log log.Modular) {
	i.log = log
}

func (i *InfluxV2) Close() error {
	i.log.Infoln("i.client.Close()")
	i.client.Close()
	return nil
}
