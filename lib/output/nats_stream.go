package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNATSStream] = TypeSpec{
		constructor: fromSimpleConstructor(NewNATSStream),
		Summary: `
Publish to a NATS Stream subject.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"urls",
				"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"nats://127.0.0.1:4222"},
				[]string{"nats://username:password@127.0.0.1:4222"},
			),
			docs.FieldCommon("cluster_id", "The cluster ID to publish to."),
			docs.FieldCommon("subject", "The subject to publish to."),
			docs.FieldCommon("client_id", "The client ID to connect with."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewNATSStream creates a new NATSStream output type.
func NewNATSStream(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	w, err := writer.NewNATSStream(conf.NATSStream, log, stats)
	if err != nil {
		return nil, err
	}
	a, err := NewAsyncWriter(TypeNATSStream, conf.NATSStream.MaxInFlight, w, log, stats)
	if err != nil {
		return nil, err
	}
	return onlySinglePayloads(a), nil
}

//------------------------------------------------------------------------------
