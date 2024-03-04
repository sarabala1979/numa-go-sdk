package main

import (
	"context"
	"github.com/numaproj/numaflow-go/pkg/mapper"
	"log"
)

func mapFn(_ context.Context, keys []string, d mapper.Datum) mapper.Messages {
	msg := d.Value()
	results := mapper.MessagesBuilder()

	output := process(d.EventTime(), d.Watermark(), msg)
	results = results.Append(mapper.NewMessage(output))
	return results
}

func main() {
	err := mapper.NewServer(mapper.MapperFunc(mapFn)).Start(context.Background())
	if err != nil {
		log.Panic("Failed to start map function server: ", err)
	}
}
