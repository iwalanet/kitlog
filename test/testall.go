package main

import (
	"fmt"
	"time"

	"github.com/go-kit/log/level"
	"github.com/iwalanet/kitlog"
)

func main() {
	cfg := kitlog.Config{
		File:  "test.log",
		Level: 4,
		ES: &kitlog.ESConfig{
			Addr:   "127.0.0.1:9200",
			User:   "elastic",
			Pass:   "biaodi",
			Stream: "tests",
		},
	}

	if err := kitlog.Open(&cfg); err != nil {
		fmt.Printf("open failed error=%s", err.Error())
		panic(err.Error())
	}

	w := kitlog.Wrapper()
	w.Exchange("binance").Symbol("usdt_btc").Event("test_event").Message("this is a test message").Log()
	level.Info(w.Exchange("okex").Message("this is a test message too!").ES().New()).Log("name", "hehehe wo lai le")

	time.Sleep(time.Second)
	if err := kitlog.Close(); err != nil {
		fmt.Printf("close failed error=%s", err.Error())
		panic(err.Error())
	}
}
