package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"bosun.org/opentsdb"
	"github.com/urfave/cli"
)

// For each metric, we need to know:
// What that metrics oldest and newest datapoints are.
// For each tag k/v of a metric, we need to know:
// What that the oldest and newest datapoints are for that k/v.

// For each day over our expire limit, get the day's max and min. If both are
// 0, delete that day.

var debug = false

func main() {
	app := cli.NewApp()
	app.Name = "tsdb-expire"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "config.toml",
			Usage: "Load configuration from `FILE`.",
		},
		cli.StringFlag{
			Name:  "host",
			Usage: "Opentsdb `HOST`",
		},
		cli.StringFlag{
			Name:  "port, p",
			Usage: "Opentsdb `PORT`",
		},
		cli.BoolFlag{
			Name:  "noop, n",
			Usage: "Don't modify data, but still run",
		},
		cli.BoolFlag{
			Name:  "debug, d",
			Usage: "Enable debug mode.",
		},
	}

	app.Action = Run

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("Error starting app: %s", err)
	}
}

func Run(c *cli.Context) error {

	metrics, err := listMetrics()
	if err != nil {
		return err
	}

	for _, m := range metrics {
		era := findMetricEra(m)
		if era == 0 {
			fmt.Printf("Metric %s has no datapoints!\n", m)
		}
		if era > 30 {
			fmt.Printf("Metric %s has no datapoints since %d days ago!\n", m, era)
		}

	}
	return nil
}

// Returns a list of all metrics.
func listMetrics() ([]string, error) {

	cmd := exec.Command("tsdb", "uid", "grep", "metrics", ".")

	// Output in the form of
	// metrics win.system.handle_count: [0, 3, 109]
	// metrics wireless.client.rssi: [0, 10, 8]
	// ...
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	results := make([]string, 0)
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.HasPrefix(line, "metrics ") {
			continue
		}
		fields := strings.Split(line, ":")
		results = append(results, strings.Split(fields[0], " ")[1])
	}

	return results, nil
}

// Returns when this metric was most recently used?
// Short-circuit if the metric is active today.
// If not active today, go back one breadth of time at a time to find when it was last active.
func findMetricEra(metric string) int {

	var query opentsdb.Query

	query.Metric = metric
	query.Downsample = "1d-count"
	query.Aggregator = "sum"

	for days := 1; days < 3650; days++ {
		var request opentsdb.Request
		request.Start = fmt.Sprintf("%dd-ago", days)
		request.Queries = []*opentsdb.Query{&query}

		resp, err := request.Query("ny-tsdb01:4242")
		if err != nil {
			fmt.Println(err)
			return 0
		}
		if len(resp) > 0 {
			return days
		}
	}

	return 0
}
