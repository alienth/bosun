package main

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

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

var host string

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

	app.Before = func(c *cli.Context) error {
		host = c.GlobalString("host")
		return nil
	}

	app.Action = Run

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("Error starting app: %s", err)
	}
}

func Run(c *cli.Context) error {

	//metrics, err := listMetrics()
	//if err != nil {
	//	return err
	//}

	metrics := make([]metric, 1)
	metrics[0] = metric{name: "linux.mem.active"}

	for _, m := range metrics {
		err := m.gatherInfo()
		if err != nil {
			fmt.Println(err)
		}

		for t, d := range m.datapointsPerDay {
			fmt.Println(t.Unix(), d)

		}

	}

	return nil
}

// Returns a list of all metrics.
func listMetrics() ([]metric, error) {

	cmd := exec.Command("tsdb", "uid", "grep", "metrics", ".")

	// Output in the form of
	// metrics win.system.handle_count: [0, 3, 109]
	// metrics wireless.client.rssi: [0, 10, 8]
	// ...
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	results := make([]metric, 0)
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.HasPrefix(line, "metrics ") {
			continue
		}
		fields := strings.Split(line, ":")
		name := strings.Split(fields[0], " ")[1]
		m := metric{name: name}
		results = append(results, m)
	}

	return results, nil
}

// Returns when this metric was most recently used?
// Short-circuit if the metric is active today.
// If not active today, go back one breadth of time at a time to find when it was last active.
func (m *metric) gatherInfo() error {
	if m.tagKeys == nil {
		m.tagKeys = make(map[string]bool)
	}
	if m.datapointsPerDay == nil {
		m.datapointsPerDay = make(map[time.Time]int64)
	}
	if m.tagSets == nil {
		m.tagSets = make([]tagSet, 0)
	}

	var query opentsdb.Query

	query.Metric = m.name
	query.Downsample = "1d-count"
	query.Aggregator = "sum"

	for days := 1; days < 10; days++ {
		var request opentsdb.Request
		request.Start = fmt.Sprintf("%dd-ago", days)
		request.End = fmt.Sprintf("%dd-ago", days-1)
		if request.End == "0d-ago" {
			request.End = "1s-ago"
		}
		request.Queries = []*opentsdb.Query{&query}
		fmt.Println(request)

		resp, err := request.Query(host)
		if err != nil {
			return err
		}
		for _, r := range resp {
			for ts, d := range r.DPS {
				tn, err := strconv.ParseInt(ts, 10, 64)
				if err != nil {
					return err
				}
				t := time.Unix(tn, 0)
				if t.After(m.last) {
					m.last = t
				}
				if m.first.IsZero() || t.Before(m.first) {
					m.first = t
				}

				// Since we're aggregating by 1d, this should
				// always effectively be a noop.
				t = t.Truncate(time.Hour * 24)

				m.datapointsPerDay[t] += int64(d)
			}

			for _, k := range r.AggregateTags {
				if !m.tagKeys[k] {
					m.tagKeys[k] = true
				}
				//	newQuery := query
				//	newQuery.Tags = opentsdb.TagSet{k: "*"}
				//	newResp, err := request.Query(host)
				//	if err != nil {
				//		return err
				//	}
				//	for _, n := range newResp {
				//		fmt.Println(n.Tags)
				//	}

			}
		}
	}

	// Where we gather all of the tag values.
	var count int64
	days := make(sortableTimes, 0)
	for t := range m.datapointsPerDay {
		days = append(days, t)
	}
	sort.Sort(days)

	var start time.Time
	for _, t := range days {
		if start.IsZero() {
			start = t
			continue
		}
		count += m.datapointsPerDay[t]
		if count > 10000000 {
			fmt.Printf("Gathering tags on %d datapoints\n", count)
			m.gatherTagSets(start, t)
			count = 0
		}
	}
	if count != 0 {
		fmt.Printf("Gathering tags on %d datapoints\n", count)
		m.gatherTagSets(start, time.Now())
	}

	return nil
}

// Takes a start time and an end time, queries for all tags on a metric, and
// populates the tagSets field.
func (m *metric) gatherTagSets(start, end time.Time) error {
	var query opentsdb.Query

	query.Metric = m.name
	query.Downsample = "1d-count"
	query.Aggregator = "sum"
	query.Tags = make(opentsdb.TagSet)
	for k := range m.tagKeys {
		query.Tags[k] = "*"
	}

	var request opentsdb.Request
	request.Start = start.Unix()
	request.End = end.Unix()
	request.Queries = []*opentsdb.Query{&query}
	fmt.Println(request)

	//		resp, err := request.Query(host)
	//		if err != nil {
	//			return err
	//		}

	return nil
}

type sortableTimes []time.Time

func (s sortableTimes) Len() int      { return len(s) }
func (s sortableTimes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortableTimes) Less(i, j int) bool {
	return s[i].Before(s[j])
}

type metric struct {
	name             string
	first            time.Time
	last             time.Time
	tagKeys          map[string]bool
	tagSets          []tagSet
	datapointsPerDay map[time.Time]int64
}

type tagSet struct {
	set              opentsdb.TagSet
	first            time.Time
	last             time.Time
	datapointsPerDay map[time.Time]int64
}

// Deletes rows containing nothing but 0 values.
func deleteZeroValues(m metric) error {

	return nil
}
