package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"bosun.org/opentsdb"
	"github.com/BurntSushi/toml"
	"github.com/gobwas/glob"
	"github.com/urfave/cli"
)

// For each metric, we need to know:
// What that metrics oldest and newest datapoints are.
// For each tag k/v of a metric, we need to know:
// What that the oldest and newest datapoints are for that k/v.

// For each day over our expire limit, get the day's max and min. If both are
// 0, delete that day.

var debug = false

const breadth = time.Hour * 6

var host string
var now = time.Now()

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
		debug = c.GlobalBool("debug")
		return nil
	}

	app.Action = Run

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("Error starting app: %s", err)
	}
}

type configFile struct {
	Rule     []*rule
	LookBack opentsdb.Duration
}

var config configFile

func loadRules(c *cli.Context) error {
	body, err := ioutil.ReadFile(c.String("config"))
	if err != nil {
		return err
	}

	if err := toml.Unmarshal(body, &config); err != nil {
		return err
	}

	for _, r := range config.Rule {
		for _, m := range r.Metrics {
			g, err := glob.Compile(m)
			if err != nil {
				return err
			}
			r.Globs = append(r.Globs, g)
		}
	}

	return nil
}

func Run(c *cli.Context) error {

	metrics, err := listMetrics()
	if err != nil {
		return err
	}

	if err := loadRules(c); err != nil {
		return err
	}

	for _, r := range config.Rule {

		for _, m := range metrics {
			found := false
			for _, g := range r.Globs {
				if g.Match(m.name) {
					found = true
					break
				}
			}
			if !found {
				continue
			}

			if err := process(m, r); err != nil {
				return err
			}

		}

	}

	return nil
}

type rule struct {
	Metrics  []string
	Globs    []glob.Glob
	Expire   opentsdb.Duration
	Cooldown opentsdb.Duration
	ZeroOnly bool
}

func process(m metric, r *rule) error {

	lookBack := now.Add(time.Duration(config.LookBack) * -1).Truncate(time.Hour * 24)
	expire := now.Add(time.Duration(r.Expire) * -1).Truncate(time.Hour * 24)

	start := lookBack
	end := expire
	var cooldown time.Time
	dataWithinCooldown := false
	if r.Cooldown.Seconds() != 0 {
		cooldown = now.Add(time.Duration(r.Cooldown) * -1).Truncate(time.Hour * 24)
		if cooldown.After(end) {
			end = cooldown
		}
	}

	m.gatherInfo(start, end, false)

	if len(m.datapointsPerDay) == 0 {
		fmt.Printf("No datapoints found for %s between %s and %s.\n", m.name, start, end)
		return nil
	}

	days := m.sortedDays(true)
	if days[0].After(cooldown) {
		dataWithinCooldown = true
	}
	if !cooldown.IsZero() && dataWithinCooldown {
		fmt.Printf("%s has data within cooldown. Ignoring\n", m.name)
	}

	deleteStart := days[len(days)-1]
	deleteEnd := expire
	if days[0].Before(expire) {
		deleteEnd = days[0]
	}
	if deleteStart.Before(expire) {
		if r.ZeroOnly {
			if err := m.deleteZeroOnly(deleteStart, deleteEnd); err != nil {
				return err
			}
		} else {
			if err := m.delete(deleteStart, deleteEnd); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m metric) delete(start, end time.Time) error {
	fmt.Printf("Deleting datapoints for metric %s. Start: %s, End: %s\n", m.name, start, end)
	var query opentsdb.Query

	query.Metric = m.name
	query.Downsample = "1d-count"
	query.Aggregator = "sum"

	for ; start.Before(end); start = start.Add(breadth) {
		var request opentsdb.Request
		request.Start = start.Unix()
		request.End = start.Add(breadth).Unix()
		request.Queries = []*opentsdb.Query{&query}
		// request.Delete = true

		if debug {
			fmt.Println(request)
		}
		_, err := request.Query(host)
		if err != nil {
			return err
		}
	}

	return nil

}

func (m metric) deleteZeroOnly(start, end time.Time) error {
	// queries := make([]*opentsdb.Query, 2)
	queries := []*opentsdb.Query{&opentsdb.Query{}, &opentsdb.Query{}}
	queries[0].Metric = m.name
	queries[0].Downsample = "1d-max"
	queries[0].Aggregator = "sum"
	queries[1].Metric = m.name
	queries[1].Downsample = "1d-min"
	queries[1].Aggregator = "sum"

	for ; start.Before(end); start = start.Add(breadth) {
		var request opentsdb.Request
		request.Start = start.Unix()
		request.End = start.Add(breadth).Unix()
		request.Queries = queries

		if debug {
			fmt.Println(request)
		}
		resp, err := request.Query(host)
		if err != nil {
			return err
		}

		nonZeroesPresent := false
		for _, r := range resp {
			for _, d := range r.DPS {
				if d != 0 {
					nonZeroesPresent = true
					break
				}
			}
		}
		if !nonZeroesPresent {
			if err := m.delete(start, end); err != nil {
				return err
			}
		}

	}
	return nil
}

// sortedDays returns a sorted list of time.Times representing days that this
// metric has datapoints.
func (m metric) sortedDays(reverse bool) []time.Time {
	days := make(sortableTimes, 0)
	for t := range m.datapointsPerDay {
		days = append(days, t)
	}
	if !reverse {
		sort.Sort(days)
	} else {
		sort.Sort(sort.Reverse(days))
	}

	return []time.Time(days)
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

func (m *metric) gatherInfo(start, end time.Time, gatherTags bool) error {
	fmt.Printf("Gathering info on %s. Start: %s, End: %s\n", m.name, start, end)
	if m.tagKeys == nil {
		m.tagKeys = make(map[string]bool)
	}
	if m.datapointsPerDay == nil {
		m.datapointsPerDay = make(map[time.Time]int64)
	}
	if m.tagSets == nil {
		m.tagSets = make(map[string]*tagSet, 0)
	}

	var query opentsdb.Query

	query.Metric = m.name
	query.Downsample = "1d-count"
	query.Aggregator = "sum"

	for ; start.Before(end); start = start.Add(breadth) {
		var request opentsdb.Request
		request.Start = start.Unix()
		request.End = start.Add(breadth).Unix()
		request.Queries = []*opentsdb.Query{&query}

		if debug {
			fmt.Println(request)
		}
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

				m.datapointsPerDay[t.Truncate(time.Hour*24)] += int64(d)
			}

			for _, k := range r.AggregateTags {
				if !m.tagKeys[k] {
					m.tagKeys[k] = true
				}
			}
		}
	}

	if gatherTags {
		// Where we gather all of the tag values.
		var count int64

		var day time.Time
		for _, t := range m.sortedDays(false) {
			if day.IsZero() {
				day = t
				continue
			}
			count += m.datapointsPerDay[t]
			if count > 10000000 {
				m.gatherTagSets(day, t)
				count = 0
			}
		}
		if count != 0 {
			if err := m.gatherTagSets(day, time.Now()); err != nil {
				return err
			}
		}
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

	resp, err := request.Query(host)
	if err != nil {
		return err
	}

	for _, r := range resp {
		if tags, ok := m.tagSets[r.Tags.String()]; !ok {
			tags = &tagSet{}
			tags.set = r.Tags
			tags.datapointsPerDay = make(map[time.Time]int64)
			m.tagSets[r.Tags.String()] = tags
		}
		tags := m.tagSets[r.Tags.String()]

		for timeStr, d := range r.DPS {
			tn, err := strconv.ParseInt(timeStr, 10, 64)
			if err != nil {
				return err
			}
			t := time.Unix(tn, 0)
			if t.After(tags.last) {
				tags.last = t
			}
			if tags.first.IsZero() || t.Before(tags.first) {
				tags.first = t
			}

			tags.datapointsPerDay[t.Truncate(time.Hour*24)] += int64(d)
		}

	}

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
	tagSets          map[string]*tagSet
	datapointsPerDay map[time.Time]int64
}

type tagSet struct {
	set              opentsdb.TagSet
	first            time.Time
	last             time.Time
	datapointsPerDay map[time.Time]int64
}
