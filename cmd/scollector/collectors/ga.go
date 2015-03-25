// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package collectors

import (
	"encoding/gob"
	"fmt"
	analytics "google.golang.org/api/analytics/v3"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"bosun.org/metadata"
	"bosun.org/opentsdb"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

var trackedSites = []*TrackedSite{}

func AddGAConfig(line string) error {
	p, err := NewTrackedSite(line)
	if err != nil {
		return err
	}
	trackedSites = append(trackedSites, p)
	return nil
}

func NewTrackedSite(site string) (*TrackedSite, error) {
	sp := strings.Split(site, ":")
	offset := 1
	var err error
	if len(sp) < 2 {
		return nil, fmt.Errorf("ga_tracked_site requires at least two fields, ex: mysite.com:1234567")
	}
	if len(sp) == 3 {
		offset, err = strconv.Atoi(sp[2])
		if err != nil {
			return nil, fmt.Errorf("ga_tracked_site format error. ex: mysite.com:1234567:8")
		}
	}
	return &TrackedSite{
		Name:    sp[0],
		Profile: "ga:" + sp[1],
		Offset:  offset,
	}, nil
}

type TrackedSite struct {
	Name    string
	Profile string
	Offset  int
}

func GA(clientid, secret string) {
	if len(trackedSites) == 0 {
		return
	}

	collectors = append(collectors, &IntervalCollector{
		F: func() (opentsdb.MultiDataPoint, error) {
			return c_ga(clientid, secret, trackedSites)
		},
		name:     "c_ga",
		Interval: time.Minute * 1,
	})
}

func c_ga(clientid string, secret string, sites []*TrackedSite) (opentsdb.MultiDataPoint, error) {
	var md opentsdb.MultiDataPoint
	var err error
	err = nil

	config := &oauth2.Config{
		ClientID:     clientid,
		ClientSecret: secret,
		Endpoint:     google.Endpoint,
		Scopes:       []string{analytics.AnalyticsScope},
	}

	ctx := context.Background()
	c := newOAuthClient(ctx, config)
	svc, _ := analytics.New(c)

	for _, site := range sites {
		call := svc.Data.Realtime.Get(site.Profile, "rt:pageviews").Dimensions("rt:minutesAgo")
		data, err := call.Do()
		if err != nil {
			continue
		}
		time := time.Now().Add(time.Duration(-1*site.Offset) * time.Minute).Unix()
		value := "0"
		for _, v := range data.Rows {
			minute, _ := strconv.Atoi(v[0])
			count := v[1]
			if minute == site.Offset {
				value = count
				break
			}
		}
		AddTS(&md, "ga.realtime.pageviews", time, value, opentsdb.TagSet{"site": site.Name}, metadata.Gauge, metadata.Count, "Number of pageviews tracked by GA in one minute")
		fmt.Printf("Addts %s %s %s %s\n", site, "ga.realtime.pageviews", value, time)
	}

	return md, err
}

func osUserCacheDir() string {
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(os.Getenv("HOME"), "Library", "Caches")
	case "linux", "freebsd":
		return filepath.Join(os.Getenv("HOME"), ".cache")
	}
	log.Printf("TODO: osUserCacheDir on GOOS %q", runtime.GOOS)
	return "."
}

func tokenCacheFile(config *oauth2.Config) string {
	hash := fnv.New32a()
	hash.Write([]byte(config.ClientID))
	hash.Write([]byte(config.ClientSecret))
	hash.Write([]byte(strings.Join(config.Scopes, " ")))
	fn := fmt.Sprintf("go-api-demo-tok%v", hash.Sum32())
	return filepath.Join(osUserCacheDir(), url.QueryEscape(fn))
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	t := new(oauth2.Token)
	err = gob.NewDecoder(f).Decode(t)
	return t, err
}

func saveToken(file string, token *oauth2.Token) {
	f, err := os.Create(file)
	if err != nil {
		log.Printf("Warning: failed to cache oauth token: %v", err)
		return
	}
	defer f.Close()
	gob.NewEncoder(f).Encode(token)
}

func newOAuthClient(ctx context.Context, config *oauth2.Config) *http.Client {
	cacheFile := tokenCacheFile(config)
	token, err := tokenFromFile(cacheFile)
	if err != nil {
		token = tokenFromWeb(ctx, config)
		saveToken(cacheFile, token)
	} else {
		log.Printf("Using cached token %#v from %q", token, cacheFile)
	}

	return config.Client(ctx, token)
}

func tokenFromWeb(ctx context.Context, config *oauth2.Config) *oauth2.Token {
	ch := make(chan string)
	randState := fmt.Sprintf("st%d", time.Now().UnixNano())
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/favicon.ico" {
			http.Error(rw, "", 404)
			return
		}
		if req.FormValue("state") != randState {
			log.Printf("State doesn't match: req = %#v", req)
			http.Error(rw, "", 500)
			return
		}
		if code := req.FormValue("code"); code != "" {
			fmt.Fprintf(rw, "<h1>Success</h1>Authorized.")
			rw.(http.Flusher).Flush()
			ch <- code
			return
		}
		log.Printf("no code")
		http.Error(rw, "", 500)
	}))
	defer ts.Close()

	config.RedirectURL = ts.URL
	authURL := config.AuthCodeURL(randState)
	go openURL(authURL)
	log.Printf("Authorize this app at: %s", authURL)
	code := <-ch
	log.Printf("Got code: %s", code)

	token, err := config.Exchange(ctx, code)
	if err != nil {
		log.Fatalf("Token exchange error: %v", err)
	}
	return token
}

func openURL(url string) {
	try := []string{"xdg-open", "google-chrome", "open"}
	for _, bin := range try {
		err := exec.Command(bin, url).Run()
		if err == nil {
			return
		}
	}
	log.Printf("Error opening URL in browser.")
}

func valueOrFileContents(value string, filename string) string {
	if value != "" {
		return value
	}
	slurp, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error reading %q: %v", filename, err)
	}
	return strings.TrimSpace(string(slurp))
}
