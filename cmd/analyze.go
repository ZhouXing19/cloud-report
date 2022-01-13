// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// analyzeCmd represents the analyze command
var analyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Analyzes benchmark results",
	Long:  `Processes log files containing benchmark results and produces CSV files`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return analyzeResults()
	},
}

func init() {
	rootCmd.AddCommand(analyzeCmd)
}

// resultsAnalyzer is an interface responsible for analyzing benchmark results.
type resultsAnalyzer interface {
	io.Closer
	Analyze(cloud CloudDetails) error
}

type perCloudAnalyzer struct {
	newAnalyzer func(cloudName string) resultsAnalyzer
	analyzers   map[string]resultsAnalyzer
}

func newPerCloudAnalyzer(f func(cloudName string) resultsAnalyzer) resultsAnalyzer {
	return &perCloudAnalyzer{
		newAnalyzer: f,
		analyzers:   make(map[string]resultsAnalyzer),
	}
}

func (p *perCloudAnalyzer) Close() error {
	for _, a := range p.analyzers {
		if err := a.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (p *perCloudAnalyzer) Analyze(cloud CloudDetails) error {
	a, ok := p.analyzers[cloud.Cloud]

	if !ok {
		a = p.newAnalyzer(cloud.Cloud)
		p.analyzers[cloud.Cloud] = a
	}
	return a.Analyze(cloud)
}

var _ resultsAnalyzer = &perCloudAnalyzer{}

// lat represents fio total latencies.
// Values in nanoseconds.
type lat struct {
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
	Dev  float64 `json:"stddev"`
}

// clat represents completion latencies.
type clat struct {
	lat
	Percentiles map[string]int64 `json:"percentile"`
}

type ioStats struct {
	TotalIOS  int64 `json:"total_ios"` // Total # Of IOs
	IOBytes   int64 `json:"io_bytes"`  // Total size of IO
	RuntimeMS int64 `json:"runtime"`   // Duration (msec)
	Lat       lat   `json:"lat_ns"`    // IO latencies
	Clat      clat  `json:"clat_ns"`   // IO completion latencies. includes percentiles
}

func ioStatsCSV(s *ioStats) []string {
	secs := float64(s.RuntimeMS) / 1000
	rate := func(v int64) float64 {
		if v > 0 {
			return float64(v) / secs
		}
		return 0
	}

	fields := []string{
		// Number and rate of IO operations.
		fmt.Sprintf("%d", s.TotalIOS),
		fmt.Sprintf("%.3f", rate(s.TotalIOS)), // IOP/sec
		// Total amount of data read or written + Bandwidth in KiB/s
		fmt.Sprintf("%d", s.IOBytes),
		fmt.Sprintf("%f", rate(s.IOBytes)/1024), // Bandwidth: KiB/s
		// Total Latency
		fmt.Sprintf("%f", s.Lat.Min),
		fmt.Sprintf("%f", s.Lat.Max),
		fmt.Sprintf("%f", s.Lat.Mean),
		fmt.Sprintf("%f", s.Lat.Dev),
	}

	// Add completion latency percentiles.
	for _, pct := range []string{"90.000000", "95.000000", "99.000000", "99.900000", "99.990000"} {
		fields = append(fields, fmt.Sprintf("%d", s.Clat.Percentiles[pct]))
	}
	return fields
}

type fioJob struct {
	Name         string             `json:"jobname"`
	Opts         map[string]string  `json:"job options"`
	ReadStats    ioStats            `json:"read"`
	WriteStats   ioStats            `json:"write"`
	LatNS        map[string]float64 `json:"latency_ns"`
	LatUS        map[string]float64 `json:"latency_us"`
	LatMS        map[string]float64 `json:"latency_ms"`
	LatDepth     float64            `json:"latency_depth"`
	LatTargetUS  int64              `json:"latency_target"`
	LatTargetPct float64            `json:"latency_percentile"`
	LatWindowUS  int64              `json:"latency_window"`
}

type fioResults struct {
	Timestamp   int64    `json:"timestamp"`
	Jobs        []fioJob `json:"jobs"`
	modtime     time.Time
	machinetype string
	disktype    string
}

const fioResultsCSVHeader = `Cloud,Group,Machine,Date,Job,BS,IoDepth,` +
	`RdIOPs,RdIOP/s,RdBytes,RdBW(KiB/s),RdlMin,RdlMax,RdlMean,RdlStd,Rd90,Rd95,Rd99,Rd99.9,Rd99.99,` +
	`WrIOPs,WrIOP/s,WrBytes,WrBW(KiB/s),WrlMin,WrlMax,WrlMean,WrlStd,Wr90,Wr95,Wr99,Wr99.9,Wr99.99,` +
	`LatDepth,LatTarget,LatTargetPct,LatWindow`

func (r *fioResults) CSV(cloud string, wr io.Writer) {
	iodepth := func(o map[string]string) string {
		if d, ok := o["iodepth"]; ok {
			return d
		}
		return "1"
	}

	for _, j := range r.Jobs {
		fields := []string{
			cloud,
			r.disktype,
			r.machinetype,
			time.Unix(r.Timestamp, 0).String(),
			j.Name,
			j.Opts["bs"],
			iodepth(j.Opts),
		}
		fields = append(fields, ioStatsCSV(&j.ReadStats)...)
		fields = append(fields, ioStatsCSV(&j.WriteStats)...)
		fields = append(fields, []string{
			fmt.Sprintf("%.2f", j.LatDepth),
			fmt.Sprintf("%d", j.LatTargetUS),
			fmt.Sprintf("%.2f", j.LatTargetPct),
			fmt.Sprintf("%d", j.LatWindowUS),
		}...)
		fmt.Fprintf(wr, "%s\n", strings.Join(fields, ","))
	}
}

func (f *fioAnalyzer) analyzeFIO(cloud CloudDetails, machineType string) error {
	// Find successful FIO runs (those that have success file)
	glob := path.Join(cloud.LogDir(), FormatMachineType(machineType), "fio-results.*/success")
	goodRuns, err := filepath.Glob(glob)
	if err != nil {
		return err
	}

	latest := &fioResults{
		machinetype: machineType,
		disktype:    cloud.Group,
	}

	for _, r := range goodRuns {
		// Read fio-results
		info, err := os.Stat(r)
		if err != nil {
			return err
		}

		log.Printf("Analyzing %s", r)
		if latest.modtime.After(info.ModTime()) {
			log.Printf("--Skipping coremark log %q (already analyzed newer)", r)
			continue
		}

		latest.modtime = info.ModTime()
		resultsPath := path.Join(filepath.Dir(r), "fio-results.json")
		data, err := ioutil.ReadFile(resultsPath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &latest); err != nil {
			fmt.Printf("Error unmarshalling %s: %v", resultsPath, err)
		}
	}

	f.results[fmt.Sprintf("%s-%s", machineType, cloud.Group)] = latest
	return nil
}

type analyzeFn func(c CloudDetails, machineType string) error

func forEachMachine(cloud CloudDetails, fn analyzeFn) error {
	for machineType := range cloud.MachineTypes {
		if err := fn(cloud, machineType); err != nil {
			return err
		}
	}
	return nil
}

type fioAnalyzer struct {
	cloud   string
	results map[string]*fioResults
}

var _ resultsAnalyzer = &fioAnalyzer{}

func newFioAnalyzer(cloud string) resultsAnalyzer {
	return &fioAnalyzer{
		cloud:   cloud,
		results: make(map[string]*fioResults)}
}

func (f *fioAnalyzer) Close() error {
	wr, err := os.OpenFile(ResultsFile("fio.csv", f.cloud), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer wr.Close()

	fmt.Fprintf(wr, "%s\n", fioResultsCSVHeader)
	for _, res := range f.results {
		res.CSV(f.cloud, wr)
	}
	return nil
}

func ResultsFile(fname string, subdirs ...string) string {
	pieces := append([]string{baseOutputDir, reportVersion, "results"}, subdirs...)
	p := path.Join(pieces...)
	if err := makeAllDirs(p); err != nil {
		panic(err)
	}
	return filepath.Join(p, fname)
}

func (f *fioAnalyzer) Analyze(cloud CloudDetails) error {
	return forEachMachine(cloud, func(details CloudDetails, machineType string) error {
		return f.analyzeFIO(details, machineType)
	})
}

//
// CPU Analysis
//
const cpuCSVHeader = "Cloud,Date,MachineType,Cores,Single,Multi,Multi/vCPU"

type coremarkResult struct {
	cores   int64
	single  float64
	multi   float64
	modtime time.Time
}

type coremarkAnalyzer struct {
	machineResults map[string]*coremarkResult
	cloud          string
}

var _ resultsAnalyzer = &coremarkAnalyzer{}

func newCoremarkAnalyzer(cloud string) resultsAnalyzer {
	return &coremarkAnalyzer{
		cloud:          cloud,
		machineResults: make(map[string]*coremarkResult),
	}
}

func parseCoremarkLog(p string) (int64, float64, error) {
	// Extract the last line for the coremark output, and emit itersations/sec as well
	// as (optional) number of cores that were used when running this benchmark.
	cmd := exec.Command("sh", "-c",
		fmt.Sprintf("tail  -1 -q %s |cut -d/ -f1,4 | cut -d: -f2", p))
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}
	pieces := strings.Split(string(out), "/")
	if len(pieces) == 0 || len(pieces) > 2 {
		return 0, 0, fmt.Errorf("expected up to 2 fields, found 0 in %q", p)
	}

	iters, err := strconv.ParseFloat(strings.TrimSpace(pieces[0]), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("error parsing %q in %s: %v", pieces[0], p, err)
	}
	var cores int64 = 1
	if len(pieces) == 2 {
		c, err := strconv.ParseInt(strings.TrimSpace(pieces[1]), 10, 32)
		if err != nil {
			return 0, 0, fmt.Errorf("error parsing %q in %s: %v", pieces[1], p, err)
		}
		cores = c
	}
	return cores, iters, nil
}

func (c *coremarkAnalyzer) analyzeCPU(cloud CloudDetails, machineType string) error {
	// Find successful Coremark runs (those that have success file)
	glob := path.Join(cloud.LogDir(), FormatMachineType(machineType), "coremark-results.*/success")
	goodRuns, err := filepath.Glob(glob)
	if err != nil {
		return err
	}

	parseLogs := func(glob string) (int64, float64, error) {
		runs, err := filepath.Glob(glob)
		if err != nil {
			return 0, 0, err
		}

		var cores int64
		var totalIters float64
		for _, run := range runs {
			nc, iters, err := parseCoremarkLog(run)
			if err != nil {
				return 0, 0, err
			}
			if cores == 0 {
				cores = nc
			} else if cores != nc {
				return 0, 0, fmt.Errorf("expected same number of cores (%d), found %d in %q", cores, nc, run)
			}
			totalIters += iters
		}
		return cores, totalIters / float64(len(runs)), nil
	}

	for _, r := range goodRuns {
		// Read coremark-results
		log.Printf("Analyzing %s", r)
		info, err := os.Stat(r)
		if err != nil {
			return err
		}

		if res, ok := c.machineResults[machineType]; ok && res.modtime.After(info.ModTime()) {
			log.Printf("Skipping coremark log %q (already analyzed newer)", r)
			continue
		}

		_, single, err := parseLogs(path.Join(filepath.Dir(r), "single-*.log"))
		if err != nil {
			return err
		}
		cores, multi, err := parseLogs(path.Join(filepath.Dir(r), "multi-*.log"))
		if err != nil {
			return err
		}
		c.machineResults[machineType] = &coremarkResult{
			cores:   cores,
			single:  single,
			multi:   multi,
			modtime: info.ModTime(),
		}
	}
	return nil
}

func (c *coremarkAnalyzer) Analyze(cloud CloudDetails) error {
	if cloud.Cloud != c.cloud {
		return fmt.Errorf("expected %s cloud, got %s", c.cloud, cloud.Cloud)
	}

	return forEachMachine(cloud, func(details CloudDetails, machineType string) error {
		return c.analyzeCPU(details, machineType)
	})
}

func (c *coremarkAnalyzer) Close() (err error) {
	f, err := os.OpenFile(ResultsFile("cpu.csv", c.cloud), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer func() { err = f.Close() }()

	fmt.Fprintf(f, "%s\n", cpuCSVHeader)
	for machineType, res := range c.machineResults {
		fields := []string{
			c.cloud,
			res.modtime.String(),
			machineType,
			fmt.Sprintf("%d", res.cores),
			fmt.Sprintf("%f", res.single),
			fmt.Sprintf("%f", res.multi),
			fmt.Sprintf("%f", res.multi/float64(res.cores)),
		}
		fmt.Fprintf(f, "%s\n", strings.Join(fields, ","))
	}
	return nil
}

const netCSVHeader = "Cloud,Date,MachineType,Throughput,ThroughputUnits,minLat," +
	"meanLat,p90Lat,p99Lat,maxLat,latStdDev,txnRate"

type networkResult struct {
	throughput      float64
	throughputUnits string
	minLat          float64
	meanLat         float64
	p90Lat          float64
	p99Lat          float64
	maxLat          float64
	latStdDev       float64
	txnRate         float64
	modtime         time.Time
}

type netAnalyzer struct {
	machineResults map[string]*networkResult
	cloud          string
}

func newNetAnalyzer(cloud string) resultsAnalyzer {
	return &netAnalyzer{
		cloud:          cloud,
		machineResults: make(map[string]*networkResult),
	}
}

func (n *netAnalyzer) Close() (err error) {
	f, err := os.OpenFile(ResultsFile("net.csv", n.cloud), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() { err = f.Close() }()

	fmt.Fprintf(f, "%s\n", netCSVHeader)
	for machineType, res := range n.machineResults {
		fields := []string{
			n.cloud,
			res.modtime.String(),
			machineType,
			fmt.Sprintf("%f", res.throughput),
			res.throughputUnits,
			fmt.Sprintf("%f", res.minLat),
			fmt.Sprintf("%f", res.meanLat),
			fmt.Sprintf("%f", res.p90Lat),
			fmt.Sprintf("%f", res.p99Lat),
			fmt.Sprintf("%f", res.maxLat),
			fmt.Sprintf("%f", res.latStdDev),
			fmt.Sprintf("%f", res.txnRate),
		}
		fmt.Fprintf(f, "%s\n", strings.Join(fields, ","))
	}
	return nil
}

func (n *netAnalyzer) analyzeNetwork(cloud CloudDetails, machineType string) error {
	glob := path.Join(cloud.LogDir(), FormatMachineType(machineType), "netperf-results.*/success")
	goodRuns, err := filepath.Glob(glob)
	if err != nil {
		return err
	}

	for _, r := range goodRuns {
		// Read the netperf-results
		log.Printf("Analyzing %s", r)
		info, err := os.Stat(r)
		if err != nil {
			return err
		}
		if res, ok := n.machineResults[machineType]; ok && res.modtime.After(info.ModTime()) {
			log.Printf("Skipping network throughput log %q (already analyzed newer", r)
			continue
		}
		runs, err := filepath.Glob(path.Join(filepath.Dir(r), "netperf-result*"))
		if err != nil {
			return err
		}
		if len(runs) != 1 {
			return fmt.Errorf("unexpected number of netperf runs found. expected 1, found %d", len(runs))
		}
		run := runs[0]
		res := &networkResult{
			modtime: info.ModTime(),
		}
		err = parseNetperfThroughput(run, res)
		if err != nil {
			return err
		}
		err = parseNetperfLatency(run, res)
		if err != nil {
			return err
		}
		// Do the same for latency below
		n.machineResults[machineType] = res
	}
	return nil
}

func (n *netAnalyzer) Analyze(cloud CloudDetails) error {
	// Sanity check.
	if cloud.Cloud != n.cloud {
		return fmt.Errorf("expected %s cloud, got %s", n.cloud, cloud.Cloud)
	}

	return forEachMachine(cloud, func(details CloudDetails, machineType string) error {
		return n.analyzeNetwork(details, machineType)
	})
}

func parseNetperfThroughput(p string, res *networkResult) error {
	// First, extract the last line of the netperf log output and emit the
	// throughput and the unit, which are the 4th and 5th entry.
	cmd := exec.Command("sh", "-c",
		fmt.Sprintf("tail -1 %s | tr -s ' ' | cut -d ' ' -f4,5", p))
	out, err := cmd.Output()
	pieces := strings.Split(string(out), " ")
	if len(pieces) != 2 {
		return fmt.Errorf("unexpected number of fields found. expected 2, found: %d", len(pieces))
	}

	res.throughput, err = strconv.ParseFloat(pieces[0], 64)
	if err != nil {
		return errors.Wrapf(err, "error parsing %q in %s", pieces[0], p)
	}
	res.throughputUnits = pieces[1]
	// trim the new line
	res.throughputUnits = res.throughputUnits[:len(res.throughputUnits)-1]
	return nil
}

func parseNetperfLatency(p string, res *networkResult) error {
	// First, extract the last line of the netperf log output and emit the
	// throughput and the unit, which are the 4th and 5th entry.
	cmd := exec.Command("sh", "-c",
		fmt.Sprintf(" tail -7 %s | head -n 1 | tr -s ' ' | cut -d ' ' -f1-7", p))
	out, err := cmd.Output()
	pieces := strings.Split(string(out), " ")

	if len(pieces) != 7 {
		return fmt.Errorf("unexpected number of fields found. expected 7, found: %d", len(pieces))
	}

	res.minLat, err = strconv.ParseFloat(pieces[0], 64)
	if err != nil {
		return errors.Wrapf(err, "error parsing %q in %s", pieces[0], p)
	}
	res.meanLat, err = strconv.ParseFloat(pieces[1], 64)
	if err != nil {
		return errors.Wrapf(err, "error parsing %q in %s", pieces[1], p)
	}
	res.p90Lat, err = strconv.ParseFloat(pieces[2], 64)
	if err != nil {
		return errors.Wrapf(err, "error parsing %q in %s", pieces[2], p)
	}
	res.p99Lat, err = strconv.ParseFloat(pieces[3], 64)
	if err != nil {
		return errors.Wrapf(err, "error parsing %q in %s", pieces[3], p)
	}
	res.maxLat, err = strconv.ParseFloat(pieces[4], 64)
	if err != nil {
		return errors.Wrapf(err, "error parsing %q in %s", pieces[4], p)
	}
	res.latStdDev, err = strconv.ParseFloat(pieces[5], 64)
	if err != nil {
		return errors.Wrapf(err, "error parsing %q in %s", pieces[5], p)
	}
	// Last entry has an extra new line
	res.txnRate, err = strconv.ParseFloat(pieces[6][:len(pieces[6])-1], 64)
	if err != nil {
		return errors.Wrapf(err, "error parsing %q in %s", pieces[6], p)
	}
	return nil
}

var _ resultsAnalyzer = &netAnalyzer{}

type tpccRun struct {
	tpmC, efc, avg, p50, p90, p95, p99, pMax float64
	warehouses                               int64
}

func (r *tpccRun) pass() bool {
	// TPCC run passes if efficiency exceeds 85% and p95 < 10s
	return r.efc > 85.0 && r.p95 < 10000
}

type tpccResult struct {
	runs              []*tpccRun
	modtime           time.Time
	machine, disktype string
}

type tpccAnalyzer struct {
	machineResults map[string]*tpccResult
	cloud          string
}

func newTPCCAnalyzer(cloud string) resultsAnalyzer {
	return &tpccAnalyzer{
		cloud:          cloud,
		machineResults: make(map[string]*tpccResult),
	}
}

const tpccCSVHeader = "Cloud,Group,Date,MachineType,Warehouses,Pass,TpmC,Efc,Avg,P50,P90,P95,P99,PMax"

func (t *tpccAnalyzer) Close() error {
	f, err := os.OpenFile(ResultsFile("tpcc.csv", t.cloud), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() { err = f.Close() }()

	fmt.Fprintf(f, "%s\n", tpccCSVHeader)
	for _, res := range t.machineResults {
		for _, run := range res.runs {
			fields := []string{
				t.cloud,
				res.disktype,
				res.modtime.String(),
				res.machine,
				fmt.Sprintf("%d", run.warehouses),
				fmt.Sprintf("%t", run.pass()),
				fmt.Sprintf("%f", run.tpmC),
				fmt.Sprintf("%f", run.efc),
				fmt.Sprintf("%f", run.avg),
				fmt.Sprintf("%f", run.p50),
				fmt.Sprintf("%f", run.p90),
				fmt.Sprintf("%f", run.p95),
				fmt.Sprintf("%f", run.p99),
				fmt.Sprintf("%f", run.pMax),
			}
			fmt.Fprintf(f, "%s\n", strings.Join(fields, ","))
		}
	}

	return nil
}

func parseTPCCRun(p string) (*tpccRun, error) {
	// First line:
	// Initializing XXX connections.
	// We use (by default) 2 connections per warehouse.
	cmd := exec.Command("head", "-1", p)
	out, err := cmd.Output()
	run := &tpccRun{}
	run.warehouses, err = strconv.ParseInt(strings.Fields(string(out))[1], 10, 64)
	if err != nil {
		return nil, err
	}
	run.warehouses /= 2

	// _elapsed_______tpmC____efc__avg(ms)__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)
	//  900.0s    30733.3  95.6%    180.8    167.8    369.1    419.4    570.4   1677.7
	cmd = exec.Command("tail", "-1", p)
	out, err = cmd.Output()
	pieces := strings.Fields(string(out))

	if len(pieces) != 9 {
		return nil, fmt.Errorf("unexpected number of fields found. expected 7, found: %d: %s", len(pieces), out)
	}

	run.tpmC, err = strconv.ParseFloat(pieces[1], 64)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing %q in %s", pieces[1], p)
	}
	// Strip '%'
	run.efc, err = strconv.ParseFloat(pieces[2][:len(pieces[2])-1], 64)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing %q in %s", pieces[2], p)
	}
	run.avg, err = strconv.ParseFloat(pieces[3], 64)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing %q in %s", pieces[3], p)
	}
	run.p50, err = strconv.ParseFloat(pieces[4], 64)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing %q in %s", pieces[4], p)
	}
	run.p90, err = strconv.ParseFloat(pieces[5], 64)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing %q in %s", pieces[5], p)
	}
	run.p95, err = strconv.ParseFloat(pieces[6], 64)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing %q in %s", pieces[6], p)
	}
	run.p99, err = strconv.ParseFloat(pieces[7], 64)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing %q in %s", pieces[7], p)
	}
	// Last entry has an extra new line
	run.pMax, err = strconv.ParseFloat(pieces[8][:len(pieces[8])-1], 64)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing %q in %s", pieces[8], p)
	}
	return run, nil
}

func (t *tpccAnalyzer) analyzeTPCC(cloud CloudDetails, machineType string) error {
	glob := path.Join(cloud.LogDir(), FormatMachineType(machineType), "tpcc-results.*/success")
	goodRuns, err := filepath.Glob(glob)
	if err != nil {
		return err
	}

	for _, r := range goodRuns {
		// Read the tpcc-results
		log.Printf("Analyzing %s", r)
		info, err := os.Stat(r)
		if err != nil {
			return err
		}
		machineKey := fmt.Sprintf("%s-%s", cloud.Group, machineType)
		if res, ok := t.machineResults[machineKey]; ok && res.modtime.After(info.ModTime()) {
			log.Printf("Skipping TPC-C throughput log %q (already analyzed newer", r)
			continue
		}
		resultsFiles, err := filepath.Glob(path.Join(filepath.Dir(r), "tpcc-result*.txt"))
		if err != nil {
			return err
		}
		res := &tpccResult{
			modtime:  info.ModTime(),
			disktype: cloud.Group,
			machine:  machineType,
		}
		t.machineResults[machineKey] = res
		for _, f := range resultsFiles {
			run, err := parseTPCCRun(f)
			if err != nil {
				return err
			}
			res.runs = append(res.runs, run)
		}

	}
	return nil
}

func (t *tpccAnalyzer) Analyze(cloud CloudDetails) error {
	// Sanity check.
	if cloud.Cloud != t.cloud {
		return fmt.Errorf("expected %s cloud, got %s", t.cloud, cloud.Cloud)
	}

	return forEachMachine(cloud, func(details CloudDetails, machineType string) error {
		return t.analyzeTPCC(details, machineType)
	})
}

var _ resultsAnalyzer = &tpccAnalyzer{}

func analyzeResults() error {
	cpu := newPerCloudAnalyzer(newCoremarkAnalyzer)
	defer cpu.Close()

	net := newPerCloudAnalyzer(newNetAnalyzer)
	defer net.Close()

	fio := newPerCloudAnalyzer(newFioAnalyzer)
	defer fio.Close()

	tpcc := newPerCloudAnalyzer(newTPCCAnalyzer)
	defer tpcc.Close()

	// Generate scripts.
	for _, cloudDetail := range clouds {
		if err := cpu.Analyze(cloudDetail); err != nil {
			return err
		}
		if err := net.Analyze(cloudDetail); err != nil {
			return err
		}
		if err := fio.Analyze(cloudDetail); err != nil {
			return err
		}
		if err := tpcc.Analyze(cloudDetail); err != nil {
			return err
		}
	}
	return nil
}
