package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

var (
	bigqueryProjectID = kingpin.Flag("bigquery-project-id", "Google Cloud project id that contains the BigQuery dataset").Envar("BQ_PROJECT_ID").Required().String()
	bigqueryDataset   = kingpin.Flag("bigquery-dataset", "Name of the BigQuery dataset").Envar("BQ_DATASET").Required().String()
	bigqueryTable     = kingpin.Flag("bigquery-table", "Name of the BigQuery table").Envar("BQ_TABLE").Required().String()
)

func main() {

	// parse command line parameters
	kingpin.Parse()

	bigqueryClient, err := NewBigQueryClient(*bigqueryProjectID)
	if err != nil {
		log.Fatal("Failed creating bigquery client", err)
	}

	fmt.Printf("Checking if table %v.%v.%v exists...", *bigqueryProjectID, *bigqueryDataset, *bigqueryTable)
	tableExist := bigqueryClient.CheckIfTableExists(*bigqueryDataset, *bigqueryTable)
	if !tableExist {
		fmt.Printf("Creating table %v.%v.%v...", *bigqueryProjectID, *bigqueryDataset, *bigqueryTable)
		err := bigqueryClient.CreateTable(*bigqueryDataset, *bigqueryTable, TimeSeriesSample{}, "Timestamp", true)
		if err != nil {
			log.Fatal("Failed creating bigquery table")
		}
	}

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for _, ts := range req.Timeseries {
			m := make(model.Metric, len(ts.Labels))
			for _, l := range ts.Labels {
				m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
			}
			fmt.Println(m)

			for _, s := range ts.Samples {
				fmt.Printf("  %f %d\n", s.Value, s.Timestamp)
			}
		}

		fmt.Printf("Inserting measurements into table %v.%v.%v...", *bigqueryProjectID, *bigqueryDataset, *bigqueryTable)
		err = bigqueryClient.InsertTimeSeries(*bigqueryDataset, *bigqueryTable, convert(req.Timeseries))
		if err != nil {
			log.Fatal("Failed inserting measurements into bigquery table", err)
		}
	})

	log.Fatal(http.ListenAndServe(":1234", nil))
}

func convert(timeseries []*prompb.TimeSeries) []TimeSeriesSample {
	tss := make([]TimeSeriesSample, 0)

	for _, ts := range timeseries {
		convertedLabels := make([]Label, 0)

		tsName := ""
		for _, l := range ts.Labels {
			convertedLabels = append(convertedLabels, Label{
				Name:  l.Name,
				Value: l.Value,
			})
			// get timeline series name
			if l.Name == "__name__" {
				tsName = l.Value
			}
		}

		for _, s := range ts.Samples {
			if !math.IsNaN(s.Value) && !math.IsInf(s.Value, 0) {
				tss = append(tss, TimeSeriesSample{
					Name:      tsName,
					Labels:    convertedLabels,
					Value:     s.Value,
					Timestamp: toTime(s.Timestamp),
				})
			}
		}
	}

	return tss
}

func toTime(ts int64) time.Time {
	return time.Unix(ts/1000, (ts%1000)*int64(time.Millisecond))
}
