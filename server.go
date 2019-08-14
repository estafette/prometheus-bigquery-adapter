package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/alecthomas/kingpin"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
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

		// for _, ts := range req.Timeseries {
		// 	m := make(model.Metric, len(ts.Labels))
		// 	for _, l := range ts.Labels {
		// 		m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		// 	}
		// 	fmt.Println(m)

		// 	for _, s := range ts.Samples {
		// 		fmt.Printf("  %f %d\n", s.Value, s.Timestamp)
		// 	}
		// }

		// fmt.Printf("Inserting measurements into table %v.%v.%v...", *bigqueryProjectID, *bigqueryDataset, *bigqueryTable)
		err = bigqueryClient.InsertTimeSeries(*bigqueryDataset, *bigqueryTable, convert(req.Timeseries))
		if err != nil {
			fmt.Printf("Failed inserting measurements into bigquery table: %v", err)
		}
	})

	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			fmt.Printf("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			fmt.Printf("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var resp *prompb.ReadResponse
		resp, err = bigqueryClient.QueryTimeSeries(*bigqueryDataset, *bigqueryTable, &req)

		if err != nil {
			fmt.Printf("msg", "Error executing query", "query", req, "storage", "bigquery", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			fmt.Printf("msg", "Error writing response", "storage", "bigquery", "err", err)
		}
	})

	log.Fatal(http.ListenAndServe(":1234", nil))
}
