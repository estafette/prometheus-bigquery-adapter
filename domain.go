package main

import "time"

type TimeSeriesSample struct {
	Name      string
	Labels    []Label
	Value     float64
	Timestamp time.Time
}

type Label struct {
	Name  string
	Value string
}
