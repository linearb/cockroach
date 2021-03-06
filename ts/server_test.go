// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package ts_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestHttpQuery(t *testing.T) {
	defer leaktest.AfterTest(t)
	tsrv := &server.TestServer{}
	if err := tsrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer tsrv.Stop()

	// Populate data directly.
	tsdb := tsrv.TsDB()
	if err := tsdb.StoreData(ts.Resolution10s, []ts.TimeSeriesData{
		{
			Name:   "test.metric",
			Source: "source1",
			Datapoints: []*ts.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          100.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          200.0,
				},
				{
					TimestampNanos: 520 * 1e9,
					Value:          300.0,
				},
			},
		},
		{
			Name:   "test.metric",
			Source: "source2",
			Datapoints: []*ts.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          100.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          200.0,
				},
				{
					TimestampNanos: 510 * 1e9,
					Value:          250.0,
				},
				{
					TimestampNanos: 530 * 1e9,
					Value:          350.0,
				},
			},
		},
		{
			Name: "other.metric",
			Datapoints: []*ts.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          100.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          200.0,
				},
				{
					TimestampNanos: 510 * 1e9,
					Value:          250.0,
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	expectedResult := &ts.TimeSeriesQueryResponse{
		Results: []*ts.TimeSeriesQueryResponse_Result{
			{
				Name:    "test.metric",
				Sources: []string{"source1", "source2"},
				Datapoints: []*ts.TimeSeriesDatapoint{
					{
						TimestampNanos: 505 * 1e9,
						Value:          400.0,
					},
					{
						TimestampNanos: 515 * 1e9,
						Value:          500.0,
					},
					{
						TimestampNanos: 525 * 1e9,
						Value:          600.0,
					},
				},
			},
			{
				Name:    "other.metric",
				Sources: []string{""},
				Datapoints: []*ts.TimeSeriesDatapoint{
					{
						TimestampNanos: 505 * 1e9,
						Value:          200.0,
					},
					{
						TimestampNanos: 515 * 1e9,
						Value:          250.0,
					},
				},
			},
		},
	}

	response := &ts.TimeSeriesQueryResponse{}
	session := testutils.NewTestHTTPSession(t, &base.Context{}, tsrv.ServingAddr())
	session.PostProto(ts.URLQuery, &ts.TimeSeriesQueryRequest{
		StartNanos: 500 * 1e9,
		EndNanos:   526 * 1e9,
		Queries: []ts.TimeSeriesQueryRequest_Query{
			{
				Name: "test.metric",
			},
			{
				Name: "other.metric",
			},
		},
	}, response)
	for _, r := range response.Results {
		sort.Strings(r.Sources)
	}
	if !reflect.DeepEqual(response, expectedResult) {
		t.Fatalf("actual response %v did not match expected response %v",
			response, expectedResult)
	}
}
