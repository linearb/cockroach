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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import "github.com/cockroachdb/cockroach/sql/parser"

// const joinBatchSize = 100

// A joinNode implements joining of two result sets. The left side of the join
// is pulled first and the resulting rows are used to lookup rows in the right
// side of the join. The work is batched: we pull joinBatchSize rows from the
// left side of the join and use those rows to construct spans that are passed
// to the right side of the join.
type joinNode struct {
	planner *planner
	left    planNode
	right   *scanNode
	filter  parser.Expr
	render  []parser.Expr
}

func (n *joinNode) Columns() []string {
	return nil
}

func (n *joinNode) Ordering() []int {
	return nil
}

func (n *joinNode) Values() parser.DTuple {
	return nil
}

func (n *joinNode) Next() bool {
	return false
}

func (n *joinNode) Err() error {
	return nil
}

func (n *joinNode) ExplainPlan() (name, description string, children []planNode) {
	return
}
