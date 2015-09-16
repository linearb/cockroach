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
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
)

// distinct constructs a distinctNode.
func (*planner) distinct(n *parser.Select, s *scanNode) (*distinctNode, error) {
	// Save away columns before calling maybeAddRender() below that can add more
	// columns.
	distinct := distinctNode{columns: s.Columns()}
	if n.Distinct != nil {
		distinct.seen = make(map[string]struct{})
		distinct.on = make([]int, 0, len(n.Distinct))
		for _, expr := range n.Distinct {
			index, err := s.maybeAddRender(expr)
			if err != nil {
				return nil, err
			}
			distinct.on = append(distinct.on, index)
		}
	}
	return &distinct, nil
}

type distinctNode struct {
	planNode
	// This map is surely going to result in an out of memory.
	// TODO(vivek): Don't use map when the results are sorted.
	seen map[string]struct{} // keep track of rows to eliminate duplicates
	// Index into Distinct ON evaluated expressions.
	on      []int // expressions that identify duplicates.
	columns []string
}

// wrap the supplied planNode with the distinctNode if distinct is required.
func (n *distinctNode) wrap(plan planNode) planNode {
	if n.seen == nil {
		return plan
	}
	n.planNode = plan
	// Notify sort node to not present the results.
	sNode, ok := plan.(*sortNode)
	if ok {
		sNode.dontPresent = true
		// Use the columns from sort because those were the original
		// columns requested.
		n.columns = sNode.Columns()
	}
	return n
}

func (n *distinctNode) Next() bool {
	for n.planNode.Next() {
		// Detect duplicates
		var v parser.DTuple
		if len(n.on) == 0 {
			v = n.Values()
		} else {
			vals := n.planNode.Values()
			for _, index := range n.on {
				v = append(v, vals[index-1])
			}
		}
		encoded := encodeValues(v)
		key := string(encoded)
		if _, ok := n.seen[key]; ok {
			// duplicate
			continue
		}
		n.seen[key] = struct{}{}
		return true
	}
	return false
}

func encodeValues(values parser.DTuple) []byte {
	b := make([]byte, 0, 100)
	for _, val := range values {
		var err error
		b, err = encodeTableKey(b, val)
		if err != nil {
			panic(fmt.Sprintf("unable to encode value: %v", val))
		}
	}
	return b
}

func (n *distinctNode) Values() parser.DTuple {
	// If DISTINCT ON or ORDER BY expression was used the number of columns in each
	// row might differ from the number of columns requested, so trim the result.
	v := n.planNode.Values()
	return v[:len(n.columns)]
}

func (n *distinctNode) Columns() []string {
	return n.columns
}

func (n *distinctNode) ExplainPlan() (string, string, []planNode) {
	return "distinct", "", []planNode{n.planNode}
}
