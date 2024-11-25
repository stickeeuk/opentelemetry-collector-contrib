// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetricutil_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector/internal/pmetricutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector/internal/pmetricutiltest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
)

func TestMoveResourcesIf(t *testing.T) {
	testCases := []struct {
		name       string
		moveIf     func(pmetric.ResourceMetrics) bool
		from       pmetric.Metrics
		to         pmetric.Metrics
		expectFrom pmetric.Metrics
		expectTo   pmetric.Metrics
	}{
		{
			name: "move_none",
			moveIf: func(pmetric.ResourceMetrics) bool {
				return false
			},
			from:       pmetricutiltest.NewMetrics("AB", "CD", "EF", "FG"),
			to:         pmetric.NewMetrics(),
			expectFrom: pmetricutiltest.NewMetrics("AB", "CD", "EF", "FG"),
			expectTo:   pmetric.NewMetrics(),
		},
		{
			name: "move_all",
			moveIf: func(pmetric.ResourceMetrics) bool {
				return true
			},
			from:       pmetricutiltest.NewMetrics("AB", "CD", "EF", "FG"),
			to:         pmetric.NewMetrics(),
			expectFrom: pmetric.NewMetrics(),
			expectTo:   pmetricutiltest.NewMetrics("AB", "CD", "EF", "FG"),
		},
		{
			name: "move_one",
			moveIf: func(rl pmetric.ResourceMetrics) bool {
				rname, ok := rl.Resource().Attributes().Get("resourceName")
				return ok && rname.AsString() == "resourceA"
			},
			from:       pmetricutiltest.NewMetrics("AB", "CD", "EF", "FG"),
			to:         pmetric.NewMetrics(),
			expectFrom: pmetricutiltest.NewMetrics("B", "CD", "EF", "FG"),
			expectTo:   pmetricutiltest.NewMetrics("A", "CD", "EF", "FG"),
		},
		{
			name: "move_to_preexisting",
			moveIf: func(rl pmetric.ResourceMetrics) bool {
				rname, ok := rl.Resource().Attributes().Get("resourceName")
				return ok && rname.AsString() == "resourceB"
			},
			from:       pmetricutiltest.NewMetrics("AB", "CD", "EF", "FG"),
			to:         pmetricutiltest.NewMetrics("1", "2", "3", "4"),
			expectFrom: pmetricutiltest.NewMetrics("A", "CD", "EF", "FG"),
			expectTo: func() pmetric.Metrics {
				move := pmetricutiltest.NewMetrics("B", "CD", "EF", "FG")
				moveTo := pmetricutiltest.NewMetrics("1", "2", "3", "4")
				move.ResourceMetrics().MoveAndAppendTo(moveTo.ResourceMetrics())
				return moveTo
			}(),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			pmetricutil.MoveResourcesIf(tt.from, tt.to, tt.moveIf)
			assert.NoError(t, pmetrictest.CompareMetrics(tt.expectFrom, tt.from), "from not modified as expected")
			assert.NoError(t, pmetrictest.CompareMetrics(tt.expectTo, tt.to), "to not as expected")
		})
	}
}

func TestMoveMetricsWithContextIf(t *testing.T) {
	testCases := []struct {
		name       string
		moveIf     func(pmetric.ResourceMetrics, pmetric.ScopeMetrics, pmetric.Metric) bool
		from       pmetric.Metrics
		to         pmetric.Metrics
		expectFrom pmetric.Metrics
		expectTo   pmetric.Metrics
	}{
		{
			name: "move_none",
			moveIf: func(_ pmetric.ResourceMetrics, _ pmetric.ScopeMetrics, _ pmetric.Metric) bool {
				return false
			},
			from:       pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
			to:         pmetric.NewMetrics(),
			expectFrom: pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
			expectTo:   pmetric.NewMetrics(),
		},
		{
			name: "move_all",
			moveIf: func(_ pmetric.ResourceMetrics, _ pmetric.ScopeMetrics, _ pmetric.Metric) bool {
				return true
			},
			from:       pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
			to:         pmetric.NewMetrics(),
			expectFrom: pmetric.NewMetrics(),
			expectTo:   pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
		},
		{
			name: "move_all_from_one_resource",
			moveIf: func(rl pmetric.ResourceMetrics, _ pmetric.ScopeMetrics, _ pmetric.Metric) bool {
				rname, ok := rl.Resource().Attributes().Get("resourceName")
				return ok && rname.AsString() == "resourceB"
			},
			from:       pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
			to:         pmetric.NewMetrics(),
			expectFrom: pmetricutiltest.NewMetrics("A", "CD", "EF", "GH"),
			expectTo:   pmetricutiltest.NewMetrics("B", "CD", "EF", "GH"),
		},
		{
			name: "move_all_from_one_scope",
			moveIf: func(rl pmetric.ResourceMetrics, sl pmetric.ScopeMetrics, _ pmetric.Metric) bool {
				rname, ok := rl.Resource().Attributes().Get("resourceName")
				return ok && rname.AsString() == "resourceB" && sl.Scope().Name() == "scopeC"
			},
			from: pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
			to:   pmetric.NewMetrics(),
			expectFrom: pmetricutiltest.NewMetricsFromOpts(
				pmetricutiltest.Resource("A",
					pmetricutiltest.Scope("C",
						pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
						pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
					pmetricutiltest.Scope("D",
						pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
						pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
				),
				pmetricutiltest.Resource("B",
					pmetricutiltest.Scope("D",
						pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
						pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
				),
			),
			expectTo: pmetricutiltest.NewMetrics("B", "C", "EF", "GH"),
		},
		{
			name: "move_all_from_one_scope_in_each_resource",
			moveIf: func(_ pmetric.ResourceMetrics, sl pmetric.ScopeMetrics, _ pmetric.Metric) bool {
				return sl.Scope().Name() == "scopeD"
			},
			from:       pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
			to:         pmetric.NewMetrics(),
			expectFrom: pmetricutiltest.NewMetrics("AB", "C", "EF", "GH"),
			expectTo:   pmetricutiltest.NewMetrics("AB", "D", "EF", "GH"),
		},
		{
			name: "move_one",
			moveIf: func(rl pmetric.ResourceMetrics, sl pmetric.ScopeMetrics, m pmetric.Metric) bool {
				rname, ok := rl.Resource().Attributes().Get("resourceName")
				return ok && rname.AsString() == "resourceA" && sl.Scope().Name() == "scopeD" && m.Name() == "metricF"
			},
			from: pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
			to:   pmetric.NewMetrics(),
			expectFrom: pmetricutiltest.NewMetricsFromOpts(
				pmetricutiltest.Resource("A",
					pmetricutiltest.Scope("C",
						pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
						pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
					pmetricutiltest.Scope("D",
						pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
				),
				pmetricutiltest.Resource("B",
					pmetricutiltest.Scope("C",
						pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
						pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
					pmetricutiltest.Scope("D",
						pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
						pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
				),
			),
			expectTo: pmetricutiltest.NewMetrics("A", "D", "F", "GH"),
		},
		{
			name: "move_one_from_each_scope",
			moveIf: func(_ pmetric.ResourceMetrics, _ pmetric.ScopeMetrics, m pmetric.Metric) bool {
				return m.Name() == "metricE"
			},
			from:       pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
			to:         pmetric.NewMetrics(),
			expectFrom: pmetricutiltest.NewMetrics("AB", "CD", "F", "GH"),
			expectTo:   pmetricutiltest.NewMetrics("AB", "CD", "E", "GH"),
		},
		{
			name: "move_one_from_each_scope_in_one_resource",
			moveIf: func(rl pmetric.ResourceMetrics, _ pmetric.ScopeMetrics, m pmetric.Metric) bool {
				rname, ok := rl.Resource().Attributes().Get("resourceName")
				return ok && rname.AsString() == "resourceB" && m.Name() == "metricE"
			},
			from: pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
			to:   pmetric.NewMetrics(),
			expectFrom: pmetricutiltest.NewMetricsFromOpts(
				pmetricutiltest.Resource("A",
					pmetricutiltest.Scope("C",
						pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
						pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
					pmetricutiltest.Scope("D",
						pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
						pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
				),
				pmetricutiltest.Resource("B",
					pmetricutiltest.Scope("C",
						pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
					pmetricutiltest.Scope("D",
						pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					),
				),
			),
			expectTo: pmetricutiltest.NewMetrics("B", "CD", "E", "GH"),
		},
		{
			name: "move_some_to_preexisting",
			moveIf: func(_ pmetric.ResourceMetrics, sl pmetric.ScopeMetrics, _ pmetric.Metric) bool {
				return sl.Scope().Name() == "scopeD"
			},
			from:       pmetricutiltest.NewMetrics("AB", "CD", "EF", "GH"),
			to:         pmetricutiltest.NewMetrics("1", "2", "3", "4"),
			expectFrom: pmetricutiltest.NewMetrics("AB", "C", "EF", "GH"),
			expectTo: pmetricutiltest.NewMetricsFromOpts(
				pmetricutiltest.Resource("1", pmetricutiltest.Scope("2",
					pmetricutiltest.Metric("3", pmetricutiltest.NumberDataPoint("4")),
				)),
				pmetricutiltest.Resource("A", pmetricutiltest.Scope("D",
					pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
				)),
				pmetricutiltest.Resource("B", pmetricutiltest.Scope("D",
					pmetricutiltest.Metric("E", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
					pmetricutiltest.Metric("F", pmetricutiltest.NumberDataPoint("G"), pmetricutiltest.NumberDataPoint("H")),
				)),
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			pmetricutil.MoveMetricsWithContextIf(tt.from, tt.to, tt.moveIf)
			assert.NoError(t, pmetrictest.CompareMetrics(tt.expectFrom, tt.from), "from not modified as expected")
			assert.NoError(t, pmetrictest.CompareMetrics(tt.expectTo, tt.to), "to not as expected")
		})
	}
}
