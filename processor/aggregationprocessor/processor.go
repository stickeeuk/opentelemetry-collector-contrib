// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/aggregationprocessor"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics/identity"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/aggregationprocessor/internal/metrics"
)

var _ processor.Metrics = (*Processor)(nil)

type Processor struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger

	stateLock sync.Mutex

	md                 pmetric.Metrics
	rmLookup           map[identity.Resource]pmetric.ResourceMetrics
	smLookup           map[identity.Scope]pmetric.ScopeMetrics
	mLookup            map[identity.Metric]pmetric.Metric
	numberLookup       map[identity.Stream]pmetric.NumberDataPoint
	histogramLookup    map[identity.Stream]pmetric.HistogramDataPoint
	expHistogramLookup map[identity.Stream]pmetric.ExponentialHistogramDataPoint
	summaryLookup      map[identity.Stream]pmetric.SummaryDataPoint

	config *Config

	nextConsumer consumer.Metrics
}

func newProcessor(config *Config, log *zap.Logger, nextConsumer consumer.Metrics) *Processor {
	ctx, cancel := context.WithCancel(context.Background())

	return &Processor{
		ctx:    ctx,
		cancel: cancel,
		logger: log,

		stateLock: sync.Mutex{},

		md:                 pmetric.NewMetrics(),
		rmLookup:           map[identity.Resource]pmetric.ResourceMetrics{},
		smLookup:           map[identity.Scope]pmetric.ScopeMetrics{},
		mLookup:            map[identity.Metric]pmetric.Metric{},
		numberLookup:       map[identity.Stream]pmetric.NumberDataPoint{},
		histogramLookup:    map[identity.Stream]pmetric.HistogramDataPoint{},
		expHistogramLookup: map[identity.Stream]pmetric.ExponentialHistogramDataPoint{},
		summaryLookup:      map[identity.Stream]pmetric.SummaryDataPoint{},

		config: config,

		nextConsumer: nextConsumer,
	}
}

func (p *Processor) Start(_ context.Context, _ component.Host) error {
	exportTicker := time.NewTicker(p.config.Interval)
	go func() {
		for {
			select {
			case <-p.ctx.Done():
				exportTicker.Stop()
				return
			case <-exportTicker.C:
				p.exportMetrics()
			}
		}
	}()

	return nil
}

func (p *Processor) Shutdown(_ context.Context) error {
	p.cancel()
	return nil
}

func (p *Processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *Processor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	var errs error

	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				switch m.Type() {
				case pmetric.MetricTypeSummary:
					return false
				case pmetric.MetricTypeGauge:
					return false
				case pmetric.MetricTypeSum:
					// Check if we care about this value
					sum := m.Sum()

					if sum.AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						return false
					}

					mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					cloneSum := mClone.Sum()

					aggregateDataPoints(sum.DataPoints(), cloneSum.DataPoints(), metricID, p.numberLookup)
					return true
				case pmetric.MetricTypeHistogram:
					histogram := m.Histogram()

					if histogram.AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						return false
					}

					mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					cloneHistogram := mClone.Histogram()

					aggregateDataPoints(histogram.DataPoints(), cloneHistogram.DataPoints(), metricID, p.histogramLookup)
					return true
				case pmetric.MetricTypeExponentialHistogram:
					expHistogram := m.ExponentialHistogram()

					if expHistogram.AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						return false
					}

					// TODO: implement ExponentialHistogram
					p.logger.Error("ExponentialHistogram aggregation not implemented")
					return false

					// mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					// cloneExpHistogram := mClone.ExponentialHistogram()

					// aggregateDataPoints(expHistogram.DataPoints(), cloneExpHistogram.DataPoints(), metricID, p.expHistogramLookup)
					// return true
				default:
					errs = errors.Join(fmt.Errorf("invalid MetricType %d", m.Type()))
					return false
				}
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	if err := p.nextConsumer.ConsumeMetrics(ctx, md); err != nil {
		errs = errors.Join(errs, err)
	}

	return errs
}

func aggregateDataPoints[DPS metrics.DataPointSlice[DP], DP metrics.DataPoint[DP]](dataPoints DPS, mCloneDataPoints DPS, metricID identity.Metric, dpLookup map[identity.Stream]DP) {
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		streamID := identity.OfStream(metricID, dp)
		existingDP, ok := dpLookup[streamID]
		if !ok {
			dpClone := mCloneDataPoints.AppendEmpty()
			dp.CopyTo(dpClone)
			dpLookup[streamID] = dpClone
			continue
		}

		// Add the value of dp to existingDP
		switch dp := any(dp).(type) {
		case pmetric.NumberDataPoint:
			existingDP := any(existingDP).(pmetric.NumberDataPoint)
			switch dp.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				existingDP.SetIntValue(existingDP.IntValue() + dp.IntValue())
			case pmetric.NumberDataPointValueTypeDouble:
				existingDP.SetDoubleValue(existingDP.DoubleValue() + dp.DoubleValue())
			}
		case pmetric.HistogramDataPoint:
			existingDP := any(existingDP).(pmetric.HistogramDataPoint)
			existingDP.SetCount(existingDP.Count() + dp.Count())
			existingDP.SetSum(existingDP.Sum() + dp.Sum())
			for j := 0; j < dp.BucketCounts().Len(); j++ {
				existingDP.BucketCounts().SetAt(j, existingDP.BucketCounts().At(j)+dp.BucketCounts().At(j))
			}
		case pmetric.ExponentialHistogramDataPoint:
			existingDP := any(existingDP).(pmetric.ExponentialHistogramDataPoint)
			existingDP.SetCount(existingDP.Count() + dp.Count())
			existingDP.SetSum(existingDP.Sum() + dp.Sum())
			for j := 0; j < dp.Positive().BucketCounts().Len(); j++ {
				existingDP.Positive().BucketCounts().SetAt(j, existingDP.Positive().BucketCounts().At(j)+dp.Positive().BucketCounts().At(j))
			}
			for j := 0; j < dp.Negative().BucketCounts().Len(); j++ {
				existingDP.Negative().BucketCounts().SetAt(j, existingDP.Negative().BucketCounts().At(j)+dp.Negative().BucketCounts().At(j))
			}
		case pmetric.SummaryDataPoint:
			existingDP := any(existingDP).(pmetric.SummaryDataPoint)
			existingDP.SetCount(existingDP.Count() + dp.Count())
			existingDP.SetSum(existingDP.Sum() + dp.Sum())
		}
	}
}

func (p *Processor) exportMetrics() {
	md := func() pmetric.Metrics {
		p.stateLock.Lock()
		defer p.stateLock.Unlock()

		// ConsumeMetrics() has prepared our own pmetric.Metrics instance ready for us to use
		// Take it and clear replace it with a new empty one
		out := p.md
		p.md = pmetric.NewMetrics()

		// Clear all the lookup references
		clear(p.rmLookup)
		clear(p.smLookup)
		clear(p.mLookup)
		clear(p.numberLookup)
		clear(p.histogramLookup)
		clear(p.expHistogramLookup)
		clear(p.summaryLookup)

		return out
	}()

	if err := p.nextConsumer.ConsumeMetrics(p.ctx, md); err != nil {
		p.logger.Error("Metrics export failed", zap.Error(err))
	}
}

func (p *Processor) getOrCloneMetric(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric) (pmetric.Metric, identity.Metric) {
	// Find the ResourceMetrics

	// Remove all attributes except for service.instance.id, service.namespace, and service.name
	rm.Resource().Attributes().RemoveIf(func(k string, _ pcommon.Value) bool {
		return k != "service.instance.id" && k != "service.namespace" && k != "service.name"
	})

	resID := identity.OfResource(rm.Resource())
	rmClone, ok := p.rmLookup[resID]
	if !ok {
		// We need to clone it *without* the ScopeMetricsSlice data
		rmClone = p.md.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(rmClone.Resource())
		rmClone.SetSchemaUrl(rm.SchemaUrl())
		p.rmLookup[resID] = rmClone
	}

	// Find the ScopeMetrics

	// Remove all attributes except for service.instance.id, service.namespace, and service.name
	sm.Scope().Attributes().RemoveIf(func(k string, _ pcommon.Value) bool {
		return k != "service.instance.id" && k != "service.namespace" && k != "service.name"
	})

	scopeID := identity.OfScope(resID, sm.Scope())
	smClone, ok := p.smLookup[scopeID]
	if !ok {
		// We need to clone it *without* the MetricSlice data
		smClone = rmClone.ScopeMetrics().AppendEmpty()
		sm.Scope().CopyTo(smClone.Scope())
		smClone.SetSchemaUrl(sm.SchemaUrl())
		p.smLookup[scopeID] = smClone
	}

	// Find the Metric
	metricID := identity.OfMetric(scopeID, m)
	mClone, ok := p.mLookup[metricID]
	if !ok {
		// We need to clone it *without* the datapoint data
		mClone = smClone.Metrics().AppendEmpty()
		mClone.SetName(m.Name())
		mClone.SetDescription(m.Description())
		mClone.SetUnit(m.Unit())

		switch m.Type() {
		case pmetric.MetricTypeGauge:
			mClone.SetEmptyGauge()
		case pmetric.MetricTypeSummary:
			mClone.SetEmptySummary()
		case pmetric.MetricTypeSum:
			src := m.Sum()

			dest := mClone.SetEmptySum()
			dest.SetAggregationTemporality(src.AggregationTemporality())
			dest.SetIsMonotonic(src.IsMonotonic())
		case pmetric.MetricTypeHistogram:
			src := m.Histogram()

			dest := mClone.SetEmptyHistogram()
			dest.SetAggregationTemporality(src.AggregationTemporality())
		case pmetric.MetricTypeExponentialHistogram:
			src := m.ExponentialHistogram()

			dest := mClone.SetEmptyExponentialHistogram()
			dest.SetAggregationTemporality(src.AggregationTemporality())
		}

		p.mLookup[metricID] = mClone
	}

	return mClone, metricID
}
