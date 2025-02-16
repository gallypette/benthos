package processor

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

type workflowDeprecated struct {
	log   log.Modular
	stats metrics.Type

	children  map[string]*ProcessMap
	dag       [][]string
	allStages map[string]struct{}
	metaPath  []string

	mCount           metrics.StatCounter
	mSent            metrics.StatCounter
	mSentParts       metrics.StatCounter
	mSkippedNoStages metrics.StatCounter
	mErr             metrics.StatCounter
	mErrJSON         metrics.StatCounter
	mErrMeta         metrics.StatCounter
	mErrOverlay      metrics.StatCounter
	mErrStages       map[string]metrics.StatCounter
	mSuccStages      map[string]metrics.StatCounter
}

func newWorkflowDeprecated(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	w := &workflowDeprecated{
		log:         log,
		stats:       stats,
		mErrStages:  map[string]metrics.StatCounter{},
		mSuccStages: map[string]metrics.StatCounter{},
		metaPath:    nil,
		allStages:   map[string]struct{}{},
	}
	if len(conf.Workflow.MetaPath) > 0 {
		w.metaPath = gabs.DotPathToSlice(conf.Workflow.MetaPath)
	}

	explicitDeps := map[string][]string{}
	w.children = map[string]*ProcessMap{}

	for k, v := range conf.Workflow.Stages {
		if len(processDAGStageName.FindString(k)) != len(k) {
			return nil, fmt.Errorf("workflow stage name '%v' contains invalid characters", k)
		}

		bMgr, bLog, bStats := interop.LabelChild(k, mgr, log, stats)
		child, err := NewProcessMap(v.ProcessMapConfig, bMgr, bLog, bStats)
		if err != nil {
			return nil, fmt.Errorf("failed to create child process_map '%v': %v", k, err)
		}

		w.children[k] = child
		explicitDeps[k] = v.Dependencies
		w.allStages[k] = struct{}{}
	}

	var err error
	if w.dag, err = resolveProcessMapDAG(explicitDeps, w.children); err != nil {
		return nil, err
	}

	w.mCount = stats.GetCounter("count")
	w.mSent = stats.GetCounter("sent")
	w.mSentParts = stats.GetCounter("parts.sent")
	w.mSkippedNoStages = stats.GetCounter("skipped.no_stages")
	w.mErr = stats.GetCounter("error")
	w.mErrJSON = stats.GetCounter("error.json_parse")
	w.mErrMeta = stats.GetCounter("error.meta_set")
	w.mErrOverlay = stats.GetCounter("error.overlay")

	w.log.Infof("Resolved workflow DAG: %v\n", w.dag)
	return w, nil
}

//------------------------------------------------------------------------------

func (w *workflowDeprecated) incrStageErr(id string) {
	if ctr, exists := w.mErrStages[id]; exists {
		ctr.Incr(1)
		return
	}

	ctr := w.stats.GetCounter(fmt.Sprintf("%v.error", id))
	ctr.Incr(1)
	w.mErrStages[id] = ctr
}

func (w *workflowDeprecated) incrStageSucc(id string) {
	if ctr, exists := w.mSuccStages[id]; exists {
		ctr.Incr(1)
		return
	}

	ctr := w.stats.GetCounter(fmt.Sprintf("%v.success", id))
	ctr.Incr(1)
	w.mSuccStages[id] = ctr
}

type deprecatedResultTracker struct {
	succeeded map[string]struct{}
	skipped   map[string]struct{}
	failed    map[string]struct{}
	sync.Mutex
}

func deprecatedTrackerFromTree(tree [][]string) *deprecatedResultTracker {
	r := &deprecatedResultTracker{
		succeeded: map[string]struct{}{},
		skipped:   map[string]struct{}{},
		failed:    map[string]struct{}{},
	}
	for _, layer := range tree {
		for _, k := range layer {
			r.succeeded[k] = struct{}{}
		}
	}
	return r
}

func (r *deprecatedResultTracker) Skipped(k string) {
	r.Lock()
	delete(r.succeeded, k)

	r.skipped[k] = struct{}{}
	r.Unlock()
}

func (r *deprecatedResultTracker) Failed(k string) {
	r.Lock()
	delete(r.succeeded, k)
	delete(r.skipped, k)

	r.failed[k] = struct{}{}
	r.Unlock()
}

func (r *deprecatedResultTracker) ToSlices() (succeeded, skipped, failed []string) {
	r.Lock()

	succeeded = make([]string, 0, len(r.succeeded))
	skipped = make([]string, 0, len(r.skipped))
	failed = make([]string, 0, len(r.failed))

	for k := range r.succeeded {
		succeeded = append(succeeded, k)
	}
	sort.Strings(succeeded)
	for k := range r.skipped {
		skipped = append(skipped, k)
	}
	sort.Strings(skipped)
	for k := range r.failed {
		failed = append(failed, k)
	}
	sort.Strings(failed)

	r.Unlock()
	return
}

// Returns a map of enrichment IDs that should be skipped for this payload.
func (w *workflowDeprecated) skipFromMeta(root interface{}) map[string]struct{} {
	skipList := map[string]struct{}{}
	if len(w.metaPath) == 0 {
		return skipList
	}

	gObj := gabs.Wrap(root)

	// If a whitelist is provided for this flow then skip stages that aren't
	// within it.
	if apply, ok := gObj.S(append(w.metaPath, "apply")...).Data().([]interface{}); ok {
		if len(apply) > 0 {
			for k := range w.allStages {
				skipList[k] = struct{}{}
			}
			for _, id := range apply {
				if idStr, isString := id.(string); isString {
					delete(skipList, idStr)
				}
			}
		}
	}

	// Skip stages that already succeeded in a previous run of this workflow.
	if succeeded, ok := gObj.S(append(w.metaPath, "succeeded")...).Data().([]interface{}); ok {
		for _, id := range succeeded {
			if idStr, isString := id.(string); isString {
				if _, exists := w.allStages[idStr]; exists {
					skipList[idStr] = struct{}{}
				}
			}
		}
	}

	// Skip stages that were already skipped in a previous run of this workflow.
	if skipped, ok := gObj.S(append(w.metaPath, "skipped")...).Data().([]interface{}); ok {
		for _, id := range skipped {
			if idStr, isString := id.(string); isString {
				if _, exists := w.allStages[idStr]; exists {
					skipList[idStr] = struct{}{}
				}
			}
		}
	}

	return skipList
}

// ProcessMessage applies workflow stages to each part of a message type.
func (w *workflowDeprecated) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	w.mCount.Incr(1)

	skipOnMeta := make([]map[string]struct{}, msg.Len())
	payload := msg.DeepCopy()
	payload.Iter(func(i int, p types.Part) error {
		p.Get()
		p.Metadata()
		if jObj, err := p.JSON(); err == nil {
			skipOnMeta[i] = w.skipFromMeta(jObj)
		} else {
			skipOnMeta[i] = map[string]struct{}{}
		}
		return nil
	})

	propMsg, _ := tracing.WithChildSpans("workflow", payload)

	records := make([]*deprecatedResultTracker, payload.Len())
	for i := range records {
		records[i] = deprecatedTrackerFromTree(w.dag)
	}

	for _, layer := range w.dag {
		results := make([]types.Message, len(layer))
		errors := make([]error, len(layer))

		wg := sync.WaitGroup{}
		wg.Add(len(layer))
		for i, eid := range layer {
			go func(id string, index int) {
				msgCopy := propMsg.Copy()
				msgCopy.Iter(func(partIndex int, p types.Part) error {
					if _, exists := skipOnMeta[partIndex][id]; exists {
						p.Set(nil)
					}
					return nil
				})

				var resSpans []*tracing.Span
				results[index], resSpans = tracing.WithChildSpans(id, msgCopy)
				errors[index] = w.children[id].CreateResult(results[index])
				for _, s := range resSpans {
					s.Finish()
				}
				results[index].Iter(func(j int, p types.Part) error {
					if p.IsEmpty() {
						records[j].Skipped(id)
					}
					if HasFailed(p) {
						records[j].Failed(id)
						p.Set(nil)
					}
					return nil
				})
				wg.Done()
			}(eid, i)
		}
		wg.Wait()

		for i, id := range layer {
			var failed []int
			err := errors[i]
			if err == nil {
				if failed, err = w.children[id].OverlayResult(payload, results[i]); err != nil {
					w.mErrOverlay.Incr(1)
				}
			}
			if err != nil {
				w.incrStageErr(id)
				w.mErr.Incr(1)
				w.log.Errorf("Failed to perform enrichment '%v': %v\n", id, err)
				for j := range records {
					records[j].Failed(id)
				}
				continue
			}
			for _, j := range failed {
				records[j].Failed(id)
			}
			w.incrStageSucc(id)
		}
	}

	// Finally, set the meta records of each document.
	if len(w.metaPath) > 0 {
		payload.Iter(func(i int, p types.Part) error {
			pJSON, err := p.JSON()
			if err != nil {
				w.mErr.Incr(1)
				w.mErrMeta.Incr(1)
				w.log.Errorf("Failed to parse message for meta update: %v\n", err)
				return nil
			}

			gObj := gabs.Wrap(pJSON)
			if oldRecord := gObj.S(w.metaPath...).Data(); oldRecord != nil {
				gObj.Delete(w.metaPath...)
				gObj.Set(oldRecord, append(w.metaPath, "previous")...)
			}

			succStrs, skipStrs, failStrs := records[i].ToSlices()
			succeeded := make([]interface{}, len(succStrs))
			skipped := make([]interface{}, len(skipStrs))
			failed := make([]interface{}, len(failStrs))

			for j, v := range succStrs {
				succeeded[j] = v
			}
			for j, v := range skipStrs {
				skipped[j] = v
			}
			for j, v := range failStrs {
				failed[j] = v
			}

			gObj.Set(succeeded, append(w.metaPath, "succeeded")...)
			gObj.Set(skipped, append(w.metaPath, "skipped")...)
			gObj.Set(failed, append(w.metaPath, "failed")...)

			p.SetJSON(gObj.Data())
			return nil
		})
	}

	tracing.FinishSpans(propMsg)

	w.mSentParts.Incr(int64(payload.Len()))
	w.mSent.Incr(1)
	msgs := [1]types.Message{payload}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (w *workflowDeprecated) CloseAsync() {
	for _, c := range w.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (w *workflowDeprecated) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range w.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
