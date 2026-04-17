package dag

import (
	"errors"
	"log"
	"sync"
	"github.com/hashicorp/terraform/tfdiags"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

// Walker is used to walk every vertex of a graph in parallel.
//
// A vertex will only be walked when the dependencies of that vertex have
// been walked. If two vertices can be walked at the same time, they will be.
//
// Update can be called to update the graph. This can be called even during
// a walk, cahnging vertices/edges mid-walk. This should be done carefully.
// If a vertex is removed but has already been executed, the result of that
// execution (any error) is still returned by Wait. Changing or re-adding
// a vertex that has already executed has no effect. Changing edges of
// a vertex that has already executed has no effect.
//
// Non-parallelism can be enforced by introducing a lock in your callback
// function. However, the goroutine overhead of a walk will remain.
// Walker will create V*2 goroutines (one for each vertex, and dependency
// waiter for each vertex). In general this should be of no concern unless
// there are a huge number of vertices.
//
// The walk is depth first by default. This can be changed with the Reverse
// option.
//
// A single walker is only valid for one graph walk. After the walk is complete
// you must construct a new walker to walk again. State for the walk is never
// deleted in case vertices or edges are changed.
type Walker struct {
	// Callback is what is called for each vertex
	// Return false, if the node visit need to be skipped
	Callback WalkFunc

	// Reverse, if true, causes the source of an edge to depend on a target.
	// When false (default), the target depends on the source.
	Reverse bool

	// changeLock must be held to modify any of the fields below. Only Update
	// should modify these fields. Modifying them outside of Update can cause
	// serious problems.
	changeLock sync.Mutex
	vertices   Set
	edges      Set
	vertexMap  map[Vertex]*walkerVertex

	// wait is done when all vertices have executed. It may become "undone"
	// if new vertices are added.
	//wait sync.WaitGroup
	futures []workflow.Future

	VisitedNodes map[Vertex]bool
	// diagsMap contains the diagnostics recorded so far for execution,
	// and upstreamFailed contains all the vertices whose problems were
	// caused by upstream failures, and thus whose diagnostics should be
	// excluded from the final set.
	//
	// Readers and writers of either map must hold diagsLock.
	diagsMap       map[Vertex]tfdiags.Diagnostics
	upstreamFailed map[Vertex]struct{}
	diagsLock      sync.Mutex
}

type walkerVertex struct {
	// These should only be set once on initialization and never written again.
	// They are not protected by a lock since they don't need to be since
	// they are write-once.

	// DoneCh is closed when this vertex has completed execution, regardless
	// of success.
	//
	// CancelCh is closed when the vertex should cancel execution. If execution
	// is already complete (DoneCh is closed), this has no effect. Otherwise,
	// execution is cancelled as quickly as possible.
	DoneCh   workflow.Channel
	CancelCh workflow.Channel

	// Dependency information. Any changes to any of these fields requires
	// holding DepsLock.
	//
	// DepsCh is sent a single value that denotes whether the upstream deps
	// were successful (no errors). Any value sent means that the upstream
	// dependencies are complete. No other values will ever be sent again.
	//
	// DepsUpdateCh is closed when there is a new DepsCh set.
	DepsCh       workflow.Channel
	DepsUpdateCh workflow.Channel
	DepsLock     sync.Mutex

	// Below is not safe to read/write in parallel. This behavior is
	// enforced by changes only happening in Update. Nothing else should
	// ever modify these.
	deps         map[Vertex]workflow.Channel
	depsCancelCh workflow.Channel
}

// errWalkUpstream is used in the errMap of a walk to note that an upstream
// dependency failed so this vertex wasn't run. This is not shown in the final
// user-returned error.
var errWalkUpstream = errors.New("upstream dependency failed")

// Wait waits for the completion of the walk and returns diagnostics describing
// any problems that arose. Update should be called to populate the walk with
// vertices and edges prior to calling this.
//
// Wait will return as soon as all currently known vertices are complete.
// If you plan on calling Update with more vertices in the future, you
// should not call Wait until after this is done.
//
// 3/15/2019: Modified to return upon one of the future returns or all walked. add third return as if the walker completes
func (w *Walker) Wait(ctx workflow.Context) ([]Vertex, tfdiags.Diagnostics, bool) {

	var diags tfdiags.Diagnostics
	var ret []Vertex

	s := workflow.NewSelector(ctx)

	log.Printf("Waiting on %d nodes", len(w.futures))
	if len(w.futures) == 0 {
		return nil, nil, true
	}

	for _, f := range w.futures {
		s.AddFuture(f, func(future workflow.Future) {
			var v Vertex

			var futures []workflow.Future
			for _, fc := range w.futures {
				if fc.IsReady() {
					fc.Get(ctx, &v)

					if v != nil {
						ret = append(ret, v)
					}

				} else {
					futures = append(futures, fc)
				}
			}

			w.futures = futures

		})
	}

	s.Select(ctx)


	w.diagsLock.Lock()
	for v, vDiags := range w.diagsMap {
		if _, upstream := w.upstreamFailed[v]; upstream {
			// Ignore diagnostics for nodes that had failed upstreams, since
			// the downstream diagnostics are likely to be redundant.
			continue
		}
		diags = diags.Append(vDiags)
	}
	w.diagsLock.Unlock()

	return ret, diags, false
}

// Update updates the currently executing walk with the given graph.
// This will perform a diff of the vertices and edges and update the walker.
// Already completed vertices remain completed (including any errors during
// their execution).
//
// This returns immediately once the walker is updated; it does not wait
// for completion of the walk.
//
// Multiple Updates can be called in parallel. Update can be called at any
// time during a walk.
func (w *Walker) Update(ctx workflow.Context, cancelFunc workflow.CancelFunc, graph *DagGraph) {

	g := graph.Graph
	log.Printf("[TRACE] dag/walk: updating graph")
	var v, e *Set
	if g != nil {
		v, e = g.vertices, g.edges
	}

	// Grab the change lock so no more updates happen but also so that
	// no new vertices are executed during this time since we may be
	// removing them.
	w.changeLock.Lock()
	defer w.changeLock.Unlock()

	// Initialize fields
	if w.vertexMap == nil {
		w.vertexMap = make(map[Vertex]*walkerVertex)
	}

	// Calculate all our sets
	newEdges := e.Difference(&w.edges)
	oldEdges := w.edges.Difference(e)
	newVerts := v.Difference(&w.vertices)
	oldVerts := w.vertices.Difference(v)

	// Add the new vertices
	for _, raw := range newVerts.List() {
		v := raw.(Vertex)

		// Add to our own set so we know about it already
		log.Printf("[TRACE] dag/walk: added new vertex: %q", VertexName(v))
		w.vertices.Add(raw)

		// Initialize the vertex info
		info := &walkerVertex{
			DoneCh:   workflow.NewChannel(ctx),
			CancelCh: workflow.NewChannel(ctx),
			deps:     make(map[Vertex]workflow.Channel),
		}

		// Add it to the map and kick off the walk
		w.vertexMap[v] = info
	}

	// Remove the old vertices
	for _, raw := range oldVerts.List() {
		v := raw.(Vertex)

		// Get the vertex info so we can cancel it
		info, ok := w.vertexMap[v]
		if !ok {
			// This vertex for some reason was never in our map. This
			// shouldn't be possible.
			continue
		}

		// Cancel the vertex
		info.CancelCh.Close()

		// Delete it out of the map
		delete(w.vertexMap, v)

		log.Printf("[TRACE] dag/walk: removed vertex: %q", VertexName(v))
		w.vertices.Delete(raw)
	}

	// Add the new edges
	var changedDeps Set
	for _, raw := range newEdges.List() {
		edge := raw.(Edge)
		waiter, dep := w.edgeParts(edge)

		// Get the info for the waiter
		waiterInfo, ok := w.vertexMap[waiter]
		if !ok {
			// Vertex doesn't exist... shouldn't be possible but ignore.
			continue
		}

		// Get the info for the dep
		depInfo, ok := w.vertexMap[dep]
		if !ok {
			// Vertex doesn't exist... shouldn't be possible but ignore.
			continue
		}

		// Add the dependency to our waiter
		waiterInfo.deps[dep] = depInfo.DoneCh

		// Record that the deps changed for this waiter
		changedDeps.Add(waiter)

		log.Printf(
			"[TRACE] dag/walk: added edge: %q waiting on %q",
			VertexName(waiter), VertexName(dep))
		w.edges.Add(raw)
	}

	// Process reoved edges
	for _, raw := range oldEdges.List() {
		edge := raw.(Edge)
		waiter, dep := w.edgeParts(edge)

		// Get the info for the waiter
		waiterInfo, ok := w.vertexMap[waiter]
		if !ok {
			// Vertex doesn't exist... shouldn't be possible but ignore.
			continue
		}

		// Delete the dependency from the waiter
		delete(waiterInfo.deps, dep)

		// Record that the deps changed for this waiter
		changedDeps.Add(waiter)

		log.Printf(
			"[TRACE] dag/walk: removed edge: %q waiting on %q",
			VertexName(waiter), VertexName(dep))
		w.edges.Delete(raw)
	}

	// For each vertex with changed dependencies, we need to kick off
	// a new waiter and notify the vertex of the changes.
	for _, raw := range changedDeps.List() {
		v := raw.(Vertex)
		info, ok := w.vertexMap[v]
		if !ok {
			// Vertex doesn't exist... shouldn't be possible but ignore.
			continue
		}

		// Create a new done channel
		doneCh := workflow.NewChannel(ctx)

		// Create the channel we close for cancellation
		cancelCh := workflow.NewChannel(ctx)

		// Build a new deps copy
		deps := make(map[Vertex]workflow.Channel)
		for k, v := range info.deps {
			deps[k] = v
		}

		// Update the update channel
		info.DepsLock.Lock()
		if info.DepsUpdateCh != nil {
			info.DepsUpdateCh.Close()
		}
		info.DepsCh = doneCh
		info.DepsUpdateCh = workflow.NewChannel(ctx)
		info.DepsLock.Unlock()

		// Cancel the older waiter
		if info.depsCancelCh != nil {
			info.depsCancelCh.Close()
		}
		info.depsCancelCh = cancelCh

		log.Printf(
			"[TRACE] dag/walk: dependencies changed for %q, sending new deps",
			VertexName(v))

		// Start the waiter

		workflow.Go(ctx, func(ctx workflow.Context) {
			w.waitDeps(ctx, v, deps, doneCh, cancelCh)
		})

	}

	// Start all the new vertices. We do this at the end so that all
	// the edge waiters and changes are setup above.

	gracefulCancel := new(BoolStruct)
	for _, raw := range newVerts.List() {
		v := raw.(Vertex)

		future, settable := workflow.NewFuture(ctx)
		workflow.Go(ctx, func(ctx workflow.Context) {
			isAdd := w.walkVertex(ctx, graph, cancelFunc, v, w.vertexMap[v], gracefulCancel, w.VisitedNodes)

			if isAdd {
				log.Printf("Walker node %s added", v)
				settable.Set(v, nil)
			} else {
				settable.Set(nil, errors.New("node visited or skipped"))
			}
		})

		w.futures = append(w.futures, future)
	}

}

// edgeParts returns the waiter and the dependency, in that order.
// The waiter is waiting on the dependency.
func (w *Walker) edgeParts(e Edge) (Vertex, Vertex) {
	if w.Reverse {
		return e.Source(), e.Target()
	}

	return e.Target(), e.Source()
}

// walkVertex walks a single vertex, waiting for any dependencies before
// executing the callback.
func (w *Walker) walkVertex(ctx workflow.Context, graph *DagGraph, cancelFunc workflow.CancelFunc, v Vertex,
	info *walkerVertex, gracefulCancel *BoolStruct, visitedNodes map[Vertex]bool) bool {

	ret := false
	logger := workflow.GetLogger(ctx)

	// When we're done executing, lower the waitgroup count
	//defer w.wait.Done()
	// When we're done, always close our done channel
	defer info.DoneCh.Close()

	// Wait for our dependencies. We create a [closed] deps channel so
	// that we can immediately fall through to load our actual DepsCh.
	var depsSuccess bool
	var depsUpdateCh = workflow.NewChannel(ctx)
	depsCh := workflow.NewBufferedChannel(ctx, 1)
	depsCh.Send(ctx, true)
	depsCh.Close()

	for {

		selector := workflow.NewSelector(ctx)

		isReturn := false
		selector.AddReceive(info.CancelCh, func(c workflow.Channel, more bool) {
			c.Receive(ctx, nil)
			// Cancel
			isReturn = true
		})

		selector.AddReceive(depsCh, func(c workflow.Channel, more bool) {
			depsSuccess = c.Receive(ctx, &depsSuccess)
			// Deps complete! Mark as nil to trigger completion handling.
			depsCh = nil
		})

		selector.AddReceive(depsUpdateCh, func(c workflow.Channel, more bool) {
			// New deps, reloop
			c.Receive(ctx, nil)
		})

		selector.Select(ctx)

		if isReturn {
			return ret
		}

		// Check if we have updated dependencies. This can happen if the
		// dependencies were satisfied exactly prior to an Update occurring.
		// In that case, we'd like to take into account new dependencies
		// if possible.
		info.DepsLock.Lock()
		if info.DepsCh != nil {
			depsCh = info.DepsCh
			info.DepsCh = nil
		}
		if info.DepsUpdateCh != nil {
			depsUpdateCh = info.DepsUpdateCh
		}
		info.DepsLock.Unlock()

		// If we still have no deps channel set, then we're done!
		if depsCh == nil {
			break
		}
	}

	// If we passed dependencies, we just want to check once more that
	// we're not cancelled, since this can happen just as dependencies pass.
	selector := workflow.NewSelector(ctx)

	isReturn := false;
	selector.AddReceive(info.CancelCh, func(c workflow.Channel, more bool) {
		c.Receive(ctx, nil)
		isReturn = true;
	})

	selector.AddDefault(func() {})

	selector.Select(ctx)

	if isReturn {
		return ret
	}

	// Run our callback or note that our upstream failed
	var diags tfdiags.Diagnostics
	var upstreamFailed bool
	if depsSuccess {
		logger.Info("[TRACE] dag/walk: visiting ", zap.String("node", VertexName(v)))
		ret, diags = w.Callback(ctx, graph, cancelFunc, v, gracefulCancel, visitedNodes)
	} else {
		log.Printf("[TRACE] dag/walk: upstream of %q errored, so skipping", VertexName(v))
		// This won't be displayed to the user because we'll set upstreamFailed,
		// but we need to ensure there's at least one error in here so that
		// the failures will cascade downstream.
		diags = diags.Append(errors.New("upstream dependencies failed"))
		upstreamFailed = true
	}

	// Record the result (we must do this after execution because we mustn't
	// hold diagsLock while visiting a vertex.)
	w.diagsLock.Lock()
	if w.diagsMap == nil {
		w.diagsMap = make(map[Vertex]tfdiags.Diagnostics)
	}
	w.diagsMap[v] = diags
	if w.upstreamFailed == nil {
		w.upstreamFailed = make(map[Vertex]struct{})
	}
	if upstreamFailed {
		w.upstreamFailed[v] = struct{}{}
	}
	w.diagsLock.Unlock()
	return ret
}

func (w *Walker) waitDeps(
	ctx workflow.Context,
	v Vertex,
	deps map[Vertex]workflow.Channel,
	doneCh workflow.Channel,
	cancelCh workflow.Channel) {

	//timerChan := workflow.NewChannel(ctx)

	// For each dependency given to us, wait for it to complete
	for dep, depCh := range deps {
	DepSatisfied:
		for {

			breakDepSatisfied := false;
			isReturn := false;
			selector := workflow.NewSelector(ctx)
			selector.AddReceive(depCh, func(c workflow.Channel, more bool) {
				c.Receive(ctx, nil)
				breakDepSatisfied = true;
			})

			selector.AddReceive(cancelCh, func(c workflow.Channel, more bool) {
				c.Receive(ctx, nil)
				// Wait cancelled. Note that we didn't satisfy dependencies
				// so that anything waiting on us also doesn't run.
				doneCh.Send(ctx, false)

				isReturn = true
			})

			//workflow.Go(ctx, func(ctx workflow.Context) {
			//	<- time.After(5 * time.Second)
			//	timerChan.Send(ctx, true)
			//})
			//
			//selector.AddReceive(timerChan, func(c workflow.Channel, more bool) {
			//	c.Receive(ctx, nil)
			//	log.Printf("[TRACE] dag/walk: vertex %q is waiting for %q",
			//		VertexName(v), VertexName(dep))
			//})

			log.Printf("[TRACE] dag/walk: vertex %q is waiting for %q", VertexName(v), VertexName(dep))
			selector.Select(ctx)

			if breakDepSatisfied {
				// Dependency satisfied!
				break DepSatisfied
			}

			if isReturn {
				return
			}

		}
	}

	// Dependencies satisfied! We need to check if any errored
	w.diagsLock.Lock()
	defer w.diagsLock.Unlock()
	for dep := range deps {
		if w.diagsMap[dep].HasErrors() {
			// One of our dependencies failed, so return false
			doneCh.Send(ctx, false)
			return
		}
	}

	// All dependencies satisfied and successful
	doneCh.Send(ctx, true)
}
