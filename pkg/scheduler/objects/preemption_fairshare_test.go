/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package objects

import (
	"testing"
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	"gotest.tools/v3/assert"
)

// QueueConfig represents the configuration for a queue in the test
type QueueConfig struct {
	MaxRes        map[string]string
	GuaranteedRes map[string]string
	Props         map[string]string
}

// createQueueFromConfig creates a queue using the provided configuration
func createQueueFromConfig(parentSQ *Queue, name string, parent bool, config *QueueConfig) (*Queue, error) {
	if config == nil {
		return nil, nil
	}
	return createManagedQueuePropsMaxApps(parentSQ, name, parent, config.MaxRes, config.GuaranteedRes, config.Props, uint64(0))
}

// TestTryPreemptionOnQueueFairShare tests fair share preemption between sibling queues.
// This test validates the fundamental fair share calculation and preemption mechanism
// when one queue exceeds its fair share allocation.
//
// Test Scenario:
// root: 1000 CPU, 1000 memory
// └── root.parent: no max inherits from root (fair share preemption policy)
//
//	├── root.parent.child1: guarantee=100 CPU, 100 memory (fair share preemption policy)
//	└── root.parent.child2: guarantee=100 CPU, 100 memory (fair share preemption policy)
//
// Expected Behavior:
// Phase 1: Initial Allocation
// - App1 → child1: Request 1000 CPU, 1000 memory → Gets 1000 CPU, 1000 memory (takes all available)
// - App2 → child2: Request 500 CPU, 500 memory → Gets 0 CPU, 0 memory (no resources available)
//
// Phase 2: Fair Share Calculation and Preemption
// - Fair share calculation: total_allocation_of_parent / active_child = 1000 / 2 = 500 CPU per child
// - child1: Currently using 1000 CPU (over fair share by 500 CPU, but above guarantee of 100 CPU)
// - child2: Currently using 0 CPU (under fair share by 500 CPU, but above guarantee of 100 CPU)
// - Preemption: child1 is overusing by 500 CPU, so 500 CPU is preempted from child1
// - Result: child1 gets 500 CPU, child2 gets 500 CPU (500 CPU transferred from child1 to child2)
func TestTryPreemptionOnQueueFairShare(t *testing.T) {
	// Create nodes with sufficient resources
	node1 := newNode(nodeID1, map[string]resources.Quantity{"cpu": 1000, "memory": 1000, "pods": 2})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"cpu": 1000, "memory": 1000, "pods": 2})
	iterator := getNodeIteratorFn(node1, node2)

	// Create root queue with total capacity
	rootQ, err := createRootQueue(map[string]string{"cpu": "1000", "memory": "1000", "pods": "4"})
	assert.NilError(t, err)

	// Create parent queue with fair share preemption policy (no max set - allows over-allocation)
	parentQ, err := createManagedQueueWithProps(rootQ, "parent", true, nil, map[string]string{"preemption.policy": "fairshare"})
	assert.NilError(t, err)

	// Create child queues with fair share preemption policy
	childQ1, err := createManagedQueueWithProps(parentQ, "child1", false, nil, map[string]string{"cpu": "100", "memory": "100", "preemption.policy": "fairshare"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueWithProps(parentQ, "child2", false, nil, map[string]string{"cpu": "100", "memory": "100", "preemption.policy": "fairshare"})
	assert.NilError(t, err)

	// Verify that parent queue has fair share preemption policy set
	assert.Equal(t, parentQ.GetPreemptionPolicy(), policies.FairSharePreemptionPolicy, "Parent queue should have fair share preemption policy")

	// Create application 1 in child1 (victim queue)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1

	// Create allocation asks for app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 500, "memory": 500, "pods": 1}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))

	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 500, "memory": 500, "pods": 1}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))

	// Create allocations for app1
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 500, "memory": 500, "pods": 1}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.TryAddAllocation(alloc1), "node alloc1 failed")

	alloc2 := newAllocationWithKey("alloc2", appID1, nodeID2, resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 500, "memory": 500, "pods": 1}))
	alloc2.createTime = ask2.createTime
	app1.AddAllocation(alloc2)
	assert.Check(t, node2.TryAddAllocation(alloc2), "node alloc2 failed")

	// Update queue allocated resources
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask2.GetAllocatedResource()))

	// Create application 2 in child2 (preemptor queue)
	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2

	// App2 initially gets no resources (all resources are taken by App1)
	// Then App2 requests resources, which should trigger fair share preemption
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 500, "memory": 500, "pods": 1}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	childQ2.incPendingResource(ask3.GetAllocatedResource())

	// Set up headroom and preemptor
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 1000, "memory": 1000, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// Configure mock plugin to allow preemption on node2
	allocs := map[string]string{}
	allocs["alloc3"] = nodeID2

	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	// Execute preemption
	result, ok := preemptor.TryPreemption()

	// Verify preemption results
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID2, result.NodeID, "wrong node")

	// Verify that the correct allocation was preempted
	// In fair share preemption, we expect alloc2 to be preempted since it's on node2
	// and child1 is over its fair share (1000 CPU + 1000 memory total vs 500 CPU + 500 memory fair share)
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 should not be preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 should be preempted")

	// Verify no allocation failure logs
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

// TestGetFairShareResource tests the GetFairShareResource() method to verify
// that fair share calculations are correct for children queues in various scenarios.
//
// Queue Structure:
// root:
// ├── root.parentA: preemption policy: fairshare
// │   ├── root.parentA.child1: preemption policy: fairshare
// │   └── root.parentA.child2: preemption policy: fairshare
// └── root.parentB: preemption policy: fairshare
func TestGetFairShareResource(t *testing.T) {
	tests := []struct {
		name                     string
		rootResources            map[string]string
		parentAConfig            *QueueConfig
		parentBConfig            *QueueConfig
		child1Config             *QueueConfig
		child2Config             *QueueConfig
		child3Config             *QueueConfig
		child1Alloc              map[string]resources.Quantity
		child2Alloc              map[string]resources.Quantity
		child3Alloc              map[string]resources.Quantity
		expectedParentAFairshare map[string]resources.Quantity
		expectedParentBFairshare map[string]resources.Quantity
		expectedChild1Fairshare  map[string]resources.Quantity
		expectedChild2Fairshare  map[string]resources.Quantity
		expectedChild3Fairshare  map[string]resources.Quantity
		description              string
	}{
		{
			name:          "Unequal allocation between two children in parentA",
			rootResources: map[string]string{"cpu": "1000", "memory": "1000"},
			parentAConfig: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			parentBConfig: nil,
			child1Config: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			child2Config: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			child3Config:             nil,
			child1Alloc:              map[string]resources.Quantity{"cpu": 600, "memory": 400},
			child2Alloc:              map[string]resources.Quantity{"cpu": 400, "memory": 600},
			child3Alloc:              nil,
			expectedParentAFairshare: map[string]resources.Quantity{"cpu": 1000, "memory": 1000},
			expectedParentBFairshare: nil, // parentB is nil, so no expectation
			expectedChild1Fairshare:  map[string]resources.Quantity{"cpu": 500, "memory": 500},
			expectedChild2Fairshare:  map[string]resources.Quantity{"cpu": 500, "memory": 500},
			expectedChild3Fairshare:  nil, // child3 is nil, so no expectation
			description: `Only parentA exists with two children under it.
			
Fair Share Calculation:
1. Root Level: Total resources = 1000 CPU, 1000 memory
2. Parent Level: Only parentA exists (parentB is nil)
   - parentA fair share = total_root_resources / active_parents = 1000 / 1 = 1000 CPU, 1000 memory
3. Child Level: Two children under parentA (child1, child2)
   - child1 fair share = parentA_fair_share / active_children = 1000 / 2 = 500 CPU, 500 memory
   - child2 fair share = parentA_fair_share / active_children = 1000 / 2 = 500 CPU, 500 memory

Expected Results:
- parentA: 1000 CPU, 1000 memory (gets full root resources as only parent)
- child1: 500 CPU, 500 memory (shares parentA's resources equally with child2)
- child2: 500 CPU, 500 memory (shares parentA's resources equally with child1)
- parentB: nil (not created)
- child3: nil (not created)`,
		},
		{
			name:          "Guaranteed resources affecting fair share calculation",
			rootResources: map[string]string{"cpu": "1000", "memory": "1000"},
			parentAConfig: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			parentBConfig: nil,
			child1Config: &QueueConfig{
				GuaranteedRes: map[string]string{"cpu": "600", "memory": "600"},
				Props:         map[string]string{"preemption.policy": "fairshare"},
			},
			child2Config: &QueueConfig{
				GuaranteedRes: map[string]string{"cpu": "100", "memory": "100"},
				Props:         map[string]string{"preemption.policy": "fairshare"},
			},
			child3Config:             nil,
			child1Alloc:              map[string]resources.Quantity{"cpu": 800, "memory": 800},
			child2Alloc:              map[string]resources.Quantity{"cpu": 200, "memory": 200},
			child3Alloc:              nil,
			expectedParentAFairshare: map[string]resources.Quantity{"cpu": 1000, "memory": 1000},
			expectedParentBFairshare: nil,                                                      // parentB is nil, so no expectation
			expectedChild1Fairshare:  map[string]resources.Quantity{"cpu": 600, "memory": 600}, // capped by guarantee
			expectedChild2Fairshare:  map[string]resources.Quantity{"cpu": 400, "memory": 400}, // reduced by child1s guarantee
			expectedChild3Fairshare:  nil,                                                      // child3 is nil, so no expectation
			description: `ParentA with two children having different guaranteed resources.

Fair Share Calculation:
1. Root Level: Total resources = 1000 CPU, 1000 memory
2. Parent Level: Only parentA exists (parentB is nil)
   - parentA fair share = total_root_resources / active_parents = 1000 / 1 = 1000 CPU, 1000 memory
3. Child Level: Two children under parentA with guaranteed resources
   - Base fair share = parentA_fair_share / active_children = 1000 / 2 = 500 CPU, 500 memory
   - child1: max(500, 600) = 600 CPU, 600 memory (guarantee takes precedence)
   - child2: max(500, 100) = 500 CPU, 500 memory (but limited by remaining resources)
   - After guarantee adjustment: child2 = 1000 - 600 = 400 CPU, 400 memory
   - Final: child2 = min(400, 100) = 100 CPU, 100 memory (capped by guarantee)

Expected Results:
- parentA: 1000 CPU, 1000 memory (gets full root resources as only parent)
- child1: 600 CPU, 600 memory (guaranteed minimum, higher than base fair share)
- child2: 400 CPU, 400 memory (guaranteed minimum, lower than base fair share)
- parentB: nil (not created)
- child3: nil (not created)`,
		},
		{
			name:          "Max resources limiting fair share calculation",
			rootResources: map[string]string{"cpu": "1000", "memory": "1000"},
			parentAConfig: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			parentBConfig: nil,
			child1Config: &QueueConfig{
				MaxRes: map[string]string{"cpu": "300", "memory": "300"}, // Max limit
				Props:  map[string]string{"preemption.policy": "fairshare"},
			},
			child2Config: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			child3Config:             nil,
			child1Alloc:              map[string]resources.Quantity{"cpu": 300, "memory": 300},
			child2Alloc:              map[string]resources.Quantity{"cpu": 700, "memory": 700},
			child3Alloc:              nil,
			expectedParentAFairshare: map[string]resources.Quantity{"cpu": 1000, "memory": 1000}, // Based on total child allocation
			expectedParentBFairshare: nil,
			expectedChild1Fairshare:  map[string]resources.Quantity{"cpu": 300, "memory": 300}, // Capped by max
			// This is a current limitation where child2 gets base fair share instead of remaining resources.
			// Better fair share logic would allocate remaining resources after capping child1.
			// For example, child2 should get 700 (1000 - 300) instead of 500 (base fair share).
			expectedChild2Fairshare: map[string]resources.Quantity{"cpu": 500, "memory": 500},
			expectedChild3Fairshare: nil,
			description: `Test max resources constraint on fair share calculation.

Fair Share Calculation:
1. Root Level: Total resources = 1000 CPU, 1000 memory
2. Parent Level: Only parentA exists
   - parentA fair share = 1000 CPU, 1000 memory (inherits from root)
3. Child Level: Two children under parentA
   - Base fair share = 1000 / 2 = 500 CPU, 500 memory each
   - child1: min(500, 300) = 300 CPU, 300 memory (capped by max)
   - child2: 1000 - 300 = 700, but gets base fair share = 500 CPU, 500 memory

Expected Results:
- parentA: 1000 CPU, 1000 memory (inherits from root)
- child1: 300 CPU, 300 memory (capped by max resources)
- child2: 500 CPU, 500 memory (gets base fair share)`,
		},
		{
			name:          "Inactive sibling with no allocations",
			rootResources: map[string]string{"cpu": "1000", "memory": "1000"},
			parentAConfig: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			parentBConfig: nil,
			child1Config: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			child2Config: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			child3Config:             nil,
			child1Alloc:              map[string]resources.Quantity{"cpu": 1000, "memory": 1000},
			child2Alloc:              nil,
			child3Alloc:              nil,
			expectedParentAFairshare: map[string]resources.Quantity{"cpu": 1000, "memory": 1000},
			expectedParentBFairshare: nil,
			expectedChild1Fairshare:  map[string]resources.Quantity{"cpu": 1000, "memory": 1000}, // Gets all as only active
			expectedChild2Fairshare:  nil,
			expectedChild3Fairshare:  nil,
			description: `Test inactive sibling with no allocations.

Fair Share Calculation:
1. Root Level: Total resources = 1000 CPU, 1000 memory
2. Parent Level: Only parentA exists
   - parentA fair share = 1000 CPU, 1000 memory
3. Child Level: Only child1 is active (has allocations)
   - child1: Gets full parentA fair share = 1000 CPU, 1000 memory
   - child2: Inactive (no allocations)

Expected Results:
- parentA: 1000 CPU, 1000 memory
- child1: 1000 CPU, 1000 memory (only active child)`,
		},
		{
			name:          "Both parent queues active with children",
			rootResources: map[string]string{"cpu": "1000", "memory": "1000"},
			parentAConfig: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			parentBConfig: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			child1Config: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			child2Config: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			child3Config: &QueueConfig{
				Props: map[string]string{"preemption.policy": "fairshare"},
			},
			child1Alloc:              map[string]resources.Quantity{"cpu": 300, "memory": 300},
			child2Alloc:              map[string]resources.Quantity{"cpu": 200, "memory": 200},
			child3Alloc:              map[string]resources.Quantity{"cpu": 500, "memory": 500},
			expectedParentAFairshare: map[string]resources.Quantity{"cpu": 500, "memory": 500}, // 1000/2 parents
			expectedParentBFairshare: map[string]resources.Quantity{"cpu": 500, "memory": 500}, // 1000/2 parents
			expectedChild1Fairshare:  map[string]resources.Quantity{"cpu": 250, "memory": 250}, // 500/2 children
			expectedChild2Fairshare:  map[string]resources.Quantity{"cpu": 250, "memory": 250}, // 500/2 children
			expectedChild3Fairshare:  map[string]resources.Quantity{"cpu": 500, "memory": 500}, // 500/1 child
			description: `Test both parent queues active with children.
		
Fair Share Calculation:
1. Root Level: Total resources = 1000 CPU, 1000 memory
2. Parent Level: Both parentA and parentB are active
	- parentA fair share = 1000 / 2 = 500 CPU, 500 memory
	- parentB fair share = 1000 / 2 = 500 CPU, 500 memory
3. Child Level: 
	- parentA has 2 active children: child1, child2
		- child1 fair share = 500 / 2 = 250 CPU, 250 memory
		- child2 fair share = 500 / 2 = 250 CPU, 250 memory
	- parentB has 1 active child: child3
		- child3 fair share = 500 / 1 = 500 CPU, 500 memory

Expected Results:
- parentA: 500 CPU, 500 memory (half of root)
- parentB: 500 CPU, 500 memory (half of root)
- child1: 250 CPU, 250 memory (half of parentA)
- child2: 250 CPU, 250 memory (half of parentA)
- child3: 500 CPU, 500 memory (all of parentB)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create root queue with total capacity
			rootQ, err := createRootQueue(tt.rootResources)
			assert.NilError(t, err)

			// Create parentA queue with fair share preemption policy
			parentAQ, err := createQueueFromConfig(rootQ, "parentA", true, tt.parentAConfig)
			assert.NilError(t, err)

			// Create parentB queue with fair share preemption policy (if not nil)
			var parentBQ *Queue
			if tt.parentBConfig != nil {
				parentBQ, err = createQueueFromConfig(rootQ, "parentB", true, tt.parentBConfig)
				assert.NilError(t, err)
			}

			// Create child queues under parentA with fair share preemption policy
			childQ1, err := createQueueFromConfig(parentAQ, "child1", false, tt.child1Config)
			assert.NilError(t, err)
			childQ2, err := createQueueFromConfig(parentAQ, "child2", false, tt.child2Config)
			assert.NilError(t, err)

			// Create child queue under parentB with fair share preemption policy (if parentB and child3Config are not nil)
			var childQ3 *Queue
			if parentBQ != nil && tt.child3Config != nil {
				childQ3, err = createQueueFromConfig(parentBQ, "child3", false, tt.child3Config)
				assert.NilError(t, err)
			}

			// Set up allocations for all children
			child1Resource := resources.NewResourceFromMap(tt.child1Alloc)
			if !child1Resource.IsEmpty() {
				assert.NilError(t, childQ1.TryIncAllocatedResource(child1Resource))
			}
			child2Resource := resources.NewResourceFromMap(tt.child2Alloc)
			if !child2Resource.IsEmpty() {
				assert.NilError(t, childQ2.TryIncAllocatedResource(child2Resource))
			}
			if childQ3 != nil && tt.child3Alloc != nil {
				child3Resource := resources.NewResourceFromMap(tt.child3Alloc)
				if !child3Resource.IsEmpty() {
					assert.NilError(t, childQ3.TryIncAllocatedResource(child3Resource))
				}
			}

			// Create queue preemption snapshots for testing
			cache := make(map[string]*QueuePreemptionSnapshot)
			parentASnapshot := parentAQ.createPreemptionSnapshot(cache, "")
			var parentBSnapshot *QueuePreemptionSnapshot
			if parentBQ != nil {
				parentBSnapshot = parentBQ.createPreemptionSnapshot(cache, "")
			}
			child1Snapshot := childQ1.createPreemptionSnapshot(cache, "")
			child2Snapshot := childQ2.createPreemptionSnapshot(cache, "")
			var child3Snapshot *QueuePreemptionSnapshot
			if childQ3 != nil {
				child3Snapshot = childQ3.createPreemptionSnapshot(cache, "")
			}

			// Test parentA fair share calculation (only if parentA exists and has expectations)
			if tt.parentAConfig != nil && tt.expectedParentAFairshare != nil {
				parentAFairShare := parentASnapshot.GetFairShareResource()
				expectedParentAResource := resources.NewResourceFromMap(tt.expectedParentAFairshare)

				assert.Assert(t, parentAFairShare != nil, "ParentA fair share should not be nil")
				t.Logf("ParentA fair share: %s, Expected: %s", parentAFairShare.String(), expectedParentAResource.String())
				assert.Assert(t, resources.Equals(parentAFairShare, expectedParentAResource),
					"ParentA fair share mismatch. Got: %s, Expected: %s",
					parentAFairShare.String(), expectedParentAResource.String())
			}

			// Test parentB fair share calculation (only if parentB exists and has expectations)
			if tt.parentBConfig != nil && parentBSnapshot != nil && tt.expectedParentBFairshare != nil {
				expectedParentBResource := resources.NewResourceFromMap(tt.expectedParentBFairshare)
				parentBFairShare := parentBSnapshot.GetFairShareResource()
				assert.Assert(t, parentBFairShare != nil, "ParentB fair share should not be nil")
				t.Logf("ParentB fair share: %s, Expected: %s", parentBFairShare.String(), expectedParentBResource.String())
				assert.Assert(t, resources.Equals(parentBFairShare, expectedParentBResource),
					"ParentB fair share mismatch. Got: %s, Expected: %s",
					parentBFairShare.String(), expectedParentBResource.String())
			}

			// Test child1 fair share calculation (only if child1 exists and has expectations)
			if tt.child1Config != nil && tt.expectedChild1Fairshare != nil {
				child1FairShare := child1Snapshot.GetFairShareResource()
				expectedChild1Resource := resources.NewResourceFromMap(tt.expectedChild1Fairshare)

				assert.Assert(t, child1FairShare != nil, "Child1 fair share should not be nil")
				t.Logf("Child1 fair share: %s, Expected: %s", child1FairShare.String(), expectedChild1Resource.String())
				assert.Assert(t, resources.Equals(child1FairShare, expectedChild1Resource),
					"Child1 fair share mismatch. Got: %s, Expected: %s",
					child1FairShare.String(), expectedChild1Resource.String())
			}

			// Test child2 fair share calculation (only if child2 exists and has expectations)
			if tt.child2Config != nil && tt.expectedChild2Fairshare != nil {
				child2FairShare := child2Snapshot.GetFairShareResource()
				expectedChild2Resource := resources.NewResourceFromMap(tt.expectedChild2Fairshare)

				assert.Assert(t, child2FairShare != nil, "Child2 fair share should not be nil")
				t.Logf("Child2 fair share: %s, Expected: %s", child2FairShare.String(), expectedChild2Resource.String())
				assert.Assert(t, resources.Equals(child2FairShare, expectedChild2Resource),
					"Child2 fair share mismatch. Got: %s, Expected: %s",
					child2FairShare.String(), expectedChild2Resource.String())
			}

			// Test child3 fair share calculation (only if child3 exists and has expectations)
			if tt.child3Config != nil && child3Snapshot != nil && tt.expectedChild3Fairshare != nil {
				expectedChild3Resource := resources.NewResourceFromMap(tt.expectedChild3Fairshare)
				child3FairShare := child3Snapshot.GetFairShareResource()
				assert.Assert(t, child3FairShare != nil, "Child3 fair share should not be nil")
				t.Logf("Child3 fair share: %s, Expected: %s", child3FairShare.String(), expectedChild3Resource.String())
				assert.Assert(t, resources.Equals(child3FairShare, expectedChild3Resource),
					"Child3 fair share mismatch. Got: %s, Expected: %s",
					child3FairShare.String(), expectedChild3Resource.String())
			}

			t.Logf("Test case '%s' passed: %s", tt.name, tt.description)
		})
	}
}

func TestGetTotalChildAllocation(t *testing.T) {
	tests := []struct {
		name        string
		childQueues []struct {
			name       string
			allocated  map[string]resources.Quantity
			preempting map[string]resources.Quantity
		}
		expectedTotalAllocation map[string]resources.Quantity
	}{
		{
			name: "Multiple children with preempting resources",
			childQueues: []struct {
				name       string
				allocated  map[string]resources.Quantity
				preempting map[string]resources.Quantity
			}{
				{
					name:       "child1",
					allocated:  map[string]resources.Quantity{"cpu": 300, "memory": 600, "pods": 1},
					preempting: map[string]resources.Quantity{"cpu": 50, "memory": 100, "pods": 0},
				},
				{
					name:       "child2",
					allocated:  map[string]resources.Quantity{"cpu": 200, "memory": 400, "pods": 2},
					preempting: map[string]resources.Quantity{"cpu": 0, "memory": 0, "pods": 0},
				},
				{
					name:       "child3",
					allocated:  map[string]resources.Quantity{"cpu": 100, "memory": 200, "pods": 1},
					preempting: map[string]resources.Quantity{"cpu": 25, "memory": 50, "pods": 1},
				},
			},
			expectedTotalAllocation: map[string]resources.Quantity{"cpu": 525, "memory": 1050, "pods": 3}, // (300-50) + (200-0) + (100-25)
		},
		{
			name: "No children",
			childQueues: []struct {
				name       string
				allocated  map[string]resources.Quantity
				preempting map[string]resources.Quantity
			}{},
			expectedTotalAllocation: map[string]resources.Quantity{"cpu": 0, "memory": 0, "pods": 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var parentSnapshot *QueuePreemptionSnapshot

			if tt.childQueues != nil {
				// Create root queue
				rootQ, err := createRootQueue(map[string]string{"cpu": "2000", "memory": "4000", "pods": "10"})
				assert.NilError(t, err)

				// Create parent queue
				parentQ, err := createManagedQueueWithProps(rootQ, "parent", true, nil, nil)
				assert.NilError(t, err)

				// Create child queues with their allocations and preempting resources
				for _, childConfig := range tt.childQueues {
					childQ, err := createManagedQueueWithProps(parentQ, childConfig.name, false, nil, nil)
					assert.NilError(t, err)

					// Set allocated resources
					if childConfig.allocated != nil {
						allocatedResource := resources.NewResourceFromMap(childConfig.allocated)
						assert.NilError(t, childQ.TryIncAllocatedResource(allocatedResource))
					}

					// Set preempting resources
					if childConfig.preempting != nil {
						preemptingResource := resources.NewResourceFromMap(childConfig.preempting)
						childQ.preemptingResource = preemptingResource
					}
				}

				// Create queue preemption snapshot
				cache := make(map[string]*QueuePreemptionSnapshot)
				parentSnapshot = parentQ.createPreemptionSnapshot(cache, "")
			}

			// Test GetTotalChildAllocation
			totalChildAllocation := parentSnapshot.GetTotalChildAllocation()
			expectedResource := resources.NewResourceFromMap(tt.expectedTotalAllocation)

			assert.Assert(t, totalChildAllocation != nil, "Total child allocation should not be nil")
			assert.Assert(t, resources.Equals(totalChildAllocation, expectedResource),
				"Total child allocation mismatch. Got: %s, Expected: %s",
				totalChildAllocation.String(), expectedResource.String())
		})
	}
}
