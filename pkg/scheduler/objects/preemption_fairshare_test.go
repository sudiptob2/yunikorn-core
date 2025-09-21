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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

// TestGetFairShareResource_BaseCase tests the base case (root queue) fair share calculation
func TestGetFairShareResource_BaseCase(t *testing.T) {
	// Create a simple hierarchy: root -> parent -> child1, child2
	rootQ, err := createRootQueue(map[string]string{"cpu": "100", "memory": "1000"})
	assert.NilError(t, err)

	parentQ, err := createManagedQueue(rootQ, "parent", true, map[string]string{"cpu": "80", "memory": "800"})
	assert.NilError(t, err)

	childQ1, err := createManagedQueue(parentQ, "child1", false, map[string]string{"cpu": "40", "memory": "400"})
	assert.NilError(t, err)

	childQ2, err := createManagedQueue(parentQ, "child2", false, map[string]string{"cpu": "40", "memory": "400"})
	assert.NilError(t, err)

	// Test 1: No allocations - should return nil
	cache := make(map[string]*QueuePreemptionSnapshot)
	snapshot := rootQ.createPreemptionSnapshot(cache, "")
	fairShare := snapshot.GetFairShareResource()
	// Root queue should have fair share even with no allocations if it has max resources
	assert.Assert(t, fairShare == nil, "Expected nil fair share when no allocations and no max resources")

	// Test 2: Single active child - should get all resources
	// Set up root allocation (total cluster usage)
	rootQ.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20, "memory": 200})
	// Set up parent allocation so it can calculate fair share
	parentQ.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20, "memory": 200})
	childQ1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20, "memory": 200})
	cache = make(map[string]*QueuePreemptionSnapshot)
	snapshot = childQ1.createPreemptionSnapshot(cache, "")

	fairShare = snapshot.GetFairShareResource()
	expected := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20, "memory": 200})

	assert.Assert(t, resources.Equals(fairShare, expected), "Expected fair share to equal total allocation when only one active child")

	// Test 3: Two active children - should split equally
	childQ2.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30, "memory": 300})
	// Update root allocation to include both children
	rootQ.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 50, "memory": 500}) // 20+30
	// Update parent allocation to include both children
	parentQ.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 50, "memory": 500}) // 20+30
	cache = make(map[string]*QueuePreemptionSnapshot)
	snapshot = childQ1.createPreemptionSnapshot(cache, "")
	fairShare = snapshot.GetFairShareResource()
	expected = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 25, "memory": 250}) // (20+30)/2
	assert.Assert(t, resources.Equals(fairShare, expected), "Expected fair share to be split equally between active children")

	// Test 4: With guaranteed resources - should respect minimum guaranteed
	childQ1.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30, "memory": 300})
	cache = make(map[string]*QueuePreemptionSnapshot)
	snapshot = childQ1.createPreemptionSnapshot(cache, "")
	fairShare = snapshot.GetFairShareResource()
	expected = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30, "memory": 300}) // max(25, 30)
	assert.Assert(t, resources.Equals(fairShare, expected), "Expected fair share to respect guaranteed minimum")

	// Test 5: With max resources - should respect maximum limit
	childQ1.maxResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 35, "memory": 350})
	childQ1.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 40, "memory": 400}) // higher than max
	cache = make(map[string]*QueuePreemptionSnapshot)
	snapshot = childQ1.createPreemptionSnapshot(cache, "")
	fairShare = snapshot.GetFairShareResource()
	expected = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 35, "memory": 350}) // min(40, 35)
	assert.Assert(t, resources.Equals(fairShare, expected), "Expected fair share to respect maximum limit")
}

// TestGetFairShareResource_RecursiveCase tests the recursive case (non-root queue) fair share calculation
func TestGetFairShareResource_RecursiveCase(t *testing.T) {
	// Create hierarchy: root -> parent (fair share) -> child1, child2 (fair share)
	rootQ, err := createRootQueue(map[string]string{"cpu": "100", "memory": "1000"})
	assert.NilError(t, err)

	parentQ, err := createManagedQueueWithProps(rootQ, "parent", true, map[string]string{"cpu": "80", "memory": "800"}, map[string]string{"preemption.policy": "fairshare"})
	assert.NilError(t, err)

	childQ1, err := createManagedQueueWithProps(parentQ, "child1", false, map[string]string{"cpu": "40", "memory": "400"}, map[string]string{"preemption.policy": "fairshare"})
	assert.NilError(t, err)

	childQ2, err := createManagedQueueWithProps(parentQ, "child2", false, map[string]string{"cpu": "40", "memory": "400"}, map[string]string{"preemption.policy": "fairshare"})
	assert.NilError(t, err)

	// Set up allocations
	rootQ.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 50, "memory": 500})   // 20+30
	parentQ.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 50, "memory": 500}) // 20+30
	childQ1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20, "memory": 200})
	childQ2.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30, "memory": 300})

	// Test 1: Child1 fair share calculation
	cache := make(map[string]*QueuePreemptionSnapshot)
	snapshot := childQ1.createPreemptionSnapshot(cache, "")

	fairShare := snapshot.GetFairShareResource()

	// Parent's fair share should be 50 CPU, 500 memory (total allocation)
	// Child1's fair share should be 50/2 = 25 CPU, 500/2 = 250 memory
	expected := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 25, "memory": 250})
	assert.Assert(t, resources.Equals(fairShare, expected), "Expected child1 fair share to be half of parent's fair share")

	// Test 2: Child2 fair share calculation
	cache = make(map[string]*QueuePreemptionSnapshot)
	snapshot = childQ2.createPreemptionSnapshot(cache, "")
	fairShare = snapshot.GetFairShareResource()
	expected = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 25, "memory": 250}) // same as child1
	assert.Assert(t, resources.Equals(fairShare, expected), "Expected child2 fair share to be half of parent's fair share")

	// Test 3: With guaranteed resources
	childQ1.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 15, "memory": 150})
	cache = make(map[string]*QueuePreemptionSnapshot)
	snapshot = childQ1.createPreemptionSnapshot(cache, "")
	fairShare = snapshot.GetFairShareResource()
	expected = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 25, "memory": 250}) // max(25, 15) = 25
	assert.Assert(t, resources.Equals(fairShare, expected), "Expected fair share to respect guaranteed minimum")
}

// TestGetFairShareResource_MixedHierarchy tests fair share calculation in mixed hierarchy
func TestGetFairShareResource_MixedHierarchy(t *testing.T) {
	// Create hierarchy: root -> parent (no fair share) -> child1 (fair share), child2 (fair share)
	rootQ, err := createRootQueue(map[string]string{"cpu": "100", "memory": "1000"})
	assert.NilError(t, err)

	parentQ, err := createManagedQueue(rootQ, "parent", true, map[string]string{"cpu": "80", "memory": "800"})
	assert.NilError(t, err)

	childQ1, err := createManagedQueueWithProps(parentQ, "child1", false, map[string]string{"cpu": "40", "memory": "400"}, map[string]string{"preemption.policy": "fairshare"})
	assert.NilError(t, err)

	childQ2, err := createManagedQueueWithProps(parentQ, "child2", false, map[string]string{"cpu": "40", "memory": "400"}, map[string]string{"preemption.policy": "fairshare"})
	assert.NilError(t, err)

	// Set up allocations
	rootQ.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 50, "memory": 500})   // 20+30
	parentQ.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 50, "memory": 500}) // 20+30
	childQ1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20, "memory": 200})
	childQ2.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30, "memory": 300})

	// Test: Both children should now use recursive case (inherit from parent)
	// Parent's fair share should be 50 CPU, 500 memory (total allocation)
	// Each child's fair share should be 50/2 = 25 CPU, 500/2 = 250 memory

	cache := make(map[string]*QueuePreemptionSnapshot)
	snapshot := childQ1.createPreemptionSnapshot(cache, "")
	fairShare := snapshot.GetFairShareResource()
	expected := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 25, "memory": 250})
	assert.Assert(t, resources.Equals(fairShare, expected), "Expected child1 to use recursive case and inherit from parent")

	cache = make(map[string]*QueuePreemptionSnapshot)
	snapshot = childQ2.createPreemptionSnapshot(cache, "")
	fairShare = snapshot.GetFairShareResource()
	expected = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 25, "memory": 250})
	assert.Assert(t, resources.Equals(fairShare, expected), "Expected child2 to use recursive case and inherit from parent")
}

// TestGetFairShareResource_EdgeCases tests edge cases
func TestGetFairShareResource_EdgeCases(t *testing.T) {
	// Test 1: Nil queue snapshot
	var snapshot *QueuePreemptionSnapshot
	fairShare := snapshot.GetFairShareResource()
	assert.Assert(t, fairShare == nil, "Expected nil fair share for nil snapshot")

	// Test 2: Empty resource types
	rootQ, err := createRootQueue(map[string]string{})
	assert.NilError(t, err)
	cache := make(map[string]*QueuePreemptionSnapshot)
	snapshot = rootQ.createPreemptionSnapshot(cache, "")
	fairShare = snapshot.GetFairShareResource()
	assert.Assert(t, fairShare == nil, "Expected nil fair share for empty resource types")

	// Test 3: Zero allocations
	rootQ, err = createRootQueue(map[string]string{"cpu": "100"})
	assert.NilError(t, err)
	childQ, err := createManagedQueue(rootQ, "child", false, map[string]string{"cpu": "50"})
	assert.NilError(t, err)
	// No allocations set
	cache = make(map[string]*QueuePreemptionSnapshot)
	snapshot = rootQ.createPreemptionSnapshot(cache, "")
	fairShare = snapshot.GetFairShareResource()
	assert.Assert(t, fairShare == nil, "Expected nil fair share for zero allocations")

	// Test 4: Single queue with no siblings
	childQ.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20})
	cache = make(map[string]*QueuePreemptionSnapshot)
	snapshot = rootQ.createPreemptionSnapshot(cache, "")
	fairShare = snapshot.GetFairShareResource()
	expected := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20})
	assert.Assert(t, resources.Equals(fairShare, expected), "Expected fair share to equal allocation for single queue")
}
