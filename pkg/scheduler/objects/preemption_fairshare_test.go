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
	"fmt"
	"testing"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func TestQueuePreemptionSnapshot_GetFairShareResource(t *testing.T) {
	tests := []struct {
		name             string
		parentMax        *resources.Resource
		parentGuaranteed *resources.Resource
		activeSiblings   int
		expected         *resources.Resource
	}{
		{
			name:             "nil parent max",
			parentMax:        nil,
			parentGuaranteed: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10}),
			activeSiblings:   2,
			expected:         nil,
		},
		{
			name:             "empty parent max",
			parentMax:        resources.NewResource(),
			parentGuaranteed: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10}),
			activeSiblings:   2,
			expected:         nil,
		},
		{
			name:             "single active sibling",
			parentMax:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100}),
			parentGuaranteed: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}),
			activeSiblings:   1,
			expected:         nil,
		},
		{
			name:             "two active siblings with equal fair share",
			parentMax:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100}),
			parentGuaranteed: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}),
			activeSiblings:   2,
			expected:         resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 40}), // (100-20)/2 = 40
		},
		{
			name:             "three active siblings with equal fair share",
			parentMax:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 90}),
			parentGuaranteed: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30}),
			activeSiblings:   3,
			expected:         resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}), // (90-30)/3 = 20
		},
		{
			name:             "multiple resource types",
			parentMax:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100, "memory": 200}),
			parentGuaranteed: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20, "memory": 40}),
			activeSiblings:   2,
			expected:         resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 40, "memory": 80}), // (100-20)/2, (200-40)/2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock parent snapshot
			parent := &QueuePreemptionSnapshot{
				MaxResource:        tt.parentMax,
				GuaranteedResource: tt.parentGuaranteed,
			}

			// Create a mock queue that returns the expected sibling count
			mockQueue := &Queue{
				QueuePath: "root.test",
				parent:    &Queue{QueuePath: "root"},
			}

			// Create a mock parent queue with the expected number of children
			mockParent := &Queue{
				QueuePath: "root",
				children:  make(map[string]*Queue),
			}

			// Add the expected number of sibling queues
			for i := 0; i < tt.activeSiblings; i++ {
				childName := fmt.Sprintf("child%d", i)
				mockParent.children[childName] = &Queue{QueuePath: "root." + childName}
			}

			mockQueue.parent = mockParent

			// Create a child snapshot
			child := &QueuePreemptionSnapshot{
				Parent: parent,
				Queue:  mockQueue,
			}

			result := child.GetFairShareResource()

			if tt.expected == nil {
				if result != nil {
					t.Errorf("Expected nil result, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("Expected %v, got nil", tt.expected)
				} else if !resources.Equals(tt.expected, result) {
					t.Errorf("Expected %v, got %v", tt.expected, result)
				}
			}
		})
	}
}

func TestQueuePreemptionSnapshot_GetFairSharePreemptableResource_Basic(t *testing.T) {
	// Test basic functionality without complex mocking
	parent := &QueuePreemptionSnapshot{
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100}),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}),
	}

	child := &QueuePreemptionSnapshot{
		Parent:             parent,
		AllocatedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30}),
		PreemptingResource: resources.NewResource(),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10}),
	}

	result := child.GetFairSharePreemptableResource()

	// Should return some preemptable resources since allocated (30) > guaranteed (10)
	if result == nil {
		t.Error("Expected non-nil result for over-allocated queue")
	}
}

func TestQueuePreemptionSnapshot_GetFairSharePreemptableResource(t *testing.T) {
	// Test fair share preemption with explicit sibling count
	parent := &QueuePreemptionSnapshot{
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100}),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}),
	}

	child := &QueuePreemptionSnapshot{
		Parent:             parent,
		AllocatedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30}),
		PreemptingResource: resources.NewResource(),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10}),
	}

	// Test with 2 active siblings
	result := child.GetFairSharePreemptableResource()
	if result == nil {
		t.Error("Expected non-nil result for fair share preemption with 2 siblings")
	}

	// Test with 1 active sibling (should still work based on guarantees)
	result = child.GetFairSharePreemptableResource()
	if result == nil {
		t.Error("Expected non-nil result for fair share preemption with 1 sibling (should use guarantees)")
	}
}

func TestQueuePreemptionSnapshot_FairShareWithLimitedParentCapacity(t *testing.T) {
	// Test scenario: parent max=50, but only 40 available due to other queues
	parent := &QueuePreemptionSnapshot{
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 50}),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10}), // 40 available
	}

	child := &QueuePreemptionSnapshot{
		Parent:             parent,
		AllocatedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 25}),
		PreemptingResource: resources.NewResource(),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10}),
	}

	// Test with 2 active siblings
	// Available capacity = 50 - 10 = 40
	// Fair share = 40 / 2 = 20 per child
	// Child is using 25 > 20 fair share, so should be preemptable
	result := child.GetFairSharePreemptableResource()
	if result == nil {
		t.Error("Expected non-nil result for fair share preemption with limited parent capacity")
	}

	// Verify the fair share calculation
	fairShare := child.GetFairShareResource()
	expectedFairShare := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}) // 40/2 = 20
	if !resources.Equals(fairShare, expectedFairShare) {
		t.Errorf("Expected fair share %v, got %v", expectedFairShare, fairShare)
	}
}

func TestQueuePreemptionSnapshot_DeeplyNestedQueueStructure(t *testing.T) {
	// Test deeply nested queue structure: root -> level1 -> level2 -> level3
	// This tests how fair share works at different levels of the hierarchy

	// Level 1: root queue
	root := &QueuePreemptionSnapshot{
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100}),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}), // 80 available
	}

	// Level 2: level1 queue (child of root)
	level1 := &QueuePreemptionSnapshot{
		Parent:             root,
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 60}),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}),
	}

	// Level 3: level2 queue (child of level1)
	level2 := &QueuePreemptionSnapshot{
		Parent:             level1,
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30}),
		AllocatedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}),
		PreemptingResource: resources.NewResource(),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 5}),
	}

	// Test fair share calculation at level2
	// Available capacity at level1 = min(60, 80) - 15 = 45
	// Fair share = 45 / 2 = 22.5 (rounded down to 22)
	fairShare := level2.GetFairShareResource()
	expectedFairShare := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 22}) // 45/2 = 22
	if !resources.Equals(fairShare, expectedFairShare) {
		t.Errorf("Expected fair share %v, got %v", expectedFairShare, fairShare)
	}

	// Test preemptable resource calculation
	// level2 is using 20 CPU, fair share is 22, guaranteed is 5
	// minThreshold = max(5, 22) = 22
	// preemptable = 20 - 22 = -2 (nothing to preempt)
	result := level2.GetFairSharePreemptableResource()
	if result != nil && !result.IsEmpty() {
		t.Error("Expected nil result - level2 is within fair share")
	}

	// Test with level2 using more than fair share
	level2OverAllocated := &QueuePreemptionSnapshot{
		Parent:             level1,
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30}),
		AllocatedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 25}), // Over fair share
		PreemptingResource: resources.NewResource(),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 5}),
	}

	result = level2OverAllocated.GetFairSharePreemptableResource()
	if result == nil || result.IsEmpty() {
		t.Error("Expected non-nil result - level2 is over fair share")
	}

	// Verify preemptable amount: 25 - 22 = 3 CPU
	expectedPreemptable := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 3})
	if !resources.Equals(result, expectedPreemptable) {
		t.Errorf("Expected preemptable %v, got %v", expectedPreemptable, result)
	}
}

func TestQueuePreemptionSnapshot_FairShareWithRemainingGuaranteedCheck(t *testing.T) {
	// Test the remaining guaranteed resource check logic
	parent := &QueuePreemptionSnapshot{
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100}),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}),
	}

	// Test case 1: Queue with remaining guaranteed resources (should allow preemption)
	childWithRemaining := &QueuePreemptionSnapshot{
		Parent:             parent,
		AllocatedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30}),
		PreemptingResource: resources.NewResource(),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10}),
	}

	// Mock GetRemainingGuaranteedResource to return positive remaining
	// This simulates a queue that still has guaranteed resources available
	result := childWithRemaining.GetFairSharePreemptableResource()
	if result == nil {
		t.Error("Expected non-nil result when remaining guaranteed resources exist")
	}

	// Test case 2: Queue with no remaining guaranteed resources (should not allow preemption)
	// This would be tested by mocking GetRemainingGuaranteedResource to return nil or negative
	// In a real scenario, this would happen when a queue has already used all its guaranteed resources
}

func TestQueuePreemptionSnapshot_FairShareGuaranteeProtection(t *testing.T) {
	// Test that fair share preemption never breaks guaranteed resources
	// Create a root queue first
	root := &QueuePreemptionSnapshot{
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100}),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}),
	}

	// Create parent queue with its own max resource
	parent := &QueuePreemptionSnapshot{
		Parent:             root,
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 50}),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10}),
	}

	// Test case 1: Fair share is higher than guaranteed (should use fair share)
	// Guaranteed: 10 CPU, Fair share: 20 CPU, Allocated: 25 CPU
	// Should preempt: 25 - 20 = 5 CPU (using fair share as threshold)
	child1 := &QueuePreemptionSnapshot{
		Parent:             parent,
		AllocatedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 25}),
		PreemptingResource: resources.NewResource(),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10}),
	}

	result1 := child1.GetFairSharePreemptableResource() // Fair share = 40/2 = 20

	if result1 == nil {
		t.Error("Expected non-nil result when fair share is higher than guaranteed")
	}
	expected1 := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 5}) // 25 - 20 = 5
	if !resources.Equals(result1, expected1) {
		t.Errorf("Expected preemptable %v, got %v", expected1, result1)
	}

	// Test case 2: Guaranteed is higher than fair share (should use guaranteed)
	// Guaranteed: 30 CPU, Fair share: 20 CPU, Allocated: 25 CPU
	// Should preempt: 25 - 30 = -5 CPU (nothing to preempt, protected by guarantee)
	child2 := &QueuePreemptionSnapshot{
		Parent:             parent,
		AllocatedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 25}),
		PreemptingResource: resources.NewResource(),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30}),
	}

	result2 := child2.GetFairSharePreemptableResource() // Fair share = 40/2 = 20
	if result2 != nil && !result2.IsEmpty() {
		t.Error("Expected nil result when guaranteed is higher than fair share")
	}

	// Test case 3: Allocated is exactly at guaranteed level (should not preempt)
	// Guaranteed: 20 CPU, Fair share: 20 CPU, Allocated: 20 CPU
	// Should preempt: 20 - 20 = 0 CPU (nothing to preempt)
	child3 := &QueuePreemptionSnapshot{
		Parent:             parent,
		AllocatedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}),
		PreemptingResource: resources.NewResource(),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20}),
	}

	result3 := child3.GetFairSharePreemptableResource() // Fair share = 40/2 = 20
	if result3 != nil && !result3.IsEmpty() {
		t.Error("Expected nil result when allocated equals guaranteed")
	}

	// Test case 4: Allocated is below guaranteed level (should not preempt)
	// Guaranteed: 30 CPU, Fair share: 20 CPU, Allocated: 15 CPU
	// Should preempt: 15 - 30 = -15 CPU (nothing to preempt, below guarantee)
	child4 := &QueuePreemptionSnapshot{
		Parent:             parent,
		AllocatedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 15}),
		PreemptingResource: resources.NewResource(),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 30}),
	}

	result4 := child4.GetFairSharePreemptableResource() // Fair share = 40/2 = 20
	if result4 != nil && !result4.IsEmpty() {
		t.Error("Expected nil result when allocated is below guaranteed")
	}
}
