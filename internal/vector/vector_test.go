package vector

import "testing"

func TestCompareEqual(t *testing.T) {
	a := Vector{"A": 1, "B": 2}
	b := Vector{"A": 1, "B": 2}
	if rel := a.Compare(b); rel != Equal {
		t.Fatalf("expected Equal, got %v", rel)
	}
}

func TestCompareEqualEmptyVsNil(t *testing.T) {
	var a Vector
	b := Vector{}
	if rel := a.Compare(b); rel != Equal {
		t.Fatalf("expected nil and empty vectors to compare Equal, got %v", rel)
	}
}

func TestCompareAncestorDescendant(t *testing.T) {
	ancestor := Vector{"A": 1, "B": 2}
	descendant := Vector{"A": 1, "B": 3}

	if rel := ancestor.Compare(descendant); rel != Ancestor {
		t.Fatalf("expected Ancestor, got %v", rel)
	}
	if rel := descendant.Compare(ancestor); rel != Descendant {
		t.Fatalf("expected Descendant, got %v", rel)
	}
}

func TestCompareConcurrent(t *testing.T) {
	a := Vector{"A": 2, "B": 1}
	b := Vector{"A": 1, "B": 2}
	if rel := a.Compare(b); rel != Concurrent {
		t.Fatalf("expected Concurrent, got %v", rel)
	}
	if rel := b.Compare(a); rel != Concurrent {
		t.Fatalf("expected Concurrent (symmetric), got %v", rel)
	}
}

func TestCompareDisjointNodes(t *testing.T) {
	// A only knows about node "A"; B only knows about node "B". Neither has
	// seen the other's writes, so this must be Concurrent, not Ancestor.
	a := Vector{"A": 1}
	b := Vector{"B": 1}
	if rel := a.Compare(b); rel != Concurrent {
		t.Fatalf("expected Concurrent for disjoint nodes, got %v", rel)
	}
}

func TestIncrementDoesNotMutateReceiver(t *testing.T) {
	a := Vector{"A": 1}
	b := a.Increment("A")

	if a["A"] != 1 {
		t.Fatalf("Increment must not mutate the receiver, got a[A]=%d", a["A"])
	}
	if b["A"] != 2 {
		t.Fatalf("expected incremented copy to have A=2, got %d", b["A"])
	}

	c := a.Increment("B")
	if _, ok := a["B"]; ok {
		t.Fatalf("Increment must not mutate the receiver with a new key")
	}
	if c["B"] != 1 {
		t.Fatalf("expected new node to start at 1, got %d", c["B"])
	}
}

func TestMergeIsComponentWiseMax(t *testing.T) {
	a := Vector{"A": 3, "B": 1}
	b := Vector{"A": 1, "B": 5, "C": 2}

	merged := Merge(a, b)
	want := Vector{"A": 3, "B": 5, "C": 2}

	if len(merged) != len(want) {
		t.Fatalf("merged vector has wrong size: got %v want %v", merged, want)
	}
	for node, count := range want {
		if merged[node] != count {
			t.Fatalf("merged[%s] = %d, want %d", node, merged[node], count)
		}
	}

	// Merge must dominate both inputs.
	if rel := merged.Compare(a); rel != Descendant && rel != Equal {
		t.Fatalf("merged vector must be a descendant of (or equal to) a, got %v", rel)
	}
	if rel := merged.Compare(b); rel != Descendant && rel != Equal {
		t.Fatalf("merged vector must be a descendant of (or equal to) b, got %v", rel)
	}
}

func TestMergeDoesNotMutateInputs(t *testing.T) {
	a := Vector{"A": 1}
	b := Vector{"B": 1}
	_ = Merge(a, b)

	if _, ok := a["B"]; ok {
		t.Fatalf("Merge must not mutate its inputs")
	}
	if _, ok := b["A"]; ok {
		t.Fatalf("Merge must not mutate its inputs")
	}
}
