package dag

import (
	"errors"
	"reflect"
	"testing"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

func TestTopologicalOrderIsDeterministic(t *testing.T) {
	nodes := []Node{
		{Key: "deploy", DependsOn: []string{"build", "test"}},
		{Key: "test", DependsOn: []string{"build"}},
		{Key: "build"},
		{Key: "docs"},
	}

	got, err := TopologicalOrder(nodes)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"build", "docs", "test", "deploy"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestTopologicalOrderRejectsCycle(t *testing.T) {
	_, err := TopologicalOrder([]Node{
		{Key: "a", DependsOn: []string{"b"}},
		{Key: "b", DependsOn: []string{"a"}},
	})
	if !errors.Is(err, domain.ErrCycleDetected) {
		t.Fatalf("error = %v, want cycle detected", err)
	}
}

func TestTopologicalOrderRejectsUnknownDependency(t *testing.T) {
	if _, err := TopologicalOrder([]Node{{Key: "a", DependsOn: []string{"missing"}}}); err == nil {
		t.Fatal("unknown dependency accepted")
	}
}
