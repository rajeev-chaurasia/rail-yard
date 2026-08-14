package dag

import (
	"fmt"
	"sort"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

type Node struct {
	Key       string
	DependsOn []string
}

func TopologicalOrder(nodes []Node) ([]string, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("workflow requires at least one node")
	}

	indegree := make(map[string]int, len(nodes))
	children := make(map[string][]string, len(nodes))
	for _, node := range nodes {
		if node.Key == "" {
			return nil, fmt.Errorf("workflow node key is required")
		}
		if _, exists := indegree[node.Key]; exists {
			return nil, fmt.Errorf("duplicate workflow node %q", node.Key)
		}
		indegree[node.Key] = 0
	}
	for _, node := range nodes {
		seen := make(map[string]struct{}, len(node.DependsOn))
		for _, dependency := range node.DependsOn {
			if _, exists := indegree[dependency]; !exists {
				return nil, fmt.Errorf("node %q depends on unknown node %q", node.Key, dependency)
			}
			if _, duplicate := seen[dependency]; duplicate {
				return nil, fmt.Errorf("node %q repeats dependency %q", node.Key, dependency)
			}
			seen[dependency] = struct{}{}
			indegree[node.Key]++
			children[dependency] = append(children[dependency], node.Key)
		}
	}

	ready := make([]string, 0)
	for key, degree := range indegree {
		if degree == 0 {
			ready = append(ready, key)
		}
	}
	sort.Strings(ready)

	order := make([]string, 0, len(nodes))
	for len(ready) > 0 {
		key := ready[0]
		ready = ready[1:]
		order = append(order, key)
		sort.Strings(children[key])
		for _, child := range children[key] {
			indegree[child]--
			if indegree[child] == 0 {
				ready = insertSorted(ready, child)
			}
		}
	}
	if len(order) != len(nodes) {
		return nil, domain.ErrCycleDetected
	}
	return order, nil
}

func insertSorted(values []string, value string) []string {
	index := sort.SearchStrings(values, value)
	values = append(values, "")
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}
