package server_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenAPICoversMountedRoutes(t *testing.T) {
	body := readOpenAPI(t)
	for _, path := range []string{
		"/metrics",
		"/ops",
		"/ops/",
		"/ops/assets/app.css",
		"/ops/assets/app.js",
		"/ops/api/snapshot",
		"/ops/api/dead-letters",
		"/ops/api/runs/{run_id}",
		"/ops/api/actions",
		"/health/live",
		"/health/ready",
		"/v1/jobs",
		"/v1/jobs/{job_id}",
		"/v1/workflows",
		"/v1/dead-letters",
		"/v1/dead-letters/{job_id}/redrive",
		"/v1/triggers/cron",
		"/v1/workers/register",
		"/v1/workers/{worker_id}/leases/acquire",
		"/v1/workers/{worker_id}/heartbeats",
		"/v1/workers/{worker_id}/attempts/start",
		"/v1/workers/{worker_id}/attempts/start-batch",
		"/v1/workers/{worker_id}/attempts/complete",
		"/v1/workers/{worker_id}/attempts/complete-batch",
		"/v1/operations/jobs",
		"/v1/operations/dags",
		"/v1/operations/jobs/{job_id}",
		"/v1/operations/jobs/{job_id}/history",
		"/v1/operations/jobs/{job_id}/cancel",
		"/v1/operations/dead-letters/{job_id}/redrive",
		"/v1/operations/tenants/{tenant_id}/queues",
		"/v1/operations/workers",
		"/v1/operations/dags/{dag_id}",
		"/v1/operations/jobs/{job_id}/force",
		"/v1/operations/operator-actions",
		"/v1/operations/audit-events",
	} {
		if !strings.Contains(body, "  "+path+":\n") {
			t.Errorf("OpenAPI does not document mounted path %s", path)
		}
	}
}

func TestOpenAPIOperationsMutationsRequireActors(t *testing.T) {
	body := readOpenAPI(t)
	for _, path := range []string{
		"/v1/operations/jobs",
		"/v1/operations/dags",
		"/v1/operations/jobs/{job_id}/cancel",
		"/v1/operations/dead-letters/{job_id}/redrive",
		"/v1/operations/jobs/{job_id}/force",
		"/v1/operations/operator-actions",
	} {
		block := pathBlock(t, body, path)
		if !strings.Contains(block, `$ref: "#/components/parameters/Actor"`) {
			t.Errorf("OpenAPI path %s does not require the actor header", path)
		}
	}

	dashboardAction := schemaBlock(t, body, "DashboardActionRequest")
	if !strings.Contains(dashboardAction, "required: [action, job_id, actor]") {
		t.Error("dashboard action schema does not require actor")
	}
}

func TestOpenAPIBatchLimitsMatchServerAndStore(t *testing.T) {
	body := readOpenAPI(t)
	assertSchemaContains(t, body, "StartAttemptsRequest", "maxItems: 128")
	assertSchemaContains(t, body, "CompleteAttemptsRequest", "maxItems: 128")
	assertSchemaContains(t, body, "HeartbeatRequest", "maxItems: 1024")
	assertSchemaContains(t, body, "RegisterWorkerRequest", "maximum: 1024")

	acquire := schemaBlock(t, body, "AcquireLeasesRequest")
	if strings.Count(acquire, "maximum: 1024") != 2 {
		t.Errorf("acquire request must cap available_slots and limit at 1024:\n%s", acquire)
	}
	heartbeat := schemaBlock(t, body, "HeartbeatRequest")
	if strings.Contains(heartbeat, "minItems:") {
		t.Error("heartbeat schema rejects the empty lease batch accepted by the server")
	}
	assertSchemaContains(t, body, "Lease", "ready_at:")
}

func readOpenAPI(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "api", "openapi.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read OpenAPI contract: %v", err)
	}
	return strings.ReplaceAll(string(body), "\r\n", "\n")
}

func pathBlock(t *testing.T, body, path string) string {
	t.Helper()
	return indentedBlock(t, body, "  "+path+":\n", "\n  /")
}

func schemaBlock(t *testing.T, body, name string) string {
	t.Helper()
	marker := "    " + name + ":\n"
	start := strings.Index(body, marker)
	if start < 0 {
		t.Fatalf("contract block %q is missing", strings.TrimSpace(marker))
	}
	var block strings.Builder
	for _, line := range strings.Split(body[start+len(marker):], "\n") {
		if strings.HasPrefix(line, "    ") &&
			len(line) > 4 &&
			line[4] != ' ' {
			break
		}
		block.WriteString(line)
		block.WriteByte('\n')
	}
	return block.String()
}

func indentedBlock(t *testing.T, body, marker, nextMarker string) string {
	t.Helper()
	start := strings.Index(body, marker)
	if start < 0 {
		t.Fatalf("contract block %q is missing", strings.TrimSpace(marker))
	}
	remainder := body[start+len(marker):]
	end := strings.Index(remainder, nextMarker)
	if end < 0 {
		return remainder
	}
	return remainder[:end]
}

func assertSchemaContains(t *testing.T, body, schema, value string) {
	t.Helper()
	block := schemaBlock(t, body, schema)
	if !strings.Contains(block, value) {
		t.Errorf("%s does not contain %q:\n%s", schema, value, block)
	}
}
