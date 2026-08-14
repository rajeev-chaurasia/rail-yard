"use strict";

const basePath = document.querySelector('meta[name="ops-base-path"]').content;
const csrfToken = document.querySelector('meta[name="csrf-token"]').content;
const configuredPoll = Number(document.querySelector('meta[name="poll-interval-ms"]').content);
const pollInterval = Math.min(60000, Math.max(2000, configuredPoll || 5000));
const maxConsecutiveFailures = 5;
const requestTimeout = 10000;

const elements = {
  actor: document.getElementById("actor"),
  reason: document.getElementById("reason"),
  forceAction: document.getElementById("force-action"),
  connection: document.getElementById("connection-status"),
  lastUpdated: document.getElementById("last-updated"),
  refresh: document.getElementById("refresh"),
  queueTotal: document.getElementById("queue-total"),
  runningTotal: document.getElementById("running-total"),
  failedTotal: document.getElementById("failed-total"),
  workerTotal: document.getElementById("worker-total"),
  queueBody: document.getElementById("queue-body"),
  queueEmpty: document.getElementById("queue-empty"),
  runningBody: document.getElementById("running-body"),
  runningEmpty: document.getElementById("running-empty"),
  failedBody: document.getElementById("failed-body"),
  failedEmpty: document.getElementById("failed-empty"),
  workerBody: document.getElementById("worker-body"),
  workerEmpty: document.getElementById("worker-empty"),
  dlqBody: document.getElementById("dlq-body"),
  dlqEmpty: document.getElementById("dlq-empty"),
  runForm: document.getElementById("run-form"),
  runID: document.getElementById("run-id"),
  runStatus: document.getElementById("run-status"),
  dag: document.getElementById("dag"),
  toast: document.getElementById("toast"),
};

const polling = {
  timer: 0,
  inFlight: false,
  failures: 0,
  stopped: false,
};

function makeElement(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined && text !== null) {
    node.textContent = String(text);
  }
  if (className) {
    node.className = className;
  }
  return node;
}

function clear(node) {
  while (node.firstChild) {
    node.removeChild(node.firstChild);
  }
}

function appendCell(row, value, className) {
  const cell = makeElement("td", value, className);
  row.appendChild(cell);
  return cell;
}

function displayName(job) {
  return job.name ? `${job.name} (${job.id})` : job.id;
}

function absoluteTime(value) {
  if (!value) {
    return "None";
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "Unknown" : date.toLocaleString();
}

function age(value) {
  if (!value) {
    return "None";
  }
  const milliseconds = Date.now() - new Date(value).getTime();
  if (!Number.isFinite(milliseconds)) {
    return "Unknown";
  }
  const seconds = Math.max(0, Math.floor(milliseconds / 1000));
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ${seconds % 60}s`;
  }
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

function timeUntil(value) {
  if (!value) {
    return "None";
  }
  const milliseconds = new Date(value).getTime() - Date.now();
  if (!Number.isFinite(milliseconds)) {
    return "Unknown";
  }
  if (milliseconds <= 0) {
    return "Expired";
  }
  return `in ${Math.ceil(milliseconds / 1000)}s`;
}

async function fetchJSON(path, options = {}) {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), requestTimeout);
  try {
    const response = await fetch(`${basePath}${path}`, {
      ...options,
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
        ...(options.headers || {}),
      },
      signal: controller.signal,
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    if (!response.ok) {
      const message = payload?.error?.message || `Request failed with HTTP ${response.status}`;
      throw new Error(message);
    }
    return payload;
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error("Request timed out");
    }
    throw error;
  } finally {
    window.clearTimeout(timeout);
  }
}

function actionButton(action, jobID, label, className) {
  const button = makeElement("button", label, className);
  button.type = "button";
  button.addEventListener("click", () => performAction(action, jobID, button));
  return button;
}

function renderQueues(depths) {
  clear(elements.queueBody);
  const rows = [...depths].sort((left, right) =>
    `${left.tenant_id}/${left.queue}/${left.state}`.localeCompare(
      `${right.tenant_id}/${right.queue}/${right.state}`,
    ),
  );
  for (const depth of rows) {
    const row = document.createElement("tr");
    appendCell(row, depth.tenant_id);
    appendCell(row, depth.queue);
    const stateCell = appendCell(row, "");
    stateCell.appendChild(makeElement("span", depth.state, "badge"));
    appendCell(row, depth.depth);
    elements.queueBody.appendChild(row);
  }
  elements.queueEmpty.hidden = rows.length !== 0;
  elements.queueTotal.textContent = String(
    rows.reduce((total, item) => total + Number(item.depth || 0), 0),
  );
}

function renderRunning(jobs) {
  clear(elements.runningBody);
  for (const job of jobs) {
    const row = document.createElement("tr");
    const jobCell = appendCell(row, "");
    jobCell.appendChild(makeElement("code", displayName(job)));
    appendCell(row, `${job.tenant_id}/${job.queue}`);
    appendCell(row, job.attempt_no);
    appendCell(row, absoluteTime(job.updated_at));
    const actions = appendCell(row, "", "actions");
    actions.appendChild(actionButton("cancel", job.id, "Cancel", "danger"));
    actions.appendChild(actionButton("force", job.id, "Force", "force"));
    elements.runningBody.appendChild(row);
  }
  elements.runningEmpty.hidden = jobs.length !== 0;
  elements.runningTotal.textContent = String(jobs.length);
}

function renderFailed(jobs) {
  clear(elements.failedBody);
  for (const job of jobs) {
    const row = document.createElement("tr");
    const jobCell = appendCell(row, "");
    jobCell.appendChild(makeElement("code", displayName(job)));
    appendCell(row, job.failure?.message || job.failure?.class || "No failure detail");
    appendCell(row, absoluteTime(job.updated_at));
    const actions = appendCell(row, "", "actions");
    actions.appendChild(actionButton("retry", job.id, "Retry"));
    actions.appendChild(actionButton("force", job.id, "Force", "force"));
    elements.failedBody.appendChild(row);
  }
  elements.failedEmpty.hidden = jobs.length !== 0;
  elements.failedTotal.textContent = String(jobs.length);
}

function renderWorkers(workers) {
  clear(elements.workerBody);
  let healthy = 0;
  for (const worker of workers) {
    const row = document.createElement("tr");
    const workerCell = appendCell(row, "");
    workerCell.appendChild(makeElement("code", worker.id));
    const healthCell = appendCell(row, "");
    const healthClass = worker.healthy ? "badge good" : "badge bad";
    healthCell.appendChild(makeElement("span", worker.healthy ? "Healthy" : "Unhealthy", healthClass));
    if (worker.healthy) {
      healthy += 1;
    }
    appendCell(row, `${worker.used_slots} / ${worker.capacity}`);
    appendCell(row, worker.active_leases);
    appendCell(row, `${age(worker.last_heartbeat_at)} ago`);
    appendCell(row, age(worker.oldest_lease_started_at));
    appendCell(row, timeUntil(worker.nearest_lease_expiry));
    elements.workerBody.appendChild(row);
  }
  elements.workerEmpty.hidden = workers.length !== 0;
  elements.workerTotal.textContent = `${healthy} / ${workers.length}`;
}

function renderDeadLetters(deadLetters) {
  clear(elements.dlqBody);
  for (const item of deadLetters) {
    const row = document.createElement("tr");
    const jobCell = appendCell(row, "");
    jobCell.appendChild(makeElement("code", item.job_id));
    appendCell(row, item.failure?.class || "Unknown");
    appendCell(row, item.failure?.message || "No failure detail");
    appendCell(row, absoluteTime(item.created_at));
    const actions = appendCell(row, "", "actions");
    if (item.redriven_job_id) {
      actions.appendChild(makeElement("span", `Redriven as ${item.redriven_job_id}`, "muted"));
    } else {
      actions.appendChild(actionButton("redrive", item.job_id, "Retry"));
    }
    elements.dlqBody.appendChild(row);
  }
  elements.dlqEmpty.hidden = deadLetters.length !== 0;
}

function renderSnapshot(snapshot) {
  renderQueues(snapshot.queue_depths || []);
  renderRunning(snapshot.running_jobs || []);
  renderFailed(snapshot.failed_jobs || []);
  renderWorkers(snapshot.workers || []);
  elements.lastUpdated.textContent = `Updated ${absoluteTime(snapshot.generated_at || new Date())}`;
}

function setConnection(message, failed) {
  elements.connection.textContent = message;
  elements.connection.classList.toggle("error", failed);
}

function schedulePoll() {
  window.clearTimeout(polling.timer);
  if (polling.stopped || document.hidden) {
    return;
  }
  polling.timer = window.setTimeout(refreshDashboard, pollInterval);
}

async function refreshDashboard() {
  if (polling.inFlight || document.hidden) {
    schedulePoll();
    return;
  }
  polling.inFlight = true;
  elements.refresh.disabled = true;
  const results = await Promise.allSettled([
    fetchJSON("api/snapshot"),
    fetchJSON("api/dead-letters"),
  ]);

  const failures = [];
  if (results[0].status === "fulfilled") {
    renderSnapshot(results[0].value);
  } else {
    failures.push(`state: ${results[0].reason.message}`);
  }
  if (results[1].status === "fulfilled") {
    renderDeadLetters(results[1].value.dead_letters || []);
  } else {
    failures.push(`dead letters: ${results[1].reason.message}`);
  }

  if (failures.length === 0) {
    polling.failures = 0;
    setConnection("Live", false);
  } else {
    polling.failures += 1;
    setConnection(`Refresh failed, ${failures.join("; ")}`, true);
  }
  if (polling.failures >= maxConsecutiveFailures) {
    polling.stopped = true;
    setConnection("Auto-refresh stopped after repeated failures. Use Refresh now to retry.", true);
  }
  polling.inFlight = false;
  elements.refresh.disabled = false;
  schedulePoll();
}

function actor() {
  const value = elements.actor.value.trim();
  if (!value) {
    elements.actor.focus();
    throw new Error("Enter an operator identity before changing a job");
  }
  return value;
}

async function performAction(action, jobID, button) {
  let operator;
  try {
    operator = actor();
  } catch (error) {
    showToast(error.message, true);
    return;
  }
  const reason = elements.reason.value.trim();
  if ((action === "cancel" || action === "force") && !reason) {
    elements.reason.focus();
    showToast("Enter a change reason before canceling or forcing a job", true);
    return;
  }
  const forceAction = action === "force" ? elements.forceAction.value : "";
  if (
    action === "force" &&
    !window.confirm(`Force ${forceAction.replace("_", " ")} for job ${jobID}?`)
  ) {
    return;
  }
  button.disabled = true;
  try {
    const request = {
      action,
      job_id: jobID,
      actor: operator,
    };
    if (action === "cancel" || action === "force") {
      request.reason = reason;
    }
    if (action === "force") {
      request.force_action = forceAction;
    }
    const result = await fetchJSON("api/actions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": csrfToken,
      },
      body: JSON.stringify(request),
    });
    showToast(result.message || `${action} accepted for ${jobID}`, false);
    polling.stopped = false;
    polling.failures = 0;
    await refreshDashboard();
  } catch (error) {
    showToast(`${action} failed: ${error.message}`, true);
  } finally {
    button.disabled = false;
  }
}

function renderRun(run) {
  clear(elements.dag);
  for (const node of run.nodes || []) {
    const failed = node.state === "FAILED" || node.state === "DEAD_LETTER";
    const item = makeElement("li", null, failed ? "dag-node failed" : "dag-node");
    item.appendChild(makeElement("h3", node.name || node.id));
    item.appendChild(makeElement("p", node.id, "muted"));
    item.appendChild(makeElement("p", `State: ${node.state}, attempt ${node.attempt_no}`));
    const dependencies = node.depends_on?.length ? node.depends_on.join(", ") : "root";
    item.appendChild(makeElement("p", `Depends on: ${dependencies}`));
    item.appendChild(makeElement("p", `Updated: ${absoluteTime(node.updated_at)}`, "muted"));
    elements.dag.appendChild(item);
  }
  const count = run.nodes?.length || 0;
  elements.runStatus.textContent = count === 0
    ? `Run ${run.id} has no nodes.`
    : `Run ${run.id}, ${count} node${count === 1 ? "" : "s"}.`;
}

async function loadRun(event) {
  event.preventDefault();
  const runID = elements.runID.value.trim();
  if (!runID) {
    elements.runStatus.textContent = "Enter a run ID.";
    return;
  }
  elements.runStatus.textContent = `Loading run ${runID}`;
  try {
    const run = await fetchJSON(`api/runs/${encodeURIComponent(runID)}`);
    renderRun(run);
  } catch (error) {
    clear(elements.dag);
    elements.runStatus.textContent = `Could not load run: ${error.message}`;
  }
}

let toastTimer = 0;
function showToast(message, failed) {
  window.clearTimeout(toastTimer);
  elements.toast.textContent = message;
  elements.toast.classList.toggle("error", failed);
  elements.toast.hidden = false;
  toastTimer = window.setTimeout(() => {
    elements.toast.hidden = true;
  }, 6000);
}

elements.refresh.addEventListener("click", () => {
  polling.stopped = false;
  polling.failures = 0;
  refreshDashboard();
});
elements.runForm.addEventListener("submit", loadRun);
elements.actor.addEventListener("change", () => {
  try {
    window.localStorage.setItem("railyard.ops.actor", elements.actor.value.trim());
  } catch {
    return;
  }
});
document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    window.clearTimeout(polling.timer);
  } else {
    polling.stopped = false;
    refreshDashboard();
  }
});

try {
  elements.actor.value = window.localStorage.getItem("railyard.ops.actor") || "";
} catch {
  elements.actor.value = "";
}

refreshDashboard();
