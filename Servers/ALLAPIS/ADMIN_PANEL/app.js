"use strict";

const state = {
  csrf: null,
  operator: null,
  expiresAt: null,
  currentPage: "review",
  caseOffset: 0,
  caseLimit: 50,
  cases: [],
  selectedCase: null,
  bugReports: [],
  bugGroups: [],
  selectedBugReportIds: new Set(),
  selectedTarget: null,
  actionContext: null,
  refreshing: false,
};

const elements = {
  loginView: document.getElementById("login-view"),
  appView: document.getElementById("app-view"),
  loginForm: document.getElementById("login-form"),
  loginButton: document.getElementById("login-button"),
  loginError: document.getElementById("login-error"),
  adminKey: document.getElementById("admin-key"),
  toggleSecret: document.getElementById("toggle-secret"),
  logoutButton: document.getElementById("logout-button"),
  operatorName: document.getElementById("operator-name"),
  sessionExpiry: document.getElementById("session-expiry"),
  queueCount: document.getElementById("queue-count"),
  stateFilter: document.getElementById("case-state-filter"),
  caseBody: document.getElementById("case-table-body"),
  queueStatus: document.getElementById("queue-status"),
  queueEmpty: document.getElementById("queue-empty"),
  metricOpen: document.getElementById("metric-open"),
  metricIndependent: document.getElementById("metric-independent"),
  metricReview: document.getElementById("metric-review"),
  metricControls: document.getElementById("metric-controls"),
  casesPrev: document.getElementById("cases-prev"),
  casesNext: document.getElementById("cases-next"),
  casesPageLabel: document.getElementById("cases-page-label"),
  bugReportCount: document.getElementById("bug-report-count"),
  bugReportBody: document.getElementById("bug-report-table-body"),
  bugReportEmpty: document.getElementById("bug-report-empty"),
  bugReportStatus: document.getElementById("bug-report-status"),
  bugReportSearch: document.getElementById("bug-report-search"),
  bugReportStatusFilter: document.getElementById("bug-report-status-filter"),
  bugReportGroupSelect: document.getElementById("bug-report-group-select"),
  selectAllBugReports: document.getElementById("select-all-bug-reports"),
  groupSelectedBugs: document.getElementById("group-selected-bugs"),
  bugReportDetail: document.getElementById("bug-report-detail"),
  detailDrawer: document.getElementById("detail-drawer"),
  drawerTitle: document.getElementById("drawer-title"),
  drawerContent: document.getElementById("drawer-content"),
  takeActionButton: document.getElementById("take-action-button"),
  searchForm: document.getElementById("search-form"),
  searchInput: document.getElementById("target-search"),
  searchStatus: document.getElementById("search-status"),
  searchResults: document.getElementById("search-results"),
  searchDetail: document.getElementById("search-detail"),
  maintenanceBadge: document.getElementById("maintenance-badge"),
  maintenanceSummary: document.getElementById("maintenance-summary"),
  maintenanceDelivery: document.getElementById("maintenance-delivery"),
  maintenanceForm: document.getElementById("maintenance-form"),
  maintenanceMinutes: document.getElementById("maintenance-minutes"),
  maintenanceReason: document.getElementById("maintenance-reason"),
  scheduleList: document.getElementById("schedule-list"),
  auditBody: document.getElementById("audit-table-body"),
  auditEmpty: document.getElementById("audit-empty"),
  auditActionFilter: document.getElementById("audit-action-filter"),
  auditTargetFilter: document.getElementById("audit-target-filter"),
  actionDialog: document.getElementById("action-dialog"),
  actionForm: document.getElementById("action-form"),
  actionTargetSummary: document.getElementById("action-target-summary"),
  actionSelect: document.getElementById("action-select"),
  actionEffect: document.getElementById("action-effect"),
  durationField: document.getElementById("duration-field"),
  actionDuration: document.getElementById("action-duration"),
  customDurationField: document.getElementById("custom-duration-field"),
  actionCustomDuration: document.getElementById("action-custom-duration"),
  actionReason: document.getElementById("action-reason"),
  confirmationField: document.getElementById("confirmation-field"),
  confirmationLabel: document.getElementById("confirmation-label"),
  actionConfirmation: document.getElementById("action-confirmation"),
  actionError: document.getElementById("action-error"),
  submitAction: document.getElementById("submit-action"),
  toastRegion: document.getElementById("toast-region"),
};

const MIN_TIMEOUT_SECONDS = 1;
const MAX_TIMEOUT_SECONDS = 10 * 365 * 24 * 60 * 60;
const TIMEOUT_UNIT_SECONDS = {
  s: 1,
  min: 60,
  h: 60 * 60,
  d: 24 * 60 * 60,
  w: 7 * 24 * 60 * 60,
  m: 30 * 24 * 60 * 60,
  mo: 30 * 24 * 60 * 60,
  y: 365 * 24 * 60 * 60,
};

function parseTimeoutDuration(value) {
  const match = String(value || "").trim().match(/^(\d+)\s*(min|mo|s|m|h|d|w|y)$/i);
  if (!match) {
    throw new Error("Use a whole number followed by s, min, h, d, w, m, mo, or y.");
  }
  const amount = Number(match[1]);
  const unit = match[2].toLowerCase();
  const seconds = amount * TIMEOUT_UNIT_SECONDS[unit];
  if (
    !Number.isSafeInteger(seconds)
    || seconds < MIN_TIMEOUT_SECONDS
    || seconds > MAX_TIMEOUT_SECONDS
  ) {
    throw new Error("Custom timeout must be between 1 second and 10 years.");
  }
  return seconds;
}

function selectedTimeoutSeconds() {
  if (elements.actionDuration.value === "custom") {
    return parseTimeoutDuration(elements.actionCustomDuration.value);
  }
  const seconds = Number(elements.actionDuration.value);
  if (!Number.isSafeInteger(seconds)) {
    throw new Error("Choose a valid timeout duration.");
  }
  return seconds;
}

function updateCustomDurationField() {
  const action = currentAction();
  const isCustom = Boolean(
    action?.temporary && elements.actionDuration.value === "custom"
  );
  elements.customDurationField.hidden = !isCustom;
  elements.actionCustomDuration.required = isCustom;
}

function text(value) {
  return document.createTextNode(value == null ? "—" : String(value));
}

function node(tag, options = {}, children = []) {
  const element = document.createElement(tag);
  if (options.className) element.className = options.className;
  if (options.title) element.title = options.title;
  if (options.type) element.type = options.type;
  if (options.dataset) {
    Object.entries(options.dataset).forEach(([key, value]) => {
      element.dataset[key] = String(value);
    });
  }
  const normalizedChildren = Array.isArray(children) ? children : [children];
  normalizedChildren.forEach((child) => {
    element.append(child instanceof Node ? child : text(child));
  });
  return element;
}

function formatDate(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function formatRelative(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const seconds = Math.round((date.getTime() - Date.now()) / 1000);
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  const units = [
    ["year", 31536000],
    ["month", 2592000],
    ["week", 604800],
    ["day", 86400],
    ["hour", 3600],
    ["minute", 60],
  ];
  for (const [unit, size] of units) {
    if (Math.abs(seconds) >= size || unit === "minute") {
      return formatter.format(Math.round(seconds / size), unit);
    }
  }
  return "just now";
}

function createStatusBadge(value) {
  const normalized = String(value || "neutral").toLowerCase();
  return node(
    "span",
    { className: `status-badge ${normalized}` },
    normalized.replaceAll("_", " ")
  );
}

function toast(message, type = "success") {
  const item = node("div", { className: `toast ${type}` }, message);
  elements.toastRegion.append(item);
  window.setTimeout(() => item.remove(), 4200);
}

async function api(path, options = {}) {
  const method = String(options.method || "GET").toUpperCase();
  const headers = new Headers(options.headers || {});
  headers.set("Accept", "application/json");
  if (options.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  if (!["GET", "HEAD", "OPTIONS"].includes(method) && state.csrf) {
    headers.set("X-RecRoom-CSRF-Token", state.csrf);
  }
  const response = await fetch(path, {
    ...options,
    method,
    headers,
    credentials: "same-origin",
    cache: "no-store",
  });
  let payload = null;
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    payload = await response.json();
  }
  if (!response.ok) {
    if (response.status === 401 && !path.endsWith("/auth/session")) {
      showLogin();
    }
    const error = new Error(payload?.detail || `Request failed (${response.status})`);
    error.status = response.status;
    throw error;
  }
  return payload;
}

function showLogin() {
  state.csrf = null;
  state.operator = null;
  state.expiresAt = null;
  elements.appView.hidden = true;
  elements.loginView.hidden = false;
  elements.adminKey.value = "";
  elements.adminKey.type = "password";
  elements.toggleSecret.textContent = "Show";
}

function showApp(session) {
  state.csrf = session.CsrfToken;
  state.operator = session.Operator;
  state.expiresAt = session.ExpiresAt;
  elements.operatorName.textContent = session.Operator || "primary_operator";
  elements.sessionExpiry.textContent = session.ExpiresAt
    ? `Expires ${formatRelative(session.ExpiresAt)}`
    : "Emergency API session";
  elements.loginView.hidden = true;
  elements.appView.hidden = false;
  switchPage("review");
}

async function restoreSession() {
  try {
    const session = await api("/admin/auth/session");
    showApp(session);
  } catch (error) {
    showLogin();
  }
}

async function login(event) {
  event.preventDefault();
  elements.loginError.textContent = "";
  const key = elements.adminKey.value;
  elements.loginButton.disabled = true;
  try {
    const session = await api("/admin/auth/login", {
      method: "POST",
      body: JSON.stringify({ key }),
    });
    elements.adminKey.value = "";
    showApp(session);
    toast("Secure operator session started.");
  } catch (error) {
    elements.loginError.textContent = error.message;
    elements.adminKey.value = "";
    elements.adminKey.focus();
  } finally {
    elements.loginButton.disabled = false;
  }
}

async function logout() {
  elements.logoutButton.disabled = true;
  try {
    await api("/admin/auth/logout", {
      method: "POST",
      body: JSON.stringify({}),
    });
  } catch (error) {
    toast(error.message, "error");
  } finally {
    elements.logoutButton.disabled = false;
    showLogin();
  }
}

function switchPage(page) {
  state.currentPage = page;
  document.querySelectorAll(".nav-item").forEach((button) => {
    button.classList.toggle("active", button.dataset.page === page);
  });
  document.querySelectorAll("[data-page-panel]").forEach((panel) => {
    const active = panel.dataset.pagePanel === page;
    panel.hidden = !active;
    panel.classList.toggle("active", active);
  });
  if (page === "review") loadCases();
  if (page === "bugs") loadBugReports();
  if (page === "apis") loadApis();
  if (page === "operations") loadOperations();
  if (page === "audit") loadAudit();
}

function caseTargetLabel(item) {
  const player = item.target_player;
  if (item.target_type === "player" && player) {
    const displayName = player.display_name || player.username || "Player";
    const username = player.username && player.username !== displayName
      ? ` (@${player.username})`
      : "";
    return `${displayName}${username}`;
  }
  return `${item.target_type} · ${item.target_id}`;
}

function reportPartyLabel(player, fallback) {
  if (!player) return fallback;
  const displayName = player.display_name || player.username || fallback;
  const username = player.username && player.username !== displayName
    ? ` (@${player.username})`
    : "";
  return `${displayName}${username}`;
}

function renderCases(items) {
  elements.caseBody.replaceChildren();
  items.forEach((item) => {
    const row = node("tr", { dataset: { clickable: "true" } });
    const stateCell = node("td");
    stateCell.append(createStatusBadge(item.state));
    const targetCell = node("td", {}, [
      node("strong", {}, item.target_type),
      node("small", {}, item.target_id),
    ]);
    const categoryCell = node("td", {}, [
      node("strong", {}, String(item.canonical_category).replaceAll("_", " ")),
      node("small", {}, `${item.severity || "normal"} severity`),
    ]);
    const reportCell = node("td", {}, [
      node("strong", {}, `${item.report_count} total`),
      node("small", {}, `${item.independent_reporters || 0} independent`),
    ]);
    const assignmentCell = node("td", {}, item.assigned_to || "Unassigned");
    const updatedCell = node("td", {}, [
      node("strong", {}, formatRelative(item.updated_at)),
      node("small", {}, formatDate(item.updated_at)),
    ]);
    row.append(
      stateCell,
      targetCell,
      categoryCell,
      reportCell,
      assignmentCell,
      updatedCell
    );
    row.addEventListener("click", () => openCase(item.case_id));
    elements.caseBody.append(row);
  });
  elements.queueEmpty.hidden = items.length !== 0;
}

async function loadCases() {
  if (!state.csrf || state.refreshing) return;
  state.refreshing = true;
  elements.queueStatus.textContent = "Refreshing canonical cases…";
  try {
    const params = new URLSearchParams({
      limit: String(state.caseLimit),
      offset: String(state.caseOffset),
    });
    if (elements.stateFilter.value) {
      params.set("state", elements.stateFilter.value);
    }
    const payload = await api(`/admin/moderation/cases?${params.toString()}`);
    state.cases = payload.Cases || [];
    renderCases(state.cases);
    const independent = state.cases.reduce(
      (sum, item) => sum + Number(item.independent_reporters || 0),
      0
    );
    const reviewRequired = state.cases.filter(
      (item) => item.state === "review_required"
    ).length;
    const controls = state.cases.reduce(
      (sum, item) => sum + Number(item.active_controls || 0),
      0
    );
    elements.queueCount.textContent = String(state.cases.length);
    elements.metricOpen.textContent = String(state.cases.length);
    elements.metricIndependent.textContent = String(independent);
    elements.metricReview.textContent = String(reviewRequired);
    elements.metricControls.textContent = String(controls);
    elements.queueStatus.textContent = `${state.cases.length} case${
      state.cases.length === 1 ? "" : "s"
    } in this page`;
    elements.casesPrev.disabled = state.caseOffset === 0;
    elements.casesNext.disabled = state.cases.length < state.caseLimit;
    elements.casesPageLabel.textContent = `Page ${
      Math.floor(state.caseOffset / state.caseLimit) + 1
    }`;
  } catch (error) {
    elements.queueStatus.textContent = error.message;
    toast(error.message, "error");
  } finally {
    state.refreshing = false;
  }
}

function bugReporterLabel(report) {
  const reporter = report.reporter || {};
  const fallback = reporter.legacy_player_id
    ? `Player ${reporter.legacy_player_id}`
    : "Unknown player";
  const displayName = reporter.display_name || reporter.username || fallback;
  return reporter.username && reporter.username !== displayName
    ? `${displayName} (@${reporter.username})`
    : displayName;
}

function updateBugSelectionControls() {
  const selectedCount = state.selectedBugReportIds.size;
  elements.groupSelectedBugs.disabled = selectedCount === 0;
  elements.groupSelectedBugs.textContent = selectedCount
    ? `Group selected (${selectedCount})`
    : "Group selected";
  const visibleIds = state.bugReports.map((item) => item.report_id);
  elements.selectAllBugReports.checked =
    visibleIds.length > 0
    && visibleIds.every((reportId) => state.selectedBugReportIds.has(reportId));
  elements.selectAllBugReports.indeterminate =
    !elements.selectAllBugReports.checked
    && visibleIds.some((reportId) => state.selectedBugReportIds.has(reportId));
}

function renderBugGroups() {
  const previousValue = elements.bugReportGroupSelect.value;
  elements.bugReportGroupSelect.replaceChildren();
  const newGroup = document.createElement("option");
  newGroup.value = "";
  newGroup.textContent = "New recurring-bug group";
  elements.bugReportGroupSelect.append(newGroup);
  state.bugGroups.forEach((group) => {
    const option = document.createElement("option");
    option.value = group.group_id;
    option.textContent = `${group.title} (${group.report_count})`;
    elements.bugReportGroupSelect.append(option);
  });
  if (
    previousValue
    && state.bugGroups.some((group) => group.group_id === previousValue)
  ) {
    elements.bugReportGroupSelect.value = previousValue;
  }
}

function renderBugReports(items) {
  const visible = new Set(items.map((item) => item.report_id));
  state.selectedBugReportIds = new Set(
    [...state.selectedBugReportIds].filter((reportId) => visible.has(reportId))
  );
  elements.bugReportBody.replaceChildren();
  items.forEach((item) => {
    const row = node("tr", { dataset: { clickable: "true" } });
    const selection = document.createElement("input");
    selection.type = "checkbox";
    selection.checked = state.selectedBugReportIds.has(item.report_id);
    selection.setAttribute("aria-label", `Select ${item.summary || item.report_id}`);
    selection.addEventListener("click", (event) => event.stopPropagation());
    selection.addEventListener("change", () => {
      if (selection.checked) state.selectedBugReportIds.add(item.report_id);
      else state.selectedBugReportIds.delete(item.report_id);
      updateBugSelectionControls();
    });
    const attachmentSummary = [
      item.has_screenshot ? "image" : null,
      item.has_output_log ? "log" : null,
    ].filter(Boolean).join(" + ") || "no attachments";
    row.append(
      node("td", {}, selection),
      node("td", {}, createStatusBadge(item.status)),
      node("td", {}, [
        node("strong", {}, item.summary || "Untitled bug report"),
        node("small", {}, `${item.report_id} · ${attachmentSummary}`),
      ]),
      node("td", {}, bugReporterLabel(item)),
      node("td", {}, item.group_title
        ? [
            node("strong", {}, item.group_title),
            node("small", {}, `${item.group_report_count} reports`),
          ]
        : "Ungrouped"),
      node("td", {}, [
        node("strong", {}, item.build_version || "Unknown build"),
        node("small", {}, item.bundle_version_code == null
          ? item.source_version
          : `Bundle ${item.bundle_version_code}`),
      ]),
      node("td", {}, [
        node("strong", {}, formatRelative(item.created_at)),
        node("small", {}, formatDate(item.created_at)),
      ])
    );
    row.addEventListener("click", () => openBugReport(item.report_id));
    elements.bugReportBody.append(row);
  });
  elements.bugReportEmpty.hidden = items.length !== 0;
  updateBugSelectionControls();
}

async function loadBugReports() {
  if (!state.csrf) return;
  const params = new URLSearchParams({ limit: "200", offset: "0" });
  const query = elements.bugReportSearch.value.trim();
  const statusFilter = elements.bugReportStatusFilter.value;
  if (query) params.set("q", query);
  if (statusFilter) params.set("status", statusFilter);
  elements.bugReportStatus.textContent = "Refreshing bug reports…";
  try {
    const payload = await api(`/admin/bug-reports?${params.toString()}`);
    state.bugReports = payload.Reports || [];
    state.bugGroups = payload.Groups || [];
    renderBugGroups();
    renderBugReports(state.bugReports);
    elements.bugReportCount.textContent = String(payload.OpenCount || 0);
    elements.bugReportStatus.textContent = `${payload.Total || 0} report${
      Number(payload.Total || 0) === 1 ? "" : "s"
    } in this view`;
  } catch (error) {
    elements.bugReportStatus.textContent = error.message;
    toast(error.message, "error");
  }
}

async function fetchBugLog(reportId) {
  const response = await fetch(
    `/admin/bug-reports/${encodeURIComponent(reportId)}/attachments/log`,
    {
      credentials: "same-origin",
      cache: "no-store",
      headers: { Accept: "text/plain" },
    }
  );
  if (!response.ok) {
    if (response.status === 401) showLogin();
    throw new Error(`Failed to load output log (${response.status})`);
  }
  return response.text();
}

async function dismissBugReport(reportId) {
  const reason = window.prompt("Reason for dismissing this bug report:");
  if (!reason?.trim()) return;
  try {
    await api(`/admin/bug-reports/${encodeURIComponent(reportId)}/dismiss`, {
      method: "POST",
      body: JSON.stringify({
        reason: reason.trim(),
        idempotency_key: crypto.randomUUID(),
      }),
    });
    toast("Bug report dismissed and audited.");
    await loadBugReports();
    await openBugReport(reportId);
  } catch (error) {
    toast(error.message, "error");
  }
}

async function ungroupBugReport(reportId) {
  const reason = window.prompt("Reason for removing this report from its group:");
  if (!reason?.trim()) return;
  try {
    await api(`/admin/bug-reports/${encodeURIComponent(reportId)}/ungroup`, {
      method: "POST",
      body: JSON.stringify({
        reason: reason.trim(),
        idempotency_key: crypto.randomUUID(),
      }),
    });
    toast("Bug report removed from the group and audited.");
    await loadBugReports();
    await openBugReport(reportId);
  } catch (error) {
    toast(error.message, "error");
  }
}

async function groupSelectedBugReports() {
  const reportIds = [...state.selectedBugReportIds];
  if (!reportIds.length) return;
  const groupId = elements.bugReportGroupSelect.value || null;
  let title = null;
  if (!groupId) {
    if (reportIds.length < 2) {
      toast("Select at least two reports to create a recurring-bug group.", "error");
      return;
    }
    title = window.prompt("Name for this recurring-bug group:");
    if (!title?.trim()) return;
  }
  const reason = window.prompt("Reason for grouping these reports:");
  if (!reason?.trim()) return;
  elements.groupSelectedBugs.disabled = true;
  try {
    await api("/admin/bug-reports/group", {
      method: "POST",
      body: JSON.stringify({
        report_ids: reportIds,
        group_id: groupId,
        title: title?.trim() || null,
        reason: reason.trim(),
        idempotency_key: crypto.randomUUID(),
      }),
    });
    state.selectedBugReportIds.clear();
    toast("Bug reports grouped and audited.");
    await loadBugReports();
  } catch (error) {
    toast(error.message, "error");
  } finally {
    updateBugSelectionControls();
  }
}

async function openBugReport(reportId) {
  elements.bugReportDetail.hidden = false;
  elements.bugReportDetail.replaceChildren(
    node("p", { className: "muted" }, "Loading bug report…")
  );
  try {
    const payload = await api(
      `/admin/bug-reports/${encodeURIComponent(reportId)}`
    );
    const report = payload.Report;
    elements.bugReportDetail.replaceChildren();
    const header = node("div", { className: "panel-heading" }, [
      node("div", {}, [
        node("p", { className: "eyebrow" }, `Bug report ${report.report_id}`),
        node("h2", {}, report.summary || "Untitled bug report"),
      ]),
      createStatusBadge(report.status),
    ]);
    const overview = node("dl", { className: "detail-grid bug-detail-grid" });
    addDefinition(overview, "Reporter", bugReporterLabel(report));
    addDefinition(
      overview,
      "Player ID",
      report.reporter?.player_id
        || report.reporter?.legacy_player_id
        || "Unknown"
    );
    addDefinition(overview, "Build", report.build_version || report.source_version);
    addDefinition(
      overview,
      "Bundle",
      report.bundle_version_code == null ? "—" : report.bundle_version_code
    );
    addDefinition(overview, "Test case", report.test_case_key || "—");
    addDefinition(overview, "Submitted", formatDate(report.created_at));
    addDefinition(overview, "Group", report.group_title || "Ungrouped");
    addDefinition(overview, "Source", report.source_endpoint);
    const body = node("div", { className: "bug-detail-body" });
    body.append(
      node("section", { className: "drawer-section" }, [
        node("h3", {}, "Description"),
        node(
          "pre",
          { className: "bug-report-text" },
          report.description || "No description supplied."
        ),
      ])
    );

    if (report.has_screenshot) {
      const image = document.createElement("img");
      image.className = "bug-report-image";
      image.alt = `Screenshot attached to ${report.summary || "bug report"}`;
      image.loading = "lazy";
      image.src = `/admin/bug-reports/${encodeURIComponent(
        report.report_id
      )}/attachments/screenshot`;
      const imageSection = node("section", { className: "drawer-section" }, [
        node("h3", {}, "Screenshot"),
        image,
      ]);
      image.addEventListener("error", () => {
        image.replaceWith(
          node("p", { className: "muted" }, "The screenshot could not be displayed.")
        );
      });
      body.append(imageSection);
    }

    if (report.has_output_log) {
      const logText = node(
        "pre",
        { className: "bug-report-log" },
        "Loading output log…"
      );
      const rawLink = document.createElement("a");
      rawLink.className = "secondary-button";
      rawLink.href = `/admin/bug-reports/${encodeURIComponent(
        report.report_id
      )}/attachments/log`;
      rawLink.target = "_blank";
      rawLink.rel = "noopener";
      rawLink.textContent = "Open raw log";
      body.append(
        node("section", { className: "drawer-section" }, [
          node("h3", {}, "Output log"),
          rawLink,
          logText,
        ])
      );
      fetchBugLog(report.report_id)
        .then((value) => {
          logText.textContent = value || "The output log is empty.";
        })
        .catch((error) => {
          logText.textContent = error.message;
        });
    }

    if ((report.group_members || []).length) {
      const memberList = node("div", { className: "timeline" });
      report.group_members.forEach((member) => {
        const memberButton = node(
          "button",
          { className: "result-item", type: "button" },
          [
            node("span", {}, [
              node("strong", {}, member.summary || member.report_id),
              node(
                "small",
                {},
                `Player ${member.reporter_legacy_id || "unknown"} · ${formatDate(
                  member.created_at
                )}`
              ),
            ]),
            createStatusBadge(member.status),
          ]
        );
        memberButton.addEventListener("click", () => openBugReport(member.report_id));
        memberList.append(memberButton);
      });
      body.append(
        node("section", { className: "drawer-section" }, [
          node("h3", {}, `Recurring group · ${report.group_title}`),
          memberList,
        ])
      );
    }

    const auditTimeline = node("div", { className: "timeline" });
    (report.actions || []).forEach((action) => {
      auditTimeline.append(
        renderTimelineItem(
          action,
          `${action.actor_id}: ${action.action}`,
          `${action.reason || "Submission received"} · ${formatDate(
            action.created_at
          )}`
        )
      );
    });
    body.append(
      node("section", { className: "drawer-section" }, [
        node("h3", {}, "Audit timeline"),
        auditTimeline,
      ])
    );

    const controls = node("div", { className: "button-row bug-action-row" });
    if (report.status !== "dismissed") {
      const dismiss = node(
        "button",
        { className: "primary-button", type: "button" },
        "Dismiss report"
      );
      dismiss.addEventListener("click", () => dismissBugReport(report.report_id));
      controls.append(dismiss);
    }
    if (report.group_id) {
      const ungroup = node(
        "button",
        { className: "secondary-button", type: "button" },
        "Remove from group"
      );
      ungroup.addEventListener("click", () => ungroupBugReport(report.report_id));
      controls.append(ungroup);
    }
    body.append(controls);
    elements.bugReportDetail.append(header, overview, body);
  } catch (error) {
    elements.bugReportDetail.replaceChildren(
      node("p", { className: "form-error" }, error.message)
    );
  }
}

function addDefinition(container, label, value) {
  const wrapper = node("div", { className: "detail-field" });
  wrapper.append(node("dt", {}, label), node("dd", {}, value));
  container.append(wrapper);
}

function renderTimelineItem(item, primary, secondary) {
  return node("div", { className: "timeline-item" }, [
    node("p", {}, primary),
    node("small", {}, secondary),
  ]);
}

function renderEvidence(evidence, rawIncluded) {
  const section = node("section", { className: "drawer-section" });
  section.append(node("h3", {}, "Evidence"));
  if (!evidence.length) {
    section.append(node("p", { className: "muted" }, "No stored evidence is available."));
    return section;
  }
  evidence.forEach((item) => {
    const card = node(
      "article",
      { className: `evidence-card ${rawIncluded ? "evidence-raw" : ""}` },
      [
        node("strong", {}, item.evidence_type),
        node("p", {}, rawIncluded ? item.raw_text || item.public_text : item.public_text),
        node("small", {}, `${item.sha256.slice(0, 14)}… · ${formatDate(item.created_at)}`),
      ]
    );
    section.append(card);
  });
  return section;
}

async function openCase(caseId) {
  try {
    const [casePayload, evidencePayload] = await Promise.all([
      api(`/admin/moderation/cases/${encodeURIComponent(caseId)}`),
      api(`/admin/moderation/cases/${encodeURIComponent(caseId)}/evidence`),
    ]);
    const item = casePayload.Case;
    state.selectedCase = item;
    state.actionContext = {
      target_type: item.target_type,
      target_id: item.target_id,
      case_id: item.case_id,
      title: caseTargetLabel(item),
      actions: item.allowed_actions || [],
      refresh: () => openCase(item.case_id),
    };
    elements.drawerTitle.textContent = `Case ${item.case_id.slice(0, 8)}`;
    elements.drawerContent.replaceChildren();

    const overview = node("dl", { className: "detail-grid" });
    addDefinition(overview, "State", String(item.state).replaceAll("_", " "));
    addDefinition(overview, "Target", caseTargetLabel(item));
    addDefinition(overview, "Category", item.canonical_category);
    addDefinition(overview, "Reports", `${item.counting_report_count} weighted / ${item.report_count} total`);
    addDefinition(overview, "Assigned to", item.assigned_to || "Unassigned");
    addDefinition(overview, "Created", formatDate(item.created_at));
    const overviewSection = node("section", { className: "drawer-section" }, [
      node("h3", {}, "Overview"),
      overview,
    ]);
    elements.drawerContent.append(overviewSection);

    const reportsSection = node("section", { className: "drawer-section" });
    reportsSection.append(node("h3", {}, "Reports"));
    const reportsTimeline = node("div", { className: "timeline" });
    (item.reports || []).forEach((report) => {
      const reporter = reportPartyLabel(
        report.reporter_player,
        report.reporter_player_id || "Unknown reporter"
      );
      const target = caseTargetLabel(item);
      reportsTimeline.append(
        renderTimelineItem(
          report,
          report.public_details || "No public details",
          `Reported by ${reporter} → ${target} · ${report.source_version} · ${report.source_endpoint} · ${formatDate(report.created_at)}`
        )
      );
    });
    if (!(item.reports || []).length) {
      reportsTimeline.append(node("p", { className: "muted" }, "No reports."));
    }
    reportsSection.append(reportsTimeline);
    elements.drawerContent.append(reportsSection);

    elements.drawerContent.append(
      renderEvidence(evidencePayload.Evidence || [], false)
    );
    if ((evidencePayload.Evidence || []).length) {
      const reveal = node(
        "button",
        { className: "secondary-button", type: "button" },
        "Reveal restricted evidence"
      );
      reveal.addEventListener("click", async () => {
        if (!window.confirm("Reveal raw evidence and record this access in the audit log?")) {
          return;
        }
        try {
          const raw = await api(
            `/admin/moderation/cases/${encodeURIComponent(caseId)}/evidence/reveal`,
            {
              method: "POST",
              body: JSON.stringify({ idempotency_key: crypto.randomUUID() }),
            }
          );
          elements.drawerContent.append(renderEvidence(raw.Evidence || [], true));
          reveal.remove();
          toast("Evidence access recorded.");
        } catch (error) {
          toast(error.message, "error");
        }
      });
      elements.drawerContent.append(reveal);
    }

    const auditSection = node("section", { className: "drawer-section" });
    auditSection.append(node("h3", {}, "Audit timeline"));
    const auditTimeline = node("div", { className: "timeline" });
    (item.actions || []).forEach((action) => {
      auditTimeline.append(
        renderTimelineItem(
          action,
          `${action.actor_id}: ${action.action}`,
          `${action.reason || "No reason"} · ${formatDate(action.created_at)}`
        )
      );
    });
    if (!(item.actions || []).length) {
      auditTimeline.append(node("p", { className: "muted" }, "No operator actions yet."));
    }
    auditSection.append(auditTimeline);
    elements.drawerContent.append(auditSection);

    elements.takeActionButton.disabled = !(item.allowed_actions || []).length;
    elements.detailDrawer.hidden = false;
  } catch (error) {
    toast(error.message, "error");
  }
}

async function searchTargets(event) {
  event.preventDefault();
  const query = elements.searchInput.value.trim();
  elements.searchStatus.textContent = "Searching canonical records…";
  elements.searchResults.replaceChildren();
  elements.searchDetail.hidden = true;
  try {
    const payload = await api(
      `/admin/moderation/targets/search?q=${encodeURIComponent(query)}`
    );
    const results = payload.Results || [];
    elements.searchStatus.textContent = `${results.length} result${
      results.length === 1 ? "" : "s"
    }`;
    results.forEach((item) => {
      const result = node("button", { className: "result-item", type: "button" }, [
        node("span", {}, [
          node("strong", {}, item.title),
          node("small", {}, `${item.target_type} · ${item.subtitle}`),
        ]),
        createStatusBadge(item.status),
      ]);
      result.addEventListener("click", () => openTarget(item));
      elements.searchResults.append(result);
    });
  } catch (error) {
    elements.searchStatus.textContent = error.message;
  }
}

async function openTarget(item) {
  try {
    const path =
      item.target_type === "player"
        ? `/admin/moderation/players/${encodeURIComponent(item.target_id)}`
        : `/admin/moderation/content/${encodeURIComponent(
            item.target_type
          )}/${encodeURIComponent(item.target_id)}`;
    const payload = await api(path);
    const detail = payload.Player || payload.Content;
    state.selectedTarget = detail;
    state.actionContext = {
      target_type: item.target_type,
      target_id: item.target_id,
      case_id: detail.latest_case_id || null,
      title: item.title,
      actions: detail.allowed_actions || [],
      refresh: () => openTarget(item),
    };
    elements.searchDetail.replaceChildren();
    elements.searchDetail.append(
      node("p", { className: "eyebrow" }, item.target_type),
      node("h2", {}, item.title),
      node("p", { className: "muted" }, item.subtitle)
    );
    const grid = node("dl", { className: "detail-grid" });
    if (item.target_type === "player") {
      addDefinition(grid, "Player ID", detail.player_id);
      addDefinition(grid, "Username", `@${detail.username}`);
      addDefinition(grid, "Display name", detail.display_name);
      addDefinition(grid, "Level", detail.canonical_level);
      addDefinition(grid, "Status", detail.is_banned ? "Banned" : "Active");
      addDefinition(grid, "Active sanctions", (detail.active_sanctions || []).length);
    } else {
      addDefinition(grid, "Target ID", item.target_id);
      addDefinition(grid, "Target type", item.target_type);
      addDefinition(grid, "Latest case", detail.latest_case_id || "None");
      if (detail.invention) {
        addDefinition(grid, "Name", detail.invention.Name || "Untitled");
        addDefinition(
          grid,
          "Publishing",
          detail.invention.IsPublished ? "Published" : "Private"
        );
      }
      if (detail.name) addDefinition(grid, "Name", detail.name);
    }
    elements.searchDetail.append(grid);
    const actionButton = node(
      "button",
      { className: "primary-button", type: "button" },
      "Take action"
    );
    actionButton.disabled = !(detail.allowed_actions || []).length;
    actionButton.addEventListener("click", openActionDialog);
    elements.searchDetail.append(actionButton);
    elements.searchDetail.hidden = false;
  } catch (error) {
    toast(error.message, "error");
  }
}

function renderMaintenance(payload) {
  const active = Boolean(payload.active);
  elements.maintenanceBadge.textContent = active ? "Scheduled" : "Inactive";
  elements.maintenanceBadge.className = `status-badge ${active ? "restricted" : "active"}`;
  elements.maintenanceSummary.replaceChildren();
  [
    ["Status", active ? "Active" : "No active schedule"],
    ["Starts", payload.starts_at_utc ? formatDate(payload.starts_at_utc) : "—"],
    ["Remaining", active ? `${payload.starts_in_minutes} minutes` : "—"],
    ["Revision", payload.revision ?? 0],
  ].forEach(([label, value]) => {
    const wrapper = node("div");
    wrapper.append(node("dt", {}, label), node("dd", {}, value));
    elements.maintenanceSummary.append(wrapper);
  });
  document.getElementById("cancel-maintenance").disabled = !active;
}

function renderMaintenanceDelivery(payload) {
  const capabilities = payload.capabilities || {};
  const version = capabilities.versions?.["25april2019"] || {};
  const hasDelivery = Number.isInteger(payload.realtime_delivered_clients);
  const cancellationUnsupported = Object.values(payload.per_version || {}).some(
    (item) => item?.status === "realtime_cancel_unsupported"
  );
  let lastDelivery = "No send performed";
  if (cancellationUnsupported) {
    lastDelivery = "Snapshot cleared; no realtime cancel";
  } else if (hasDelivery) {
    lastDelivery = `${payload.realtime_delivered_clients} client(s)`;
  }

  elements.maintenanceDelivery.replaceChildren();
  [
    ["Connected clients", capabilities.connected_clients ?? 0],
    ["Last realtime delivery", lastDelivery],
    ["Config snapshot", version.snapshot_supported ? "Supported" : "Unavailable"],
    ["Other builds", capabilities.other_versions_supported ? "Supported" : "Unsupported"],
  ].forEach(([label, value]) => {
    addDefinition(elements.maintenanceDelivery, label, value);
  });
}

function renderSchedules(items) {
  elements.scheduleList.replaceChildren();
  if (!items.length) {
    elements.scheduleList.append(
      node("p", { className: "muted" }, "No timed-content schedules are registered.")
    );
    return;
  }
  items.forEach((item) => {
    elements.scheduleList.append(
      node("article", { className: "schedule-item" }, [
        node("strong", {}, item.schedule_key),
        node(
          "small",
          {},
          item.period_id
            ? `${item.period_id} · ends ${formatDate(item.ends_at_utc)}`
            : `${item.model} · no materialized period`
        ),
      ])
    );
  });
}

async function loadOperations() {
  if (!state.csrf) return;
  try {
    const timed = await api("/admin/operations/timed-content");
    renderSchedules(timed.Schedules || []);
  } catch (error) {
    toast(error.message, "error");
  }
}

async function loadApis() {
  if (!state.csrf) return;
  try {
    const maintenance = await api("/admin/maintenance");
    renderMaintenance(maintenance);
    renderMaintenanceDelivery(maintenance);
  } catch (error) {
    toast(error.message, "error");
  }
}

async function scheduleMaintenance(event) {
  event.preventDefault();
  const submit = elements.maintenanceForm.querySelector('button[type="submit"]');
  submit.disabled = true;
  try {
    const payload = await api("/admin/maintenance", {
      method: "POST",
      body: JSON.stringify({
        starts_in_minutes: Number(elements.maintenanceMinutes.value),
        reason: elements.maintenanceReason.value.trim(),
        idempotency_key: crypto.randomUUID(),
      }),
    });
    renderMaintenance(payload);
    renderMaintenanceDelivery(payload);
    elements.maintenanceReason.value = "";
    toast(
      `Maintenance sent to ${payload.realtime_delivered_clients || 0} connected client(s).`
    );
  } catch (error) {
    toast(error.message, "error");
  } finally {
    submit.disabled = false;
  }
}

async function cancelMaintenance() {
  if (!window.confirm("Cancel the active maintenance schedule?")) return;
  const button = document.getElementById("cancel-maintenance");
  button.disabled = true;
  try {
    const payload = await api("/admin/maintenance", {
      method: "DELETE",
      body: JSON.stringify({
        reason: "Cancelled from first-party admin panel.",
        idempotency_key: crypto.randomUUID(),
      }),
    });
    renderMaintenance(payload);
    renderMaintenanceDelivery(payload);
    toast("Maintenance schedule cancelled.");
  } catch (error) {
    toast(error.message, "error");
  } finally {
    button.disabled = false;
  }
}

async function reconcileTimedContent() {
  const reason = window.prompt("Reason for reconciling timed content:");
  if (!reason) return;
  const button = document.getElementById("reconcile-content");
  button.disabled = true;
  try {
    await api("/admin/operations/timed-content/reconcile", {
      method: "POST",
      body: JSON.stringify({
        reason,
        idempotency_key: crypto.randomUUID(),
      }),
    });
    await loadOperations();
    toast("Timed content reconciled against canonical UTC.");
  } catch (error) {
    toast(error.message, "error");
  } finally {
    button.disabled = false;
  }
}

async function loadAudit() {
  if (!state.csrf) return;
  const params = new URLSearchParams({ limit: "100", offset: "0" });
  const action = elements.auditActionFilter.value.trim();
  const target = elements.auditTargetFilter.value.trim();
  if (action) params.set("action", action);
  if (target) params.set("target", target);
  try {
    const payload = await api(`/admin/moderation/audit?${params.toString()}`);
    const rows = payload.Audit || [];
    elements.auditBody.replaceChildren();
    rows.forEach((item) => {
      const row = node("tr");
      row.append(
        node("td", {}, formatDate(item.created_at)),
        node("td", {}, item.actor_id),
        node("td", {}, item.action),
        node("td", {}, [
          node("strong", {}, item.target_type),
          node("small", {}, item.target_id),
        ]),
        node("td", {}, `${item.previous_state || "—"} → ${item.new_state || "—"}`),
        node("td", {}, item.reason || "—")
      );
      elements.auditBody.append(row);
    });
    elements.auditEmpty.hidden = rows.length !== 0;
  } catch (error) {
    toast(error.message, "error");
  }
}

function currentAction() {
  return (state.actionContext?.actions || []).find(
    (item) => item.name === elements.actionSelect.value
  );
}

function updateActionFields() {
  const action = currentAction();
  if (!action) return;
  elements.actionEffect.textContent = action.effect;
  elements.durationField.hidden = !action.temporary;
  updateCustomDurationField();
  elements.confirmationField.hidden = !action.requires_confirmation;
  elements.actionConfirmation.value = "";
  if (action.requires_confirmation) {
    elements.confirmationLabel.textContent = `Type “${action.confirmation_phrase}” to continue`;
  }
  elements.submitAction.className = action.requires_confirmation
    ? "danger-button"
    : "primary-button";
}

function openActionDialog() {
  const context = state.actionContext;
  if (!context || !context.actions?.length) {
    toast("No server-authorized actions are available.", "error");
    return;
  }
  elements.actionTargetSummary.replaceChildren(
    node("strong", {}, context.title || `${context.target_type} ${context.target_id}`),
    node("small", {}, `${context.target_type} · ${context.target_id}`)
  );
  elements.actionSelect.replaceChildren();
  context.actions.forEach((action) => {
    const option = document.createElement("option");
    option.value = action.name;
    option.textContent = action.label;
    elements.actionSelect.append(option);
  });
  elements.actionReason.value = "";
  elements.actionDuration.value = "900";
  elements.actionCustomDuration.value = "";
  elements.actionConfirmation.value = "";
  elements.actionError.textContent = "";
  updateActionFields();
  elements.actionDialog.showModal();
}

function closeActionDialog() {
  elements.actionDialog.close();
  elements.actionError.textContent = "";
}

async function submitAction(event) {
  event.preventDefault();
  const context = state.actionContext;
  const action = currentAction();
  if (!context || !action) return;
  elements.actionError.textContent = "";
  let durationSeconds = null;
  if (action.temporary) {
    try {
      durationSeconds = selectedTimeoutSeconds();
    } catch (error) {
      elements.actionError.textContent = error.message;
      return;
    }
  }
  elements.submitAction.disabled = true;
  const idempotencyKey = crypto.randomUUID();
  try {
    if (action.name === "reverse_action") {
      await api(
        `/admin/moderation/actions/${encodeURIComponent(action.action_id)}/reverse`,
        {
          method: "POST",
          body: JSON.stringify({
            reason: elements.actionReason.value.trim(),
            idempotency_key: idempotencyKey,
          }),
        }
      );
    } else {
      await api("/admin/moderation/actions", {
        method: "POST",
        body: JSON.stringify({
          case_id: context.case_id,
          target_type: context.target_type,
          target_id: context.target_id,
          action: action.name,
          duration_seconds: durationSeconds,
          reason: elements.actionReason.value.trim(),
          idempotency_key: idempotencyKey,
          confirmation: action.requires_confirmation
            ? elements.actionConfirmation.value
            : null,
        }),
      });
    }
    closeActionDialog();
    toast("Action applied and recorded in the audit log.");
    await loadCases();
    if (typeof context.refresh === "function") await context.refresh();
  } catch (error) {
    elements.actionError.textContent = error.message;
  } finally {
    elements.submitAction.disabled = false;
  }
}

elements.loginForm.addEventListener("submit", login);
elements.logoutButton.addEventListener("click", logout);
elements.toggleSecret.addEventListener("click", () => {
  const revealing = elements.adminKey.type === "password";
  elements.adminKey.type = revealing ? "text" : "password";
  elements.toggleSecret.textContent = revealing ? "Hide" : "Show";
});
document.querySelectorAll(".nav-item").forEach((button) => {
  button.addEventListener("click", () => switchPage(button.dataset.page));
});
document.getElementById("refresh-cases").addEventListener("click", loadCases);
document
  .getElementById("refresh-bug-reports")
  .addEventListener("click", loadBugReports);
document
  .getElementById("bug-report-filter-form")
  .addEventListener("submit", (event) => {
    event.preventDefault();
    loadBugReports();
  });
elements.bugReportStatusFilter.addEventListener("change", loadBugReports);
elements.selectAllBugReports.addEventListener("change", () => {
  state.bugReports.forEach((item) => {
    if (elements.selectAllBugReports.checked) {
      state.selectedBugReportIds.add(item.report_id);
    } else {
      state.selectedBugReportIds.delete(item.report_id);
    }
  });
  renderBugReports(state.bugReports);
});
elements.groupSelectedBugs.addEventListener("click", groupSelectedBugReports);
elements.stateFilter.addEventListener("change", () => {
  state.caseOffset = 0;
  loadCases();
});
elements.casesPrev.addEventListener("click", () => {
  state.caseOffset = Math.max(0, state.caseOffset - state.caseLimit);
  loadCases();
});
elements.casesNext.addEventListener("click", () => {
  state.caseOffset += state.caseLimit;
  loadCases();
});
document.getElementById("close-drawer").addEventListener("click", () => {
  elements.detailDrawer.hidden = true;
});
elements.takeActionButton.addEventListener("click", openActionDialog);
elements.searchForm.addEventListener("submit", searchTargets);
document
  .getElementById("refresh-operations")
  .addEventListener("click", loadOperations);
document.getElementById("refresh-apis").addEventListener("click", loadApis);
elements.maintenanceForm.addEventListener("submit", scheduleMaintenance);
document
  .getElementById("cancel-maintenance")
  .addEventListener("click", cancelMaintenance);
document
  .getElementById("reconcile-content")
  .addEventListener("click", reconcileTimedContent);
document.getElementById("refresh-audit").addEventListener("click", loadAudit);
document.getElementById("audit-filter-form").addEventListener("submit", (event) => {
  event.preventDefault();
  loadAudit();
});
elements.actionSelect.addEventListener("change", updateActionFields);
elements.actionDuration.addEventListener("change", updateCustomDurationField);
elements.actionForm.addEventListener("submit", submitAction);
document
  .getElementById("close-action-dialog")
  .addEventListener("click", closeActionDialog);
document.getElementById("cancel-action").addEventListener("click", closeActionDialog);

window.setInterval(() => {
  if (document.hidden || !state.csrf) return;
  if (state.currentPage === "review") loadCases();
  if (state.currentPage === "bugs") loadBugReports();
  if (state.currentPage === "apis") loadApis();
  if (state.currentPage === "operations") loadOperations();
}, 30000);

restoreSession();
