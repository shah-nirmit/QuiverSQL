import * as vscode from 'vscode';
import { ExplainQueryResult, PlanMetrics, PlanNode, SourcePlanEntry } from './models';
import { iconSymbolIdFor, labelFor, svgSymbolsLibrary } from './providerIcons';

export function escapePlanHtml(unsafe: unknown): string {
    if (unsafe === null || unsafe === undefined) {
        return '';
    }
    return String(unsafe)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

export function formatPlanMetrics(metrics?: PlanMetrics | null): string {
    if (!metrics) {
        return '';
    }

    const parts: string[] = [];
    if (typeof metrics.estimated_rows === 'number' && Number.isFinite(metrics.estimated_rows)) {
        parts.push(`Rows: ${metrics.estimated_rows}`);
    }
    if (
        typeof metrics.startup_cost === 'number' &&
        Number.isFinite(metrics.startup_cost) &&
        typeof metrics.total_cost === 'number' &&
        Number.isFinite(metrics.total_cost)
    ) {
        parts.push(`Cost: ${metrics.startup_cost}..${metrics.total_cost}`);
    } else if (typeof metrics.total_cost === 'number' && Number.isFinite(metrics.total_cost)) {
        parts.push(`Cost: ${metrics.total_cost}`);
    }
    return parts.join(' | ');
}

export function formatPlanForDisplay(plan: unknown): string {
    if (typeof plan === 'string') {
        return plan;
    }
    return JSON.stringify(plan, null, 2);
}

export function nodeDisplayTitle(node: PlanNode): string {
    return node.attributes?.table ||
        node.attributes?.join_condition ||
        node.attributes?.predicate ||
        node.attributes?.sort ||
        node.attributes?.expressions ||
        node.label ||
        node.node_type;
}

/**
 * Returns a per-table-name index of the `label` of every `TableScan` node
 * in the plan graph. Used to populate step 3 ("Logical plan fragment") of
 * each per-table card in the Source tab without dropping back to text-search
 * the raw plan dump.
 */
function collectLogicalFragmentsByTable(
    nodes: Record<string, PlanNode>,
): Map<string, string> {
    const map = new Map<string, string>();
    for (const node of Object.values(nodes)) {
        if (node.node_type === 'TableScan' && node.source_ref) {
            map.set(node.source_ref, node.label);
        }
    }
    return map;
}

/**
 * Stable, HTML-id-safe slug — used to build `id="source-card-…"` anchors that
 * the Tree-tab click-through scrolls into view. Keeps letters/digits/dashes
 * as-is and replaces every other char with `-` to avoid CSS-selector
 * surprises when a table name contains dots or quotes.
 */
function slugifyForId(value: string): string {
    return value.replace(/[^A-Za-z0-9_-]+/g, '-');
}

export function renderPlanVisualizationHtml(result: ExplainQueryResult, nonce: string): string {
    const graphJson = JSON.stringify(result.federated_plan).replace(/</g, '\\u003c');
    const sourcePlans: Record<string, SourcePlanEntry> = result.source_plans || {};
    const sourcePlansJson = JSON.stringify(sourcePlans).replace(/</g, '\\u003c');
    const rawPlan = result.raw || '';
    let rawText = rawPlan;
    if (rawText.length > 50000) {
        rawText = rawText.substring(0, 50000) + "\n\n... [TRUNCATED - PLAN TOO LARGE] ...";
    }
    const physicalText = result.physical_plan_text || '';

    const copyPayloads: Record<string, string> = {
        logical: rawText,
    };
    if (physicalText) {
        copyPayloads['physical'] = physicalText;
    }

    // Each remote table gets a stacked card showing the three layers of a
    // federated query in the order they execute: Native SQL → Remote EXPLAIN
    // → Logical-plan fragment (the DataFusion TableScan that consumes the
    // returned rows). This replaces the old wall-of-text "Native Source
    // Plans" section that showed only the EXPLAIN output disconnected from
    // its SQL.
    const nativePlanEntries = Object.entries(sourcePlans).sort(([left], [right]) => left.localeCompare(right));
    const logicalFragmentByTable = collectLogicalFragmentsByTable(result.federated_plan.nodes);
    const sourceCardsHtml = nativePlanEntries.map(([sourceRef, entry]) => {
        const sqlCopyKey = `sql:${sourceRef}`;
        const explainCopyKey = `native:${sourceRef}`;
        const fragmentCopyKey = `fragment:${sourceRef}`;
        const explainText = formatPlanForDisplay(entry.native_explain);
        const fragmentText = logicalFragmentByTable.get(sourceRef) || '(not found in logical plan)';
        copyPayloads[sqlCopyKey] = entry.native_sql || '';
        copyPayloads[explainCopyKey] = explainText;
        copyPayloads[fragmentCopyKey] = fragmentText;
        const cardAnchor = `source-card-${slugifyForId(sourceRef)}`;
        const providerLabel = labelFor(entry.provider_kind);
        const iconId = iconSymbolIdFor(entry.provider_kind);
        return `
            <section class="source-card" id="${escapePlanHtml(cardAnchor)}">
                <header class="source-card-header">
                    <svg class="source-card-icon" width="18" height="18" viewBox="0 0 16 16" aria-hidden="true"><use href="#${escapePlanHtml(iconId)}"/></svg>
                    <span class="source-card-title">${escapePlanHtml(sourceRef)}</span>
                    <span class="source-card-meta">${escapePlanHtml(providerLabel)} &middot; ${escapePlanHtml(entry.dialect || '')}</span>
                </header>
                <div class="source-card-step">
                    <div class="step-heading">
                        <span class="step-num">1</span>
                        <span class="step-label">Native SQL</span>
                        <button class="icon-button copy-button" data-copy-key="${escapePlanHtml(sqlCopyKey)}" title="Copy native SQL">&#x2398;</button>
                    </div>
                    <pre class="step-sql">${escapePlanHtml(entry.native_sql || '(no SQL captured)')}</pre>
                </div>
                <div class="source-card-step">
                    <div class="step-heading">
                        <span class="step-num">2</span>
                        <span class="step-label">Remote EXPLAIN</span>
                        <button class="icon-button copy-button" data-copy-key="${escapePlanHtml(explainCopyKey)}" title="Copy remote EXPLAIN">&#x2398;</button>
                    </div>
                    <pre>${escapePlanHtml(explainText)}</pre>
                </div>
                <div class="source-card-step">
                    <div class="step-heading">
                        <span class="step-num">3</span>
                        <span class="step-label">Logical plan fragment</span>
                        <button class="icon-button copy-button" data-copy-key="${escapePlanHtml(fragmentCopyKey)}" title="Copy logical fragment">&#x2398;</button>
                    </div>
                    <pre>${escapePlanHtml(fragmentText)}</pre>
                </div>
            </section>
        `;
    }).join('');

    const copyPayloadsJson = JSON.stringify(copyPayloads).replace(/</g, '\\u003c');
    const warningsHtml = result.warnings && result.warnings.length > 0
        ? `<div class="warning-banner"><strong>Warnings:</strong><ul>${result.warnings.map(w => `<li>${escapePlanHtml(w)}</li>`).join('')}</ul></div>`
        : '';
    const truncatedHtml = result.federated_plan.truncated
        ? `<div class="warning-banner"><strong>Warning:</strong> The plan graph was truncated because it is too large.</div>`
        : '';
    const symbolsLibrary = svgSymbolsLibrary();

    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-${nonce}';">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Query Plan</title>
    <style>
        body {
            font-family: var(--vscode-font-family);
            background-color: var(--vscode-editor-background);
            color: var(--vscode-editor-foreground);
            padding: 0;
            margin: 0;
            display: flex;
            flex-direction: column;
            height: 100vh;
            overflow: hidden;
        }
        .warning-banner {
            background-color: var(--vscode-inputValidation-warningBackground);
            border: 1px solid var(--vscode-inputValidation-warningBorder);
            color: var(--vscode-editor-foreground);
            padding: 8px 12px;
            margin: 0;
            font-size: 13px;
        }
        .warning-banner ul { margin: 4px 0 0 0; padding-left: 20px; }
        .tabs {
            display: flex;
            background-color: var(--vscode-editorGroupHeader-tabsBackground);
            border-bottom: 1px solid var(--vscode-editorGroupHeader-tabsBorder);
        }
        .tab {
            padding: 8px 16px;
            cursor: pointer;
            color: var(--vscode-tab-inactiveForeground);
            background-color: var(--vscode-tab-inactiveBackground);
            border-right: 1px solid var(--vscode-tab-border);
            user-select: none;
        }
        .tab.active {
            color: var(--vscode-tab-activeForeground);
            background-color: var(--vscode-tab-activeBackground);
            border-bottom: 1px solid var(--vscode-tab-activeBorder);
        }
        .tab-content {
            display: none;
            flex: 1;
            min-height: 0;
            overflow: hidden;
            position: relative;
        }
        .tab-content.active {
            display: flex;
            flex-direction: column;
        }
        .tree-toolbar {
            display: flex;
            align-items: center;
            gap: 6px;
            padding: 6px 8px;
            border-bottom: 1px solid var(--vscode-panel-border);
            background: var(--vscode-editorGroupHeader-noTabsBackground);
        }
        .toolbar-label {
            color: var(--vscode-descriptionForeground);
            font-size: 12px;
            margin-left: 4px;
        }
        .icon-button {
            min-width: 28px;
            height: 26px;
            padding: 0 8px;
            border: 1px solid var(--vscode-button-border, transparent);
            border-radius: 4px;
            color: var(--vscode-button-foreground);
            background: var(--vscode-button-background);
            cursor: pointer;
            font: inherit;
            line-height: 24px;
        }
        .icon-button:hover {
            background: var(--vscode-button-hoverBackground);
        }
        #tree-container {
            flex: 1;
            min-height: 0;
            overflow: hidden;
            position: relative;
            background: var(--vscode-editor-background);
            cursor: grab;
        }
        #tree-container.dragging {
            cursor: grabbing;
        }
        #plan-svg {
            display: block;
            width: 100%;
            height: 100%;
            user-select: none;
        }
        .node-rect {
            fill: var(--vscode-editorWidget-background);
            stroke: var(--vscode-editorWidget-border);
            stroke-width: 1.5;
            rx: 6;
            ry: 6;
        }
        .plan-node.table-scan .node-rect {
            stroke: var(--vscode-focusBorder);
        }
        .node-text {
            fill: var(--vscode-editor-foreground);
            font-family: var(--vscode-font-family);
            font-size: 12px;
            pointer-events: none;
        }
        .node-type {
            font-weight: 700;
            fill: var(--vscode-symbolIcon-classForeground);
        }
        .node-muted {
            fill: var(--vscode-descriptionForeground);
            font-size: 11px;
        }
        .node-native {
            fill: var(--vscode-textLink-foreground);
            font-size: 11px;
        }
        .node-broadcast {
            fill: var(--vscode-charts-orange);
            font-size: 11px;
            font-weight: 600;
        }
        .node-sort-pushed {
            fill: var(--vscode-charts-blue, #4fc1ff);
            font-size: 11px;
            font-weight: 600;
        }
        .edge {
            fill: none;
            stroke: var(--vscode-editorIndentGuide-background);
            stroke-width: 2;
        }
        #table-container,
        #source-container {
            flex: 1;
            min-height: 0;
            overflow: auto;
            padding: 10px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            text-align: left;
            padding: 6px 10px;
            border-bottom: 1px solid var(--vscode-panel-border);
            vertical-align: top;
        }
        th {
            background-color: var(--vscode-editorGroupHeader-noTabsBackground);
            position: sticky;
            top: 0;
        }
        .indent {
            display: inline-block;
            width: 20px;
        }
        pre {
            margin: 0 0 14px 0;
            white-space: pre-wrap;
            overflow-wrap: anywhere;
            font-family: var(--vscode-editor-font-family, monospace);
        }
        .section-heading {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
            font-weight: 700;
            margin-top: 15px;
            margin-bottom: 5px;
            border-bottom: 1px solid var(--vscode-panel-border);
            padding-bottom: 4px;
        }
        .section-heading:first-child {
            margin-top: 0;
        }
        /* Legend bar above the plan tree — explains badge colours so users
           don't have to read the docs to know what orange/blue mean. */
        .legend-bar {
            display: flex;
            flex-wrap: wrap;
            gap: 14px;
            padding: 4px 10px;
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
            border-bottom: 1px solid var(--vscode-panel-border);
            background: var(--vscode-editorGroupHeader-noTabsBackground);
        }
        .legend-item { display: inline-flex; align-items: center; gap: 5px; }
        .legend-swatch {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 2px;
        }
        .swatch-broadcast { background: var(--vscode-charts-orange); }
        .swatch-sort      { background: var(--vscode-charts-blue, #4fc1ff); }
        .swatch-scan      { background: transparent; border: 1.5px solid var(--vscode-focusBorder); }
        .swatch-warn      { background: var(--vscode-editorWarning-foreground, #e0a000); }
        /* Phase 10 — legend toggle button for the runtime-metrics overlay. */
        .legend-toggle {
            margin-left: auto;
            padding: 2px 8px;
            font-size: 11px;
            border: 1px solid var(--vscode-button-border, transparent);
            background: var(--vscode-button-secondaryBackground);
            color: var(--vscode-button-secondaryForeground);
            border-radius: 4px;
            cursor: pointer;
        }
        .legend-toggle[aria-pressed="true"] {
            background: var(--vscode-button-background);
            color: var(--vscode-button-foreground);
        }
        .legend-toggle:disabled {
            opacity: 0.55;
            cursor: not-allowed;
        }
        /* Per-node text classes for the new Phase 10 badges. */
        .node-warn    { fill: var(--vscode-editorWarning-foreground, #e0a000); font-weight: 700; }
        .node-info    { fill: var(--vscode-descriptionForeground); font-style: italic; }
        .node-runtime { fill: var(--vscode-charts-green, #4caf50); font-style: italic; }
        /* TableScan node icon is rendered in the SVG at (8, 8); shift the
           node-type label right so it doesn't overlap. */
        .node-type-with-icon { transform: translate(20px, 0); }
        /* Clickable TableScan groups in the plan tree — hint with a pointer
           cursor and a subtle highlight on hover. */
        .plan-node.table-scan { cursor: pointer; }
        .plan-node.table-scan:hover .node-rect {
            stroke-width: 2;
            filter: brightness(1.1);
        }
        /* Global Logical / Physical plan sections in the Source tab — wrapped
           in <details> so users can collapse them; the per-table cards
           below are the main content. */
        .global-plan-section { margin-bottom: 12px; }
        .global-plan-section > summary {
            cursor: pointer;
            list-style: revert;
        }
        .global-plan-section > summary::-webkit-details-marker {
            display: revert;
        }
        /* Per-table card in the Source tab: Native SQL → Remote EXPLAIN →
           Logical fragment, stacked in execution order. */
        .source-card {
            border: 1px solid var(--vscode-panel-border);
            border-radius: 6px;
            margin: 10px 0 16px 0;
            background: var(--vscode-editorWidget-background);
            overflow: hidden;
        }
        .source-card-header {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 10px;
            background: var(--vscode-editorGroupHeader-noTabsBackground);
            border-bottom: 1px solid var(--vscode-panel-border);
        }
        .source-card-icon { flex: 0 0 18px; }
        .source-card-title {
            font-weight: 700;
            font-family: var(--vscode-editor-font-family, monospace);
        }
        .source-card-meta {
            margin-left: auto;
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
        }
        .source-card-step { padding: 8px 10px; border-bottom: 1px solid var(--vscode-panel-border); }
        .source-card-step:last-child { border-bottom: none; }
        .step-heading {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 4px;
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
        }
        .step-num {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 18px;
            height: 18px;
            border-radius: 50%;
            background: var(--vscode-button-background);
            color: var(--vscode-button-foreground);
            font-weight: 700;
            font-size: 11px;
        }
        .step-label { font-weight: 600; color: var(--vscode-editor-foreground); }
        .step-heading .copy-button { margin-left: auto; }
        .step-sql {
            background: var(--vscode-textCodeBlock-background, var(--vscode-editor-background));
            border: 1px solid var(--vscode-panel-border);
            border-radius: 4px;
            padding: 8px 10px;
            font-family: var(--vscode-editor-font-family, monospace);
        }
        .source-card.highlight { box-shadow: 0 0 0 2px var(--vscode-focusBorder); }
        .muted-text { color: var(--vscode-descriptionForeground); font-style: italic; padding: 8px 0; }
    </style>
</head>
<body>
    ${warningsHtml}
    ${truncatedHtml}

    <div class="tabs">
        <div class="tab active" data-target="tree-tab">Tree</div>
        <div class="tab" data-target="table-tab">Table</div>
        <div class="tab" data-target="source-tab">Source</div>
    </div>

    <div id="tree-tab" class="tab-content active">
        <div class="tree-toolbar">
            <button class="icon-button" id="tree-zoom-in" title="Zoom in" aria-label="Zoom in">+</button>
            <button class="icon-button" id="tree-zoom-out" title="Zoom out" aria-label="Zoom out">-</button>
            <button class="icon-button" id="tree-fit" title="Fit plan" aria-label="Fit plan">Fit</button>
            <button class="icon-button" id="tree-reset" title="Reset pan and zoom" aria-label="Reset pan and zoom">Reset</button>
            <span class="toolbar-label">Drag to pan, scroll to zoom, click a TableScan for its Source card</span>
        </div>
        <div class="legend-bar" role="note" aria-label="Plan badge legend">
            <span class="legend-item"><span class="legend-swatch swatch-broadcast"></span>Broadcast pushdown</span>
            <span class="legend-item"><span class="legend-swatch swatch-sort"></span>Sort pushdown</span>
            <span class="legend-item"><span class="legend-swatch swatch-warn"></span>Full scan</span>
            <span class="legend-item"><span class="legend-swatch swatch-scan"></span>TableScan</span>
            <button id="metrics-toggle" class="legend-toggle" aria-pressed="false"
                    title="Show actual rows + elapsed compute from EXPLAIN ANALYZE">Metrics</button>
        </div>
        <div id="tree-container">
            <svg id="plan-svg" role="img" aria-label="Federated query plan tree">
                ${symbolsLibrary}
                <g id="plan-viewport"></g>
            </svg>
        </div>
    </div>

    <div id="table-tab" class="tab-content">
        <div id="table-container">
            <input type="text" id="table-search" placeholder="Search..." style="margin-bottom: 10px; padding: 4px; width: 300px; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border);">
            <table id="plan-table">
                <thead>
                    <tr>
                        <th>Operator</th>
                        <th>Details</th>
                        <th>Est. Rows</th>
                    </tr>
                </thead>
                <tbody id="plan-table-body"></tbody>
            </table>
        </div>
    </div>

    <div id="source-tab" class="tab-content">
        <div id="source-container">
            <details class="global-plan-section" open>
                <summary class="section-heading">
                    <span>Federated Logical Plan</span>
                    <button class="icon-button copy-button" data-copy-key="logical" title="Copy logical plan" aria-label="Copy logical plan">&#x2398;</button>
                </summary>
                <pre>${escapePlanHtml(rawText)}</pre>
            </details>
            ${physicalText ? `
            <details class="global-plan-section">
                <summary class="section-heading">
                    <span>DataFusion Physical Plan</span>
                    <button class="icon-button copy-button" data-copy-key="physical" title="Copy physical plan" aria-label="Copy physical plan">&#x2398;</button>
                </summary>
                <pre>${escapePlanHtml(physicalText)}</pre>
            </details>` : ''}
            ${nativePlanEntries.length > 0 ? `
            <div class="section-heading"><span>Per-Table Pushdowns</span></div>
            ${sourceCardsHtml}
            ` : '<div class="section-heading"><span>Per-Table Pushdowns</span></div><p class="muted-text">No remote tables in this query — every scan is local (CSV / JSON / SQLite file). The Native SQL cards only appear for Postgres, MySQL, MariaDB, and remote SQLite sources.</p>'}
        </div>
    </div>

    <script nonce="${nonce}">
        const vscodeApi = typeof acquireVsCodeApi === 'function' ? acquireVsCodeApi() : null;
        const graph = ${graphJson};
        const sourcePlans = ${sourcePlansJson};
        const copyPayloads = ${copyPayloadsJson};
        const treeState = { rendered: false, scale: 1, x: 24, y: 24, width: 1, height: 1 };
        let panState = null;
        // Phase 10 — metrics overlay state. When true, nodeLines appends
        // a muted "actual: N rows · Xms" line per node from runtime
        // metrics. Toggled via the legend-bar button below.
        let metricsOverlayOn = false;

        document.querySelectorAll('.tab').forEach(function(tab) {
            tab.addEventListener('click', function() {
                document.querySelectorAll('.tab').forEach(function(t) { t.classList.remove('active'); });
                document.querySelectorAll('.tab-content').forEach(function(c) { c.classList.remove('active'); });
                tab.classList.add('active');
                document.getElementById(tab.dataset.target).classList.add('active');
                if (tab.dataset.target === 'tree-tab') {
                    renderTree();
                    fitTree();
                }
            });
        });

        document.querySelectorAll('.copy-button').forEach(function(button) {
            button.addEventListener('click', function() {
                const key = button.getAttribute('data-copy-key');
                if (vscodeApi && key && Object.prototype.hasOwnProperty.call(copyPayloads, key)) {
                    vscodeApi.postMessage({ command: 'copyPlan', text: copyPayloads[key] });
                }
            });
        });

        function hasNumber(value) {
            return typeof value === 'number' && Number.isFinite(value);
        }

        function formatMetrics(metrics) {
            if (!metrics) return '';
            const parts = [];
            if (hasNumber(metrics.estimated_rows)) parts.push('Rows: ' + metrics.estimated_rows);
            if (hasNumber(metrics.startup_cost) && hasNumber(metrics.total_cost)) {
                parts.push('Cost: ' + metrics.startup_cost + '..' + metrics.total_cost);
            } else if (hasNumber(metrics.total_cost)) {
                parts.push('Cost: ' + metrics.total_cost);
            }
            return parts.join(' | ');
        }

        function displayTitle(node) {
            const attrs = node.attributes || {};
            return attrs.table || attrs.join_condition || attrs.predicate || attrs.sort || attrs.expressions || node.label || node.node_type;
        }

        function displayDetails(node) {
            const attrs = node.attributes || {};
            if (attrs.output_columns) return 'Cols: ' + attrs.output_columns;
            return '';
        }

        function shortText(value, max) {
            const text = String(value || '').replace(/\\s+/g, ' ').trim();
            if (text.length <= max) return text;
            return text.slice(0, Math.max(0, max - 3)) + '...';
        }

        function wrapText(value, maxChars, maxLines) {
            const words = String(value || '').replace(/\\s+/g, ' ').trim().split(' ').filter(Boolean);
            const lines = [];
            let current = '';
            words.forEach(function(word) {
                if (!current) {
                    current = word;
                } else if ((current + ' ' + word).length <= maxChars) {
                    current += ' ' + word;
                } else {
                    lines.push(current);
                    current = word;
                }
            });
            if (current) lines.push(current);
            if (lines.length > maxLines) {
                const kept = lines.slice(0, maxLines);
                kept[maxLines - 1] = shortText(kept[maxLines - 1], maxChars);
                return kept;
            }
            return lines.length ? lines : [''];
        }

        function escapeHtml(str) {
            if (str === null || str === undefined) return '';
            return String(str)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#039;');
        }

        // Phase 10 — formats a runtime-row count using K/M/B suffixes so a
        // 1,234,567-row scan renders as "1.2M" instead of overflowing the
        // node box.
        function formatRowCount(n) {
            if (!hasNumber(n)) return '';
            if (n >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1) + 'B';
            if (n >= 1_000_000)     return (n / 1_000_000).toFixed(1) + 'M';
            if (n >= 1_000)         return (n / 1_000).toFixed(1) + 'K';
            return String(n);
        }

        // Phase 10 — formats elapsed-compute milliseconds compactly:
        // sub-millisecond renders as "<1ms", anything above 1s uses "s".
        function formatElapsedMs(ms) {
            if (!hasNumber(ms)) return '';
            if (ms < 1) return '<1ms';
            if (ms < 1000) return ms + 'ms';
            return (ms / 1000).toFixed(2) + 's';
        }

        function nodeLines(node) {
            const isTableScan = node.node_type === 'TableScan';
            const lines = [{
                text: node.node_type,
                // Shift label right so it doesn't overlap the provider icon
                // we render at (8, 8) on TableScan nodes.
                cls: isTableScan ? 'node-type node-type-with-icon' : 'node-type',
            }];
            wrapText(displayTitle(node), 42, 2).forEach(function(line) {
                if (line) lines.push({ text: line, cls: 'node-text' });
            });
            const details = displayDetails(node);
            if (details) lines.push({ text: shortText(details, 46), cls: 'node-muted' });
            const metrics = formatMetrics(node.metrics);
            if (metrics) lines.push({ text: metrics, cls: 'node-muted' });
            if (node.native_plan_ref && sourcePlans[node.native_plan_ref]) {
                lines.push({ text: 'Click for Native SQL ↦', cls: 'node-native' });
            }
            // Phase 10 — full-scan warning. Surfaced as orange "Full scan ⚠"
            // whenever the daemon stamped is_full_scan="true" on a
            // TableScan that had neither a captured WHERE/LIMIT nor a local
            // filter+fetch pair. Always rendered (not gated by the metrics
            // toggle) because it is a planner-time finding, not runtime.
            if (node.attributes && node.attributes.is_full_scan === 'true') {
                lines.push({ text: 'Full scan ⚠', cls: 'node-warn' });
            }
            // Phase 10 — pushdown reasoning info line. The daemon classifies
            // why a filter wasn't pushed down (multi_source_join /
            // unsupported_expression / local_file_scan); the UI renders it
            // as a muted hint so the user can understand the plan shape
            // without re-reading the docs.
            if (node.attributes && node.attributes.pushdown_reason) {
                lines.push({
                    text: 'Why: ' + String(node.attributes.pushdown_reason).replace(/_/g, ' '),
                    cls: 'node-info',
                });
            }
            // Phase 10 — runtime metrics overlay (EXPLAIN ANALYZE). Renders
            // a single "actual: <rows> · <ms>" line per node when the
            // overlay toggle is on and runtime metrics survived the
            // ANALYZE drain. Mem is only shown when present.
            if (metricsOverlayOn && node.metrics && hasNumber(node.metrics.actual_rows)) {
                const m = node.metrics;
                const segments = ['actual: ' + formatRowCount(m.actual_rows) + ' rows'];
                if (hasNumber(m.elapsed_compute_ms)) {
                    segments.push(formatElapsedMs(m.elapsed_compute_ms));
                }
                if (hasNumber(m.mem_used_bytes)) {
                    segments.push(formatRowCount(m.mem_used_bytes) + 'B');
                }
                lines.push({ text: segments.join(' · '), cls: 'node-runtime' });
            }
            // Broadcast-rewrite badge. The daemon stamps the rewrite metadata
            // on up to three surfaces per BroadcastApplication — the remote
            // TableScan, the rewritten Join, and (if it survives optimization)
            // the synthesized Filter — each tagged with a broadcast_role
            // attribute. The badge text mirrors that role so the user can
            // tell at a glance which side of the broadcast they are looking at.
            if (node.attributes && node.attributes.broadcast_rewrite === 'true') {
                const count = node.attributes.broadcast_predicate_value_count;
                const role = node.attributes.broadcast_role || 'filter';
                const suffix = count ? count + ' keys' : 'rewrite';
                let prefix;
                switch (role) {
                    case 'remote_scan': prefix = 'Broadcast IN ↓ '; break;
                    case 'local_scan':  prefix = 'Broadcast keys ↑ '; break;
                    case 'join':        prefix = 'Broadcast ⇆ '; break;
                    case 'filter':      prefix = 'Broadcast: '; break;
                    default:            prefix = 'Broadcast: ';
                }
                lines.push({ text: prefix + suffix, cls: 'node-broadcast' });
            }
            // Sort-pushdown badge — appears on both the Sort node and the
            // TableScan it feeds. The daemon stamps the attribute on a remote
            // TableScan whenever its captured remote_sql contains ORDER BY,
            // so users see exactly which scan is returning pre-sorted rows.
            if (node.attributes && node.attributes.sort_pushed_down === 'true') {
                lines.push({ text: 'Sort ↓ pushed', cls: 'node-sort-pushed' });
            }
            return lines.slice(0, 6);
        }

        function nodeTooltip(node) {
            // Hover tooltip surfaces full attributes (predicates, sorts,
            // projection lists) that get truncated in the visible node body.
            // Native SVG <title> works inside webviews without extra JS or
            // CSP relaxation.
            const lines = [node.node_type];
            if (node.label) lines.push(node.label);
            const attrs = node.attributes || {};
            Object.keys(attrs).sort().forEach(function(key) {
                const value = attrs[key];
                if (value !== undefined && value !== '') {
                    lines.push(key + ': ' + value);
                }
            });
            if (node.remote_sql) {
                lines.push('remote_sql: ' + node.remote_sql);
            }
            return lines.join('\\n');
        }

        function slugForId(value) {
            return String(value || '').replace(/[^A-Za-z0-9_-]+/g, '-');
        }

        function renderTree() {
            if (treeState.rendered || !graph.root_ids || graph.root_ids.length === 0) return;
            treeState.rendered = true;

            const viewport = document.getElementById('plan-viewport');
            const nodeWidth = 280;
            const nodeHeight = 112;
            const levelHeight = 152;
            const siblingSpacing = 36;
            const layouts = {};
            let maxLevel = 0;

            function measure(nodeId, level) {
                const node = graph.nodes[nodeId];
                if (!node) return 0;
                maxLevel = Math.max(maxLevel, level);
                let width = nodeWidth;
                if (node.children && node.children.length > 0) {
                    let childWidth = 0;
                    node.children.forEach(function(childId, index) {
                        childWidth += measure(childId, level + 1);
                        if (index < node.children.length - 1) childWidth += siblingSpacing;
                    });
                    width = Math.max(nodeWidth, childWidth);
                }
                layouts[nodeId] = { id: nodeId, node: node, level: level, width: width, x: 0, y: level * levelHeight + 24 };
                return width;
            }

            function position(nodeId, left) {
                const layout = layouts[nodeId];
                if (!layout) return;
                layout.x = left + (layout.width - nodeWidth) / 2;
                const node = layout.node;
                if (node.children && node.children.length > 0) {
                    let childLeft = left;
                    node.children.forEach(function(childId, index) {
                        position(childId, childLeft);
                        childLeft += layouts[childId].width;
                        if (index < node.children.length - 1) childLeft += siblingSpacing;
                    });
                }
            }

            let cursor = 32;
            graph.root_ids.forEach(function(rootId) {
                const width = measure(rootId, 0);
                position(rootId, cursor);
                cursor += width + siblingSpacing;
            });

            treeState.width = Math.max(1, cursor + 32);
            treeState.height = Math.max(1, (maxLevel + 1) * levelHeight + nodeHeight + 48);

            let svgContent = '';
            Object.keys(layouts).forEach(function(id) {
                const parent = layouts[id];
                const node = parent.node;
                if (!node.children) return;
                node.children.forEach(function(childId) {
                    const child = layouts[childId];
                    if (!child) return;
                    const startX = parent.x + nodeWidth / 2;
                    const startY = parent.y + nodeHeight;
                    const endX = child.x + nodeWidth / 2;
                    const endY = child.y;
                    svgContent += '<path class="edge" d="M' + startX + ',' + startY + ' C' + startX + ',' + (startY + 32) + ' ' + endX + ',' + (endY - 32) + ' ' + endX + ',' + endY + '" />';
                });
            });

            Object.keys(layouts).forEach(function(id) {
                const layout = layouts[id];
                const node = layout.node;
                const isTableScan = node.node_type === 'TableScan';
                const className = isTableScan ? 'plan-node table-scan' : 'plan-node';
                // data-source-ref lets the click handler look up the
                // matching Source-tab card without re-parsing the node label.
                const dataAttr = isTableScan && node.source_ref
                    ? ' data-source-ref="' + escapeHtml(node.source_ref) + '"'
                    : '';
                svgContent += '<g class="' + className + '" transform="translate(' + layout.x + ', ' + layout.y + ')"' + dataAttr + '>';
                svgContent += '<title>' + escapeHtml(nodeTooltip(node)) + '</title>';
                svgContent += '<rect class="node-rect" width="' + nodeWidth + '" height="' + nodeHeight + '" />';
                // Provider icon: TableScan nodes get a 16x16 glyph at the
                // top-left so users can tell Postgres from MySQL from CSV at
                // a glance. We embedded each kind as a <symbol> in the SVG
                // <defs> at the root; reference it via <use href="#icon-…">.
                if (isTableScan) {
                    const kind = node.provider_kind || 'unknown';
                    svgContent += '<use class="plan-node-icon" href="#icon-' + escapeHtml(kind) + '" x="8" y="8" width="16" height="16"/>';
                }
                nodeLines(node).forEach(function(line, index) {
                    const y = 22 + index * 18;
                    svgContent += '<text class="node-text ' + line.cls + '" x="12" y="' + y + '">' + escapeHtml(shortText(line.text, 48)) + '</text>';
                });
                svgContent += '</g>';
            });

            viewport.innerHTML = svgContent;
            fitTree();
        }

        function applyTreeTransform() {
            document.getElementById('plan-viewport').setAttribute(
                'transform',
                'translate(' + treeState.x + ',' + treeState.y + ') scale(' + treeState.scale + ')'
            );
        }

        function clampZoom(value) {
            return Math.max(0.25, Math.min(2.5, value));
        }

        function zoomTree(factor) {
            const container = document.getElementById('tree-container');
            const rect = container.getBoundingClientRect();
            const centerX = rect.width / 2;
            const centerY = rect.height / 2;
            const oldScale = treeState.scale;
            const nextScale = clampZoom(oldScale * factor);
            if (nextScale === oldScale) return;
            const ratio = nextScale / oldScale;
            treeState.x = centerX - (centerX - treeState.x) * ratio;
            treeState.y = centerY - (centerY - treeState.y) * ratio;
            treeState.scale = nextScale;
            applyTreeTransform();
        }

        function fitTree() {
            renderTree();
            const container = document.getElementById('tree-container');
            const width = Math.max(1, container.clientWidth);
            const height = Math.max(1, container.clientHeight);
            const scale = clampZoom(Math.min((width - 48) / treeState.width, (height - 48) / treeState.height));
            treeState.scale = scale;
            treeState.x = Math.max(16, (width - treeState.width * scale) / 2);
            treeState.y = 24;
            applyTreeTransform();
        }

        function resetTree() {
            treeState.scale = 1;
            treeState.x = 24;
            treeState.y = 24;
            applyTreeTransform();
        }

        document.getElementById('tree-zoom-in').addEventListener('click', function() { zoomTree(1.2); });
        document.getElementById('tree-zoom-out').addEventListener('click', function() { zoomTree(1 / 1.2); });
        document.getElementById('tree-fit').addEventListener('click', fitTree);
        document.getElementById('tree-reset').addEventListener('click', resetTree);

        // Phase 10 — metrics overlay toggle. We disable the button entirely
        // when no plan node carries an actual_rows field (the response
        // came from a planner-only EXPLAIN, not from an ANALYZE run). The
        // tooltip explains how to enable it.
        (function setupMetricsToggle() {
            const btn = document.getElementById('metrics-toggle');
            if (!btn) return;
            const anyRuntime = Object.keys(graph.nodes || {}).some(function(id) {
                const m = graph.nodes[id] && graph.nodes[id].metrics;
                return m && hasNumber(m.actual_rows);
            });
            if (!anyRuntime) {
                btn.disabled = true;
                btn.title = 'Re-run with Explain (ANALYZE) to see runtime metrics';
                return;
            }
            btn.addEventListener('click', function() {
                metricsOverlayOn = !metricsOverlayOn;
                btn.setAttribute('aria-pressed', metricsOverlayOn ? 'true' : 'false');
                // Force a fresh render — treeState.rendered is the latch
                // renderTree uses to skip repeated layout computation.
                treeState.rendered = false;
                renderTree();
            });
        })();

        const treeContainer = document.getElementById('tree-container');
        treeContainer.addEventListener('mousedown', function(event) {
            if (event.button !== 0) return;
            panState = {
                x: event.clientX,
                y: event.clientY,
                startX: treeState.x,
                startY: treeState.y,
                moved: false,
            };
            treeContainer.classList.add('dragging');
            event.preventDefault();
        });
        window.addEventListener('mousemove', function(event) {
            if (!panState) return;
            const dx = event.clientX - panState.x;
            const dy = event.clientY - panState.y;
            if (Math.abs(dx) > 3 || Math.abs(dy) > 3) {
                panState.moved = true;
            }
            treeState.x = panState.startX + dx;
            treeState.y = panState.startY + dy;
            applyTreeTransform();
        });
        window.addEventListener('mouseup', function(event) {
            const wasClick = panState && !panState.moved;
            panState = null;
            treeContainer.classList.remove('dragging');
            if (!wasClick) return;
            // Treat a drag-free mousedown→mouseup as a click. If it landed
            // inside a TableScan group, jump to the matching Source-tab card.
            const target = event.target;
            if (!(target instanceof Element)) return;
            const scanGroup = target.closest('g.plan-node.table-scan');
            if (!scanGroup) return;
            const sourceRef = scanGroup.getAttribute('data-source-ref');
            if (!sourceRef) return;
            revealSourceCard(sourceRef);
        });
        treeContainer.addEventListener('wheel', function(event) {
            event.preventDefault();
            zoomTree(event.deltaY < 0 ? 1.08 : 1 / 1.08);
        }, { passive: false });

        function revealSourceCard(sourceRef) {
            // Programmatically switch to the Source tab and scroll the
            // matching card into view. Briefly highlight it so the user
            // sees what they jumped to.
            const tabs = document.querySelectorAll('.tab');
            tabs.forEach(function(t) {
                t.classList.toggle('active', t.dataset.target === 'source-tab');
            });
            document.querySelectorAll('.tab-content').forEach(function(c) {
                c.classList.toggle('active', c.id === 'source-tab');
            });
            const card = document.getElementById('source-card-' + slugForId(sourceRef));
            if (!card) return;
            // Force expanded state on the global Logical/Physical sections so
            // the user's jump-target is visible without an extra click.
            const card_parent = card.parentElement;
            if (card_parent) {
                card.scrollIntoView({ block: 'start', behavior: 'smooth' });
            }
            card.classList.add('highlight');
            setTimeout(function() { card.classList.remove('highlight'); }, 1400);
        }

        function renderTable() {
            const flatNodes = [];
            function traverse(nodeId, depth) {
                const node = graph.nodes[nodeId];
                if (!node) return;
                flatNodes.push({ node: node, depth: depth });
                if (node.children) node.children.forEach(function(child) { traverse(child, depth + 1); });
            }
            if (graph.root_ids) {
                graph.root_ids.forEach(function(id) { traverse(id, 0); });
            } else {
                Object.keys(graph.nodes).forEach(function(id) {
                    flatNodes.push({ node: graph.nodes[id], depth: 0 });
                });
            }
            window.flatPlanNodes = flatNodes;
            updateTableRows(flatNodes);
        }

        function metricOrDash(metrics) {
            return metrics && hasNumber(metrics.estimated_rows) ? String(metrics.estimated_rows) : '-';
        }

        function updateTableRows(nodesList) {
            const tbody = document.getElementById('plan-table-body');
            tbody.innerHTML = nodesList.map(function(item) {
                const node = item.node;
                const indent = '<span class="indent"></span>'.repeat(item.depth);
                return '<tr>' +
                    '<td>' + indent + '<strong>' + escapeHtml(node.node_type) + '</strong></td>' +
                    '<td>' + escapeHtml(displayTitle(node)) + '</td>' +
                    '<td>' + escapeHtml(metricOrDash(node.metrics)) + '</td>' +
                    '</tr>';
            }).join('');
        }

        document.getElementById('table-search').addEventListener('input', function(event) {
            const q = event.target.value.toLowerCase();
            if (!q) {
                updateTableRows(window.flatPlanNodes);
                return;
            }
            const filtered = window.flatPlanNodes.filter(function(item) {
                return item.node.node_type.toLowerCase().includes(q) ||
                    displayTitle(item.node).toLowerCase().includes(q);
            });
            updateTableRows(filtered);
        });

        renderTable();
        renderTree();
    </script>
</body>
</html>`;
}

export class PlanVisualizationPanel {
    public static currentPanel: PlanVisualizationPanel | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private _disposables: vscode.Disposable[] = [];

    private constructor(panel: vscode.WebviewPanel, extensionUri: vscode.Uri) {
        this._panel = panel;
        this._extensionUri = extensionUri;

        this._panel.webview.onDidReceiveMessage(async (message) => {
            if (message?.command === 'copyPlan' && typeof message.text === 'string') {
                await vscode.env.clipboard.writeText(message.text);
            }
        }, null, this._disposables);

        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);
    }

    public static createOrShow(extensionUri: vscode.Uri, title: string = 'Query Plan') {
        const column = vscode.window.activeTextEditor ? vscode.ViewColumn.Beside : vscode.ViewColumn.One;

        if (PlanVisualizationPanel.currentPanel) {
            PlanVisualizationPanel.currentPanel._panel.reveal(column);
            PlanVisualizationPanel.currentPanel._panel.title = title;
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'qsqlPlanVisualization',
            title,
            column,
            {
                enableScripts: true,
                localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'media')]
            }
        );

        PlanVisualizationPanel.currentPanel = new PlanVisualizationPanel(panel, extensionUri);
    }

    public updatePlan(result: ExplainQueryResult) {
        this._panel.webview.html = this._getHtmlForWebview(result);
    }

    public updateError(error: string) {
        this._panel.webview.html = `
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Error</title>
                <style>
                    body { font-family: var(--vscode-font-family); color: var(--vscode-editorError-foreground); padding: 20px; }
                </style>
            </head>
            <body>
                <h2>Failed to Generate Query Plan</h2>
                <pre>${escapePlanHtml(error)}</pre>
            </body>
            </html>
        `;
    }

    private dispose() {
        PlanVisualizationPanel.currentPanel = undefined;
        this._panel.dispose();
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) x.dispose();
        }
    }

    private _getHtmlForWebview(result: ExplainQueryResult): string {
        return renderPlanVisualizationHtml(result, this._getNonce());
    }

    private _getNonce() {
        let text = '';
        const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        for (let i = 0; i < 32; i++) {
            text += possible.charAt(Math.floor(Math.random() * possible.length));
        }
        return text;
    }
}
