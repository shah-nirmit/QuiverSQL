import * as vscode from 'vscode';
import { ExplainQueryResult, PlanMetrics, PlanNode } from './models';

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

export function renderPlanVisualizationHtml(result: ExplainQueryResult, nonce: string): string {
    const graphJson = JSON.stringify(result.federated_plan).replace(/</g, '\\u003c');
    const sourcePlans = result.source_plans || {};
    const sourcePlansJson = JSON.stringify(sourcePlans).replace(/</g, '\\u003c');
    const rawPlan = result.raw || '';
    let rawText = rawPlan;
    if (rawText.length > 50000) {
        rawText = rawText.substring(0, 50000) + "\n\n... [TRUNCATED - PLAN TOO LARGE] ...";
    }

    const copyPayloads: Record<string, string> = {
        logical: rawText
    };

    const nativePlanEntries = Object.entries(sourcePlans).sort(([left], [right]) => left.localeCompare(right));
    const nativePlansHtml = nativePlanEntries.map(([sourceRef, sourcePlan]) => {
        const copyKey = `native:${sourceRef}`;
        const display = formatPlanForDisplay(sourcePlan);
        copyPayloads[copyKey] = display;
        return `
            <section class="plan-source-section">
                <div class="section-heading">
                    <span>${escapePlanHtml(sourceRef)}</span>
                    <button class="icon-button copy-button" data-copy-key="${escapePlanHtml(copyKey)}" title="Copy native source plan" aria-label="Copy native source plan">&#x2398;</button>
                </div>
                <pre>${escapePlanHtml(display)}</pre>
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
            <span class="toolbar-label">Drag the canvas to pan</span>
        </div>
        <div id="tree-container">
            <svg id="plan-svg" role="img" aria-label="Federated query plan tree">
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
            <div class="section-heading">
                <span>Federated Logical Plan</span>
                <button class="icon-button copy-button" data-copy-key="logical" title="Copy logical plan" aria-label="Copy logical plan">&#x2398;</button>
            </div>
            <pre>${escapePlanHtml(rawText)}</pre>
            ${nativePlanEntries.length > 0 ? `<div class="section-heading"><span>Native Source Plans</span></div>${nativePlansHtml}` : ''}
        </div>
    </div>

    <script nonce="${nonce}">
        const vscodeApi = typeof acquireVsCodeApi === 'function' ? acquireVsCodeApi() : null;
        const graph = ${graphJson};
        const sourcePlans = ${sourcePlansJson};
        const copyPayloads = ${copyPayloadsJson};
        const treeState = { rendered: false, scale: 1, x: 24, y: 24, width: 1, height: 1 };
        let panState = null;

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

        function nodeLines(node) {
            const lines = [{ text: node.node_type, cls: 'node-type' }];
            wrapText(displayTitle(node), 42, 2).forEach(function(line) {
                if (line) lines.push({ text: line, cls: 'node-text' });
            });
            const details = displayDetails(node);
            if (details) lines.push({ text: shortText(details, 46), cls: 'node-muted' });
            const metrics = formatMetrics(node.metrics);
            if (metrics) lines.push({ text: metrics, cls: 'node-muted' });
            if (node.native_plan_ref && sourcePlans[node.native_plan_ref]) {
                lines.push({ text: 'Native plan available', cls: 'node-native' });
            }
            return lines.slice(0, 5);
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
                const className = node.node_type === 'TableScan' ? 'plan-node table-scan' : 'plan-node';
                svgContent += '<g class="' + className + '" transform="translate(' + layout.x + ', ' + layout.y + ')">';
                svgContent += '<rect class="node-rect" width="' + nodeWidth + '" height="' + nodeHeight + '" />';
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

        const treeContainer = document.getElementById('tree-container');
        treeContainer.addEventListener('mousedown', function(event) {
            if (event.button !== 0) return;
            panState = { x: event.clientX, y: event.clientY, startX: treeState.x, startY: treeState.y };
            treeContainer.classList.add('dragging');
            event.preventDefault();
        });
        window.addEventListener('mousemove', function(event) {
            if (!panState) return;
            treeState.x = panState.startX + event.clientX - panState.x;
            treeState.y = panState.startY + event.clientY - panState.y;
            applyTreeTransform();
        });
        window.addEventListener('mouseup', function() {
            panState = null;
            treeContainer.classList.remove('dragging');
        });
        treeContainer.addEventListener('wheel', function(event) {
            event.preventDefault();
            zoomTree(event.deltaY < 0 ? 1.08 : 1 / 1.08);
        }, { passive: false });

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
