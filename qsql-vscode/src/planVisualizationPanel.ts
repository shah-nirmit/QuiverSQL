import * as vscode from 'vscode';
import { ExplainQueryResult } from './models';

export class PlanVisualizationPanel {
    public static currentPanel: PlanVisualizationPanel | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private _disposables: vscode.Disposable[] = [];

    private constructor(panel: vscode.WebviewPanel, extensionUri: vscode.Uri) {
        this._panel = panel;
        this._extensionUri = extensionUri;

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
                <pre>${this._escapeHtml(error)}</pre>
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

    private _escapeHtml(unsafe: string): string {
        if (!unsafe) return '';
        return unsafe
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }

    private _getHtmlForWebview(result: ExplainQueryResult): string {
        const nonce = this._getNonce();
        
        // Serialize graph for the client side script
        const graphJson = JSON.stringify(result.federated_plan).replace(/</g, '\\u003c');
        const sourcePlansJson = JSON.stringify(result.source_plans || {}).replace(/</g, '\\u003c');
        let rawText = result.raw;
        if (rawText && rawText.length > 50000) {
            rawText = rawText.substring(0, 50000) + "\n\n... [TRUNCATED - PLAN TOO LARGE] ...";
        }
        rawText = this._escapeHtml(rawText);
        const warningsHtml = result.warnings && result.warnings.length > 0 
            ? `<div class="warning-banner"><strong>Warnings:</strong><ul>${result.warnings.map(w => `<li>${this._escapeHtml(w)}</li>`).join('')}</ul></div>`
            : '';
        const truncatedHtml = result.federated_plan.truncated
            ? `<div class="warning-banner"><strong>Warning:</strong> The plan graph was truncated because it is too large.</div>`
            : '';

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline' var(--vscode-editor-background); script-src 'nonce-${nonce}';">
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
            overflow: auto;
            position: relative;
        }
        .tab-content.active {
            display: flex;
            flex-direction: column;
        }

        /* SVG Tree Styles */
        #tree-container {
            flex: 1;
            overflow: auto;
            position: relative;
            background: var(--vscode-editor-background);
        }
        .node-rect {
            fill: var(--vscode-editorWidget-background);
            stroke: var(--vscode-editorWidget-border);
            stroke-width: 1.5;
            rx: 4;
            ry: 4;
            cursor: pointer;
        }
        .node-rect:hover {
            stroke: var(--vscode-focusBorder);
        }
        .node-text {
            fill: var(--vscode-editor-foreground);
            font-family: var(--vscode-font-family);
            font-size: 12px;
            pointer-events: none;
        }
        .node-type {
            font-weight: bold;
            fill: var(--vscode-symbolIcon-classForeground);
        }
        .node-metrics {
            fill: var(--vscode-descriptionForeground);
            font-size: 10px;
        }
        .edge {
            fill: none;
            stroke: var(--vscode-editorIndentGuide-background);
            stroke-width: 2;
        }
        
        /* Table Styles */
        #table-container {
            flex: 1;
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

        /* Source Styles */
        #source-container {
            flex: 1;
            overflow: auto;
            padding: 10px;
        }
        pre {
            margin: 0;
            white-space: pre-wrap;
            font-family: var(--vscode-editor-font-family, monospace);
        }
        .section-title {
            font-weight: bold;
            margin-top: 15px;
            margin-bottom: 5px;
            border-bottom: 1px solid var(--vscode-panel-border);
            padding-bottom: 4px;
        }
        
        /* Details Panel */
        #details-panel {
            position: absolute;
            right: 0;
            top: 0;
            bottom: 0;
            width: 300px;
            background-color: var(--vscode-sideBar-background);
            border-left: 1px solid var(--vscode-sideBar-border);
            padding: 15px;
            overflow-y: auto;
            transform: translateX(100%);
            transition: transform 0.2s ease;
            box-shadow: -2px 0 5px rgba(0,0,0,0.1);
        }
        #details-panel.open {
            transform: translateX(0);
        }
        .close-btn {
            float: right;
            cursor: pointer;
            font-weight: bold;
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
        <div id="tree-container">
            <svg id="plan-svg" width="100%" height="100%"></svg>
        </div>
        <div id="details-panel">
            <div class="close-btn" id="close-details">x</div>
            <h3 id="details-title">Node Details</h3>
            <div id="details-content"></div>
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
            <div class="section-title">Federated Logical Plan</div>
            <pre>${rawText}</pre>
            <div id="native-plans-container"></div>
        </div>
    </div>

    <script nonce="${nonce}">
        const graph = ${graphJson};
        const sourcePlans = ${sourcePlansJson};
        
        // Tab switching
        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                
                tab.classList.add('active');
                document.getElementById(tab.dataset.target).classList.add('active');
                
                if (tab.dataset.target === 'tree-tab') {
                    renderTree();
                }
            });
        });
        
        // Helper: format metrics
        function formatMetrics(metrics) {
            if (!metrics) return '';
            let s = [];
            if (metrics.estimated_rows !== undefined) s.push(\`Rows: \${metrics.estimated_rows}\`);
            if (metrics.startup_cost !== undefined) s.push(\`Cost: \${metrics.startup_cost}..\${metrics.total_cost}\`);
            return s.join(' | ');
        }
        
        // Tree Rendering (Vanilla JS SVG)
        let treeRendered = false;
        function renderTree() {
            if (treeRendered || !graph.root_ids || graph.root_ids.length === 0) return;
            treeRendered = true;
            
            const svg = document.getElementById('plan-svg');
            const NODE_WIDTH = 200;
            const NODE_HEIGHT = 60;
            const LEVEL_HEIGHT = 100;
            const SIBLING_SPACING = 20;
            
            let nodesArray = [];
            let levels = {};
            let maxLevel = 0;
            
            // Layout calculations
            function computeLayout(nodeId, level, xOffset) {
                const node = graph.nodes[nodeId];
                if (!node) return 0;
                
                if (!levels[level]) levels[level] = [];
                const nodeLayout = { id: nodeId, node, level, x: 0, y: level * LEVEL_HEIGHT + 20, width: 0 };
                
                if (!node.children || node.children.length === 0) {
                    nodeLayout.width = NODE_WIDTH;
                } else {
                    let childrenWidth = 0;
                    for (let i = 0; i < node.children.length; i++) {
                        childrenWidth += computeLayout(node.children[i], level + 1, xOffset + childrenWidth);
                        if (i < node.children.length - 1) childrenWidth += SIBLING_SPACING;
                    }
                    nodeLayout.width = Math.max(NODE_WIDTH, childrenWidth);
                }
                
                levels[level].push(nodeLayout);
                maxLevel = Math.max(maxLevel, level);
                return nodeLayout.width;
            }
            
            let totalWidth = 0;
            for (let rootId of graph.root_ids) {
                totalWidth += computeLayout(rootId, 0, totalWidth);
                totalWidth += SIBLING_SPACING;
            }
            
            // Second pass: position nodes
            let placed = {};
            function positionNodes(nodeId, xCenter) {
                if (placed[nodeId]) return placed[nodeId];
                
                const node = graph.nodes[nodeId];
                if (!node) return null;
                
                // find node layout
                let layout = null;
                for (let lvl in levels) {
                    let l = levels[lvl].find(n => n.id === nodeId);
                    if (l) { layout = l; break; }
                }
                
                layout.x = xCenter - (NODE_WIDTH / 2);
                placed[nodeId] = layout;
                
                if (node.children && node.children.length > 0) {
                    let childrenTotalWidth = 0;
                    let childrenLayouts = [];
                    for (let cid of node.children) {
                        for (let lvl in levels) {
                            let cl = levels[lvl].find(n => n.id === cid);
                            if (cl) {
                                childrenLayouts.push(cl);
                                childrenTotalWidth += cl.width;
                                break;
                            }
                        }
                    }
                    
                    let startX = xCenter - (childrenTotalWidth + (childrenLayouts.length-1)*SIBLING_SPACING) / 2;
                    for (let i = 0; i < node.children.length; i++) {
                        let cwidth = childrenLayouts[i].width;
                        positionNodes(node.children[i], startX + cwidth / 2);
                        startX += cwidth + SIBLING_SPACING;
                    }
                }
                
                return layout;
            }
            
            let rootStartX = totalWidth / 2;
            for (let rootId of graph.root_ids) {
                positionNodes(rootId, rootStartX);
                // just simplified, assuming single root mostly
            }
            
            // Draw
            let svgContent = '';
            
            // Draw edges
            for (let id in placed) {
                let p = placed[id];
                let node = p.node;
                if (node.children) {
                    for (let cid of node.children) {
                        let cp = placed[cid];
                        if (cp) {
                            const startX = p.x + NODE_WIDTH / 2;
                            const startY = p.y + NODE_HEIGHT;
                            const endX = cp.x + NODE_WIDTH / 2;
                            const endY = cp.y;
                            svgContent += \`<path class="edge" d="M\${startX},\${startY} C\${startX},\${startY+20} \${endX},\${endY-20} \${endX},\${endY}" />\`;
                        }
                    }
                }
            }
            
            // Draw nodes
            for (let id in placed) {
                let p = placed[id];
                let node = p.node;
                
                let metricsText = formatMetrics(node.metrics);
                
                svgContent += \`
                    <g transform="translate(\${p.x}, \${p.y})" onclick="showDetails('\${id}')">
                        <rect class="node-rect" width="\${NODE_WIDTH}" height="\${NODE_HEIGHT}" />
                        <text class="node-text node-type" x="10" y="20">\${escapeHtml(node.node_type)}</text>
                        <text class="node-text" x="10" y="38" textLength="180" lengthAdjust="spacingAndGlyphs">\${escapeHtml(node.label)}</text>
                        <text class="node-text node-metrics" x="10" y="52">\${escapeHtml(metricsText)}</text>
                    </g>
                \`;
            }
            
            svg.innerHTML = svgContent;
            
            // Resize SVG
            svg.setAttribute('width', Math.max(800, totalWidth + 100));
            svg.setAttribute('height', (maxLevel + 2) * LEVEL_HEIGHT);
        }
        
        // Table Rendering
        function renderTable() {
            const tbody = document.getElementById('plan-table-body');
            let rows = '';
            
            // Flatten tree for table
            let flatNodes = [];
            function traverse(nodeId, depth) {
                const node = graph.nodes[nodeId];
                if (!node) return;
                flatNodes.push({ node, depth });
                if (node.children) {
                    node.children.forEach(c => traverse(c, depth + 1));
                }
            }
            
            if (graph.root_ids) {
                graph.root_ids.forEach(id => traverse(id, 0));
            } else {
                // fallback to unordered if roots missing
                for (let id in graph.nodes) {
                    flatNodes.push({ node: graph.nodes[id], depth: 0 });
                }
            }
            
            window.flatPlanNodes = flatNodes; // for search
            
            updateTableRows(flatNodes);
        }
        
        function updateTableRows(nodesList) {
            const tbody = document.getElementById('plan-table-body');
            tbody.innerHTML = nodesList.map(item => {
                const node = item.node;
                const indent = '<span class="indent"></span>'.repeat(item.depth);
                return \`
                    <tr>
                        <td>\${indent}<strong>\${escapeHtml(node.node_type)}</strong></td>
                        <td>\${escapeHtml(node.label)}</td>
                        <td>\${node.metrics && node.metrics.estimated_rows ? node.metrics.estimated_rows : '-'}</td>
                    </tr>
                \`;
            }).join('');
        }
        
        document.getElementById('table-search').addEventListener('input', (e) => {
            const q = e.target.value.toLowerCase();
            if (!q) {
                updateTableRows(window.flatPlanNodes);
                return;
            }
            const filtered = window.flatPlanNodes.filter(item => 
                item.node.node_type.toLowerCase().includes(q) || 
                item.node.label.toLowerCase().includes(q)
            );
            updateTableRows(filtered);
        });
        
        // Source native plans render
        function renderSourcePlans() {
            const container = document.getElementById('native-plans-container');
            if (!sourcePlans || Object.keys(sourcePlans).length === 0) return;
            
            let html = '<div class="section-title">Native Source Plans</div>';
            for (let sourceRef in sourcePlans) {
                html += \`<strong>\${escapeHtml(sourceRef)}</strong>\`;
                html += \`<pre>\${escapeHtml(JSON.stringify(sourcePlans[sourceRef], null, 2))}</pre>\`;
            }
            container.innerHTML = html;
        }
        
        // Details panel
        window.showDetails = function(nodeId) {
            const node = graph.nodes[nodeId];
            if (!node) return;
            
            document.getElementById('details-title').innerText = node.node_type;
            
            let content = \`<p><strong>Label:</strong> \${escapeHtml(node.label)}</p>\`;
            
            if (node.metrics) {
                content += '<h4>Metrics</h4><ul>';
                for (let k in node.metrics) {
                    content += \`<li><strong>\${k}:</strong> \${node.metrics[k]}</li>\`;
                }
                content += '</ul>';
            }
            
            if (node.attributes && Object.keys(node.attributes).length > 0) {
                content += '<h4>Attributes</h4><ul>';
                for (let k in node.attributes) {
                    content += \`<li><strong>\${k}:</strong> \${escapeHtml(node.attributes[k])}</li>\`;
                }
                content += '</ul>';
            }
            
            if (node.native_plan_ref && sourcePlans[node.native_plan_ref]) {
                content += '<h4>Native Plan</h4>';
                content += \`<pre style="font-size: 10px;">\${escapeHtml(JSON.stringify(sourcePlans[node.native_plan_ref], null, 2))}</pre>\`;
            }
            
            document.getElementById('details-content').innerHTML = content;
            document.getElementById('details-panel').classList.add('open');
        };
        
        document.getElementById('close-details').addEventListener('click', () => {
            document.getElementById('details-panel').classList.remove('open');
        });
        
        function escapeHtml(str) {
            if (typeof str !== 'string') return '';
            return str
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }
        
        // Initial render
        renderTable();
        renderSourcePlans();
        if (document.querySelector('.tab[data-target="tree-tab"]').classList.contains('active')) {
            renderTree();
        }
    </script>
</body>
</html>`;
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
