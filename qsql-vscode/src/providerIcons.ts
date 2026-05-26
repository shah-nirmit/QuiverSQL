import * as vscode from 'vscode';
import { SourceKind } from './models';

// Single source of truth for provider-specific icons + display labels, used
// by both the Data Sources tree view and the plan visualization webview.
//
// Each entry binds a `SourceKind` to:
//   - a relative path under `media/icons/` for the on-disk SVG, used as the
//     TreeItem `iconPath`. VS Code tints monochrome SVGs to match the theme;
//     our icons keep their brand-color accents because TreeItem icons stay
//     literal when the SVG declares explicit `fill`/`stroke` colors.
//   - an inline `<symbol>` body that we splat into the plan-graph SVG root
//     so each `TableScan` node can render `<use href="#icon-postgres" />`.
//     We inline rather than `<image href="…">` so the whole webview stays
//     self-contained and we keep the strict `default-src 'none'` CSP that
//     already locks down the panel.

export type IconKind = SourceKind | 'unknown';

interface ProviderIcon {
    readonly label: string;
    readonly file: string;
    readonly symbolBody: string;
    readonly accent: string;
}

const ICONS: Record<IconKind, ProviderIcon> = {
    postgres: {
        label: 'PostgreSQL',
        file: 'postgres.svg',
        accent: '#336791',
        symbolBody: `
            <g fill="none" stroke="#336791" stroke-width="1.1" stroke-linejoin="round" stroke-linecap="round">
                <path d="M8 2.2c2.4 0 4.7 1.1 4.7 3.6 0 1.4-.6 2.3-1.3 3 .6.4 1 1 1 1.7 0 1.4-1.5 2.4-3 2.8-.6.2-1.3.3-1.8.3-.7-.5-1.1-1.5-1.1-2.5"/>
                <path d="M6.2 13.5c-.6-.1-1.2-.4-1.7-.8-1.2-.9-1.7-2.6-1.7-4.3 0-1.3.3-2.6.9-3.6.4-.7 1.1-1.4 2-1.8"/>
                <path d="M7.3 4.5c-.2 1.5-.1 3 .2 4.2.2.7.4 1.3.7 1.8"/>
                <circle cx="6.4" cy="6.1" r=".55" fill="#336791" stroke="none"/>
            </g>`,
    },
    mysql: {
        label: 'MySQL',
        file: 'mysql.svg',
        accent: '#00758F',
        symbolBody: `
            <g fill="none" stroke="#00758F" stroke-width="1.1" stroke-linejoin="round" stroke-linecap="round">
                <path d="M1.8 9.5c1.2-1.6 3.4-2.8 5.6-2.8 2.5 0 4.6 1 6 2.7.4.5.9 1.1 1 1.8"/>
                <path d="M11 7.2c.4-.5.7-1 .8-1.6.1-.5 0-1.1-.4-1.5-.5-.5-1.4-.6-2.1-.3-.6.2-1.1.7-1.5 1.3"/>
                <path d="M2.4 9.7c.6.6 1.4 1 2.2 1.2.3.1.7.1 1 0"/>
                <circle cx="10.3" cy="5.6" r=".55" fill="#00758F" stroke="none"/>
            </g>
            <path d="M3 12.5l1.6.6.4-.6-.6-1.2" fill="none" stroke="#F29111" stroke-width="1" stroke-linecap="round"/>`,
    },
    mariadb: {
        label: 'MariaDB',
        file: 'mariadb.svg',
        accent: '#003545',
        symbolBody: `
            <g fill="none" stroke="#003545" stroke-width="1.1" stroke-linejoin="round" stroke-linecap="round">
                <path d="M2 11.5c.5-1.6 2-2.8 3.7-3.2 1.6-.4 3.4-.1 4.7.8.7.5 1.3 1.2 1.6 2"/>
                <path d="M10.5 9c1-.4 1.9-1.1 2.5-2 .3-.4.5-1 .3-1.5-.2-.5-.8-.7-1.3-.6-.7.1-1.3.6-1.7 1.2"/>
                <path d="M3.4 11.7c-.4.5-.5 1.2-.3 1.7"/>
                <circle cx="11.2" cy="6.5" r=".55" fill="#003545" stroke="none"/>
            </g>
            <path d="M5.5 12.5c.4-.4 1-.4 1.4 0" fill="none" stroke="#C0765A" stroke-width="1" stroke-linecap="round"/>`,
    },
    sqlite: {
        label: 'SQLite',
        file: 'sqlite.svg',
        accent: '#003B57',
        symbolBody: `
            <g fill="none" stroke="#003B57" stroke-width="1.1" stroke-linejoin="round" stroke-linecap="round">
                <path d="M2.5 4.5c0-1 1.6-1.8 3.7-1.8h4.3c1.6 0 2.9.5 2.9 1.2v7.3c0 .8-1.3 1.3-2.9 1.3H6.2c-2.1 0-3.7-.7-3.7-1.7z"/>
                <path d="M5 6.5c.8-.6 1.7-1 2.7-1.1 1.5-.2 3 .3 3.9 1.2"/>
                <path d="M5.4 9.5c.4-.6 1-.9 1.7-1 .8-.1 1.6.2 2.2.7"/>
                <path d="M11.5 3.2l1.5 1.5-3.3 6.3-1.3.2.5-1.2z" fill="#003B57"/>
            </g>`,
    },
    csv: {
        label: 'CSV',
        file: 'csv.svg',
        accent: '#4CAF50',
        symbolBody: `
            <g fill="none" stroke="#4CAF50" stroke-width="1.1" stroke-linejoin="round" stroke-linecap="round">
                <rect x="2" y="3" width="12" height="10" rx="1.2"/>
                <path d="M2 6.5h12"/>
                <path d="M2 9.5h12"/>
                <path d="M6 3v10"/>
                <path d="M10 3v10"/>
            </g>`,
    },
    ndjson: {
        label: 'NDJSON',
        file: 'ndjson.svg',
        accent: '#FFB300',
        symbolBody: `
            <g fill="none" stroke="#FFB300" stroke-width="1.1" stroke-linejoin="round" stroke-linecap="round">
                <path d="M4 3.2c-.6 0-1 .4-1 1v1.5c0 .5-.3.8-.8.8v1c.5 0 .8.3.8.8v1.5c0 .6.4 1 1 1"/>
                <path d="M12 3.2c.6 0 1 .4 1 1v1.5c0 .5.3.8.8.8v1c-.5 0-.8.3-.8.8v1.5c0 .6-.4 1-1 1"/>
                <path d="M6 5.5h4"/>
                <path d="M6 8h4"/>
                <path d="M6 10.5h4"/>
            </g>`,
    },
    json: {
        label: 'JSON',
        file: 'json.svg',
        accent: '#FB8C00',
        symbolBody: `
            <g fill="none" stroke="#FB8C00" stroke-width="1.2" stroke-linejoin="round" stroke-linecap="round">
                <path d="M5.5 3c-1.4 0-2.2.7-2.2 2v1.7c0 .8-.5 1.2-1.3 1.3.8.1 1.3.5 1.3 1.3V11c0 1.3.8 2 2.2 2"/>
                <path d="M10.5 3c1.4 0 2.2.7 2.2 2v1.7c0 .8.5 1.2 1.3 1.3-.8.1-1.3.5-1.3 1.3V11c0 1.3-.8 2-2.2 2"/>
            </g>`,
    },
    parquet: {
        label: 'Parquet',
        file: 'parquet.svg',
        accent: '#7E57C2',
        symbolBody: `
            <g fill="#7E57C2">
                <rect x="2" y="4" width="2.4" height="9" rx=".4"/>
                <rect x="5.4" y="2" width="2.4" height="11" rx=".4" opacity=".82"/>
                <rect x="8.8" y="5" width="2.4" height="8" rx=".4" opacity=".7"/>
                <rect x="12.2" y="3.5" width="2.4" height="9.5" rx=".4" opacity=".58"/>
            </g>`,
    },
    fixed_width: {
        label: 'Fixed-width',
        file: 'fixed-width.svg',
        accent: '#607D8B',
        symbolBody: `
            <g fill="#607D8B">
                <rect x="2"  y="3"   width="3" height="1.4" rx=".3"/>
                <rect x="5.5" y="3"  width="3" height="1.4" rx=".3"/>
                <rect x="9"  y="3"   width="3" height="1.4" rx=".3"/>
                <rect x="2"  y="5.6" width="3" height="1.4" rx=".3"/>
                <rect x="5.5" y="5.6" width="3" height="1.4" rx=".3"/>
                <rect x="9"  y="5.6" width="3" height="1.4" rx=".3"/>
                <rect x="2"  y="8.2" width="3" height="1.4" rx=".3"/>
                <rect x="5.5" y="8.2" width="3" height="1.4" rx=".3"/>
                <rect x="9"  y="8.2" width="3" height="1.4" rx=".3"/>
                <rect x="2"  y="10.8" width="3" height="1.4" rx=".3"/>
                <rect x="5.5" y="10.8" width="3" height="1.4" rx=".3"/>
                <rect x="9"  y="10.8" width="3" height="1.4" rx=".3"/>
            </g>`,
    },
    unknown: {
        label: 'Unknown',
        file: '',
        accent: 'var(--vscode-descriptionForeground)',
        symbolBody: `
            <g fill="none" stroke="currentColor" stroke-width="1.1" stroke-linejoin="round" stroke-linecap="round">
                <rect x="2.5" y="3" width="11" height="10" rx="1.2"/>
                <path d="M6 7c0-1 .9-1.7 2-1.7s2 .7 2 1.7c0 .8-.6 1.2-1.3 1.5-.5.2-.7.6-.7 1.1"/>
                <circle cx="8" cy="11" r=".55" fill="currentColor" stroke="none"/>
            </g>`,
    },
};

/** Display label for a kind ("PostgreSQL", "MySQL", …). Useful for tooltips. */
export function labelFor(kind: string | undefined): string {
    return ICONS[normalize(kind)].label;
}

/** Symbol id consumed inside the plan-graph SVG as `<use href="#icon-…">`. */
export function iconSymbolIdFor(kind: string | undefined): string {
    return `icon-${normalize(kind)}`;
}

/** Brand accent color associated with the kind, for badges or borders. */
export function accentFor(kind: string | undefined): string {
    return ICONS[normalize(kind)].accent;
}

/**
 * Builds the on-disk SVG path to use as `TreeItem.iconPath`. Returns
 * `vscode.ThemeIcon('database')` as a graceful fallback when the kind is
 * unknown — keeps the existing behaviour for sources QuiverSQL does not yet
 * have an icon for, instead of rendering nothing.
 */
export function treeIconFor(
    extensionUri: vscode.Uri,
    kind: string | undefined,
): vscode.Uri | vscode.ThemeIcon {
    const entry = ICONS[normalize(kind)];
    if (!entry.file) {
        return new vscode.ThemeIcon('database');
    }
    return vscode.Uri.joinPath(extensionUri, 'media', 'icons', entry.file);
}

/**
 * Returns the inline `<defs>` block embedding every provider icon as a
 * `<symbol>`. Splice this once into the plan-graph SVG root; afterwards each
 * `TableScan` node can render its provider glyph via
 * `<use href="#icon-postgres" x="…" y="…" width="16" height="16"/>`.
 */
export function svgSymbolsLibrary(): string {
    const symbols = (Object.entries(ICONS) as [IconKind, ProviderIcon][])
        .map(
            ([kind, icon]) => `
            <symbol id="icon-${kind}" viewBox="0 0 16 16">${icon.symbolBody}</symbol>`,
        )
        .join('');
    return `<defs>${symbols}</defs>`;
}

/** The list of every kind we have an icon for. Used by tests. */
export function allIconKinds(): IconKind[] {
    return Object.keys(ICONS) as IconKind[];
}

function normalize(kind: string | undefined): IconKind {
    if (!kind) {
        return 'unknown';
    }
    return (kind in ICONS ? kind : 'unknown') as IconKind;
}
