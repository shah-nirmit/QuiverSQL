# Security Policy

QuiverSQL is an alpha prototype and should not be used for production access to sensitive systems yet.

## Supported Versions

Only the current `master` branch is supported during the alpha phase.

## Reporting A Vulnerability

Please do not open a public issue for vulnerabilities.

Until a dedicated security email is published, report security concerns privately to the repository owner. Include:

- A description of the issue.
- Steps to reproduce.
- Affected operating system and QuiverSQL commit.
- Whether credentials, local files, or query results are exposed.

## Sensitive Data Guidance

- Do not commit credentials, connection strings, database dumps, or private datasets.
- Use only fictional sample data in pull requests.
- Treat daemon logs and screenshots as potentially sensitive if they include file paths or query results.
