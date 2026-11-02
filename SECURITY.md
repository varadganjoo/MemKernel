# Security

Use the latest patched 0.3.x release. Older releases do not receive separate fixes.

Report suspected vulnerabilities through this repository's **Security → Report a
vulnerability** option when available. If private reporting is unavailable, use
the maintainer's contact details on their GitHub profile to request a private
channel. Do not put secrets, private memory, or exploit details in a public issue.
Include the affected version, prerequisites, minimal reproduction with synthetic
data, impact, and any suggested fix. Never include real private signing keys.

MemKernel's stdio server is for cooperating agents in one OS trust domain. Agent
IDs are self-asserted. A local process with database access can bypass application
controls. File leases are advisory; fencing protects memory appends, not arbitrary
filesystem edits. Read-only sessions reject mutations at both MCP and SQLite layers.

Treat stored text and model summaries as untrusted data. Signatures verify a pinned
peer and permitted author, not truth or safety. Signed notices are not encrypted.
The operator owns transport, key protection, trust policy, endpoint permissions,
backup and retention. Keep SQLite on local disk and outside exchange folders.

Rollback and compaction retain raw history. They do not erase secrets or personal
data. Use OS access controls, encrypted storage where needed, and tested backups.
Do not expose this local stdio server as a public multi-user service without a
separate authentication, authorization and data-isolation design.
