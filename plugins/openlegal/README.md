# OpenLegal plugin for Claude and ChatGPT

This folder packages the public openlegal4everyone.stream MCP server with
end-user skills, for Claude (Claude Code, Cowork and claude.ai) and for
ChatGPT/Codex. Both platforms read the same `skills/` folder.

The plugin provides legal information, not legal advice. Upstream legal data
keeps its own provenance and reuse conditions; see the
[Korean provider profile](../../docs/providers/kr-law-go-kr.md).

## Status

The manifests and skills are validated offline only. Live Claude and ChatGPT
connections, widget rendering, directory listings and live LAW OPEN DATA
acceptance have not been established. See [remaining work](#remaining-work).

## Contents

| Path | Purpose |
| --- | --- |
| `.claude-plugin/plugin.json` | Claude plugin manifest |
| `.mcp.json` | Claude remote MCP declaration (`type: http`) |
| `plugin.json` | [Agent Plugins 1.0](https://agent-plugins.org/) portable manifest with `extensions.com.openai` for ChatGPT/Codex |
| `mcp.json` | Portable remote MCP declaration (`type: streamable-http`) |
| `skills/` | Shared [Agent Skills](https://agentskills.io/specification) |
| `../../.claude-plugin/marketplace.json` | Claude Code marketplace entry for this repository |

Both MCP declarations point to
`https://openlegal4everyone.mcp.publicdata.stream/mcp`. The server is
anonymous; no account, token or OAuth flow is required.

## Skills

| Skill | Use |
| --- | --- |
| `korean-law-research` | Resolve law names and abbreviations, search and read statutes, ordinances and precedents, check citations and cite them with provenance; request collection when the corpus lacks an object |
| `legal-revision-history` | List revisions of one object and compare two versions |
| `legal-text-comparison` | Compare and patch user-supplied texts |

The skills call the server's `database.*`, `law.*`, `citation.verify` and
`text.*` tools. The synthetic
`demo_*` tools are not part of the public plugin.

## Install

Claude Code:

```text
/plugin marketplace add PublicData-stream/openlegal4everyoneMCP
/plugin install openlegal@openlegal4everyone
```

claude.ai or Cowork without the plugin: add
`https://openlegal4everyone.mcp.publicdata.stream/mcp` as a custom connector.

ChatGPT: in developer mode, create a connection to the same URL, as described
in the [ChatGPT connection guide](https://developers.openai.com/plugins/deploy/connect-chatgpt).

## Validate

```sh
claude plugin validate ./plugins/openlegal --strict
claude plugin validate . --strict
```

The portable manifests follow the Agent Plugins 1.0
[plugin](https://agent-plugins.org/schemas/1.0.0/plugin.schema.json) and
[MCP](https://agent-plugins.org/schemas/1.0.0/mcp.schema.json) schemas. Each
`SKILL.md` name must match its folder and use lowercase letters, digits and
single hyphens.

## Privacy policy

The server serves anonymous tool calls. Its private `/metrics` endpoint records
aggregate call, failure and rate-limit counts, not request payloads; see the
[server contract](../../docs/server.md#configure-and-run). Text supplied to
`text.*` tools is retained temporarily unless deleted earlier: uploaded
attachments expire ten minutes after the initial upload, and comparisons and
generated attachments expire ten minutes after publication; see
[text comparison](../../docs/text-diff.md).
The operator's public privacy policy URL, covering collection, use, storage,
third-party sharing, retention and contact, is still to be published.

## Remaining work

- Publish privacy policy, terms and support URLs, then add them to
  `plugin.json` `interface` (`privacyPolicyURL`, `termsOfServiceURL`).
- Add an icon, logo and 3–5 widget screenshots under `assets/`.
- Test every tool through Claude custom connectors and ChatGPT developer mode,
  including dotted tool names such as `database.query`, and record the results.
- Submit the server as a Claude MCP connector and this folder as a Claude
  plugin, and submit the ChatGPT plugin. These are manual, outward-facing steps.

## License

[AGPL-3.0-only](LICENSE), the same as the rest of the repository.
