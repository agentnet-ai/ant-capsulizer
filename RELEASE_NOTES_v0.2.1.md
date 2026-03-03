# AgentNet v0.2.1 — Search Indexing & Grounding Reliability Patch

Release Date: 2026-03-03  
Version: v0.2.1

## Summary
This patch release resolves a search indexing issue that could cause valid repository capsules, especially DOCX-backed content, to appear ungrounded in downstream orchestration flows.

## What Changed
- **Search indexing reliability:** `capsules.search_text` is now populated consistently on insert/upsert for crawl and repo capsule write paths.
- **Legacy row repair:** empty `search_text` rows are backfilled when touched by current write paths.
- **Grounding consistency:** resolver query discoverability is restored for DOCX and Markdown repository content.

## Compatibility
- No schema changes
- No API contract changes
- No ANS spec changes required

## Operational Note
Re-run full repository capsulization after upgrading:

```bash
node src/tools/capsulizeRepo.js --repoPath ../agentnet
```
