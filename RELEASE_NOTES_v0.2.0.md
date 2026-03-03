# AgentNet v0.2.0 — Resolver Stability Milestone

Release Date: 2026-03-02  
Version: v0.2.0

## Overview

v0.2.0 represents a significant architectural maturation of the AgentNet reference stack.

This release hardens ownership guarantees, improves resolver scoring fidelity,
increases ranking robustness, and reduces reliance on external WebRAG when
authoritative capsules are present.

The system now behaves as a grounded knowledge infrastructure layer rather than
a retrieval experiment.

## Key Improvements

### Owner Integrity Enforcement
- Strict owner_id persistence on nodes and capsules
- Legacy null-owner repairs
- Resolver queries fully owner-scoped
- ownerSlug returned in query results

### Authority-Aware Confidence Calibration
- Confidence now derived from:
  - Top hit relevance
  - Token coverage
  - Ranking margin
  - Authority weighting
- Authoritative capsule matches now clear high-confidence thresholds (~0.80+)
- Ranking order unchanged

### Single-Token Query Support
- minMatch updated
- QUERY_TOO_SHORT now triggers only when filtered token count is zero
- Definitional queries now resolve correctly

### Expanded SQL Candidate Pool
- Increased candidate multiplier for more stable ranking
- No outward API change

### Increased Capsule Limit (10)
- Orchestrator now requests up to 10 capsules
- Improved grounding context
- Reduced WebRAG dependency

### macOS-Compatible Reset Script
- Removed mapfile/readarray
- Compatible with default macOS Bash
- Safe under set -u

## Architectural Impact

With v0.2.0:
- Confidence reflects authority
- Resolver results are production-calibrated
- Ownership enforcement is deterministic
- WebRAG is optional, not required
- Orchestrator operates on reliable resolver signals

This marks the transition from experimental implementation
to stable knowledge infrastructure foundation.

## Breaking Changes
None.

API response shape unchanged.
No schema changes.
No ANS updates required.
