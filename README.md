# ant-capsulizer

Ingestion and capsule generation service for AgentNet that converts source content into structured records for downstream resolution.

Status: Alpha / Reference Implementation
Docs: https://www.agent-net.ai
Org: https://github.com/agentnet-ai

## What it does

`ant-capsulizer` is the capsulizer component of the AgentNet reference architecture.

It is responsible for ingestion and capsule-oriented data transformation workflows that prepare source content for resolver consumption.

In a local development environment, it typically runs alongside `ant-registrar` for identity issuance and `ant-resolver` for downstream retrieval, with `ant-orchestrator` consuming resolved outputs.

## Where it fits

Within the canonical AgentNet flow:

1. Capsulizer
2. Registrar
3. Resolver
4. Orchestrator

This repository provides the capsulizer role.

## Quickstart

```bash
cp .env.example .env
npm install
```

Run commands:

- `npm run dev`
- `npm start`
- `npm run worker`
- `npm run schedule`
- `npm run dev:seed`
- `npm run capsulize:repo`

## Configuration

Runtime configuration is read from `.env`, with starter values in `.env.example`.

- `ANT_WORKER_OWNER_ID` is required and must be a positive integer for seed/worker publish flows.
- Discover it from registrar: `curl -s http://localhost:4002/v1/owners/ant-worker`

## Status

Status: Alpha / Reference Implementation  
These components are intended to demonstrate ANS-aligned architecture patterns.

## Related Repositories

Other core AgentNet reference components:

- https://github.com/agentnet-ai/AgentNet
- https://github.com/agentnet-ai/ant-capsulizer
- https://github.com/agentnet-ai/ant-registrar
- https://github.com/agentnet-ai/ant-resolver
- https://github.com/agentnet-ai/ant-orchestrator

## License

Apache License 2.0. See `LICENSE` for details.
