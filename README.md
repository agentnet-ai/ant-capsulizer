# ant-capsulizer

Ingestion and capsule generation service for AgentNet that converts source content into structured records for downstream resolution.

## What it does

`ant-capsulizer` gathers source material and transforms it into capsule-compatible data. It runs worker processes and related tooling for seed, scheduling, and report workflows.

The service writes data used by resolver infrastructure and can coordinate with registrar for node identity assignment when new origins are first encountered.

This repository is maintained as a reference implementation aligned to ANS Core v2.0 integration patterns.

## Where it fits

Canonical flow:

1. Capsulizer (this repository)
2. Registrar
3. Resolver
4. Orchestrator

## Quickstart

```bash
cp .env.example .env
npm install
npm start
```

Also available:

- `npm run dev`
- `npm run worker`
- `npm run seed`
- `npm run report`
- `npm run capsulize:repo`

## Configuration

Runtime configuration is read from `.env`, with starter values in `.env.example`.

## Repo structure

- `src/` capsulizer service and tooling
- `runs/` generated run artifacts
- `seeds/` seed support files

## Status

Alpha, reference implementation.

## Related Repositories

- https://github.com/agentnet-ai/AgentNet
- https://github.com/agentnet-ai/ant-capsulizer
- https://github.com/agentnet-ai/ant-registrar
- https://github.com/agentnet-ai/ant-resolver
- https://github.com/agentnet-ai/ant-orchestrator

## License

Apache License 2.0. See `LICENSE`.
