# START HERE

## Purpose

Set up and run `ant-capsulizer` locally as the ingestion stage in the AgentNet reference flow.

## Prereqs

- Node.js and npm
- MySQL
- Redis

## Setup

```bash
cp .env.example .env
npm install
```

## Run commands

- `npm run dev`
- `npm start`
- `npm run worker`
- `npm run schedule`
- `npm run dev:seed`
- `npm run capsulize:repo`

## Typical local integration flow

1. Start `ant-registrar` for node identity issuance.
2. Start `ant-resolver` with a reachable database.
3. Start `ant-capsulizer` with one of the run commands above.
4. Start `ant-orchestrator` to query resolver-backed outputs.

## Related Repositories

- https://github.com/agentnet-ai/AgentNet
- https://github.com/agentnet-ai/ant-capsulizer
- https://github.com/agentnet-ai/ant-registrar
- https://github.com/agentnet-ai/ant-resolver
- https://github.com/agentnet-ai/ant-orchestrator
