# START HERE

## Purpose

This guide gets `ant-capsulizer` running locally and shows how it feeds data into the broader AgentNet reference stack.

## Prerequisites

- Node.js and npm
- MySQL
- Redis

## Setup

```bash
cp .env.example .env
npm install
```

## Run

```bash
npm start
```

Useful alternatives:

- `npm run dev`
- `npm run worker`
- `npm run seed`
- `npm run report`
- `npm run capsulize:repo`

## Typical local integration flow

1. Start `ant-registrar` for node identity issuance.
2. Start `ant-resolver` with a reachable database.
3. Run `ant-capsulizer` to ingest content and write capsule-ready data.
4. Start `ant-orchestrator` to query resolver-backed outputs.

## Links

- AgentNet: https://github.com/agentnet-ai/AgentNet
- ant-registrar: https://github.com/agentnet-ai/ant-registrar
- ant-resolver: https://github.com/agentnet-ai/ant-resolver
- ant-orchestrator: https://github.com/agentnet-ai/ant-orchestrator
