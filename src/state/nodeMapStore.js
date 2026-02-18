// src/state/nodeMapStore.js
const fs = require("fs-extra");
const path = require("path");
const { issueNode } = require("../clients/registrarClient");

async function loadNodeMap(mapPath) {
  try {
    return await fs.readJson(mapPath);
  } catch {
    return {};
  }
}

async function saveNodeMap(mapPath, mapObj) {
  await fs.ensureDir(path.dirname(mapPath));
  await fs.writeJson(mapPath, mapObj, { spaces: 2 });
}

async function getOrIssueNodeId({ mapPath, key }) {
  const map = await loadNodeMap(mapPath);

  if (map[key]) {
    return map[key];
  }

  const result = await issueNode();
  const nodeId = result.nodeId;
  if (!nodeId) {
    throw new Error(
      `Registrar issueNode() did not return a nodeId. Response: ${JSON.stringify(result)}`
    );
  }

  map[key] = nodeId;
  await saveNodeMap(mapPath, map);
  return nodeId;
}

module.exports = { loadNodeMap, saveNodeMap, getOrIssueNodeId };
