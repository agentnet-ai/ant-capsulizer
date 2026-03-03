// src/index.js (ant-capsulizer)
require("./bootstrap/env");
const { startWebApi } = require("./webApi");

// This file is intentionally thin.
// All worker logic lives in ./worker.js so dev:seed and start/dev behave consistently.
require("./worker");
startWebApi();

console.log("👷 ant-capsulizer worker booted (see worker.js for details)");

