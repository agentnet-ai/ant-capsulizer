// src/index.js (ant-capsulizer)
require("dotenv").config();

// This file is intentionally thin.
// All worker logic lives in ./worker.js so dev:seed and start/dev behave consistently.
require("./worker");

console.log("👷 ant-capsulizer worker booted (see worker.js for details)");

