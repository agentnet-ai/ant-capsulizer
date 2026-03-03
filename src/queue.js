// src/queue.js
require("./bootstrap/env");

const { Queue } = require("bullmq");
const Redis = require("ioredis");

// ------------------------------
// Redis connection (BullMQ v5 safe)
// ------------------------------
const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6379", 10);
const REDIS_HOST = process.env.REDIS_HOST || "127.0.0.1";
const REDIS_PASSWORD = process.env.REDIS_PASSWORD || undefined;

const connection = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  password: REDIS_PASSWORD,
  maxRetriesPerRequest: null, // ✅ required for BullMQ v5
  enableReadyCheck: false,    // ✅ recommended with BullMQ v5
});

// ------------------------------
// Crawl/Capsule queue (existing)
// ------------------------------
const queueName = process.env.CAPSULE_QUEUE_NAME || "capsuleQueue";
const queue = new Queue(queueName, { connection });
const queueConn = queue.opts?.connection || connection;
const queuePrefix = queue.opts?.prefix || "bull";
const queueHost = queueConn?.options?.host || REDIS_HOST;
const queuePort = queueConn?.options?.port || REDIS_PORT;
const queueDb = queueConn?.options?.db ?? 0;
console.log(`[queue:init] name=${queueName}, prefix=${queuePrefix}, redis=${queueHost}:${queuePort}, db=${queueDb}`);

// ------------------------------
// Resolver inquiry queue (new)
// ------------------------------
const inquiryQueueName = process.env.INQUIRY_QUEUE_NAME || "resolver-inquiry";
const inquiryQueue = new Queue(inquiryQueueName, { connection });

// ------------------------------
// Logging
// ------------------------------
console.log("🧩 Queues initialized:", {
  capsuleQueue: queueName,
  inquiryQueue: inquiryQueueName,
  redis: `${REDIS_HOST}:${REDIS_PORT}`,
});

// ------------------------------
// Exports
// ------------------------------
module.exports = {
  connection,

  // existing exports (do not break imports)
  queueName,
  queue,

  // new exports
  inquiryQueueName,
  inquiryQueue,
};

