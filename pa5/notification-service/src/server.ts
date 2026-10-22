/**
 * PA5 notification-service — EMPTY. Student implements everything below.
 *
 * Build:
 *   1. connectWithRetry(process.env.RABBITMQ_URL) from ../../shared/rabbit,
 *      then channel.prefetch(1).
 *   2. On startup, load /data/processed-ids.json (create it as `[]` if it
 *      does not exist yet — this file, and /data/notification.log, are
 *      bind-mounted so they survive `docker compose restart
 *      notification-service`; see ../../docs/adr-004.md for why this is a
 *      file and not an in-memory Set).
 *   3. channel.consume("notifications.queue", async (msg) => { ... }):
 *        - correlationId = msg.properties.headers?.correlationId
 *        - If correlationId is already in the processed-ids array: ack
 *          silently, log "duplicate skipped", do NOT touch
 *          notification.log. This is the idempotent-receiver requirement —
 *          the retry topology fans a retried message back out to every
 *          consumer queue (see ../../rabbitmq/definitions.json), so a
 *          notification you already logged WILL arrive again.
 *        - Otherwise: append one JSON line to /data/notification.log —
 *            {"correlationId":"...","orderId":"...","customerEmail":"...","timestamp":"...","message":"Order received"}
 *          (order is the canonical order from the message body — see
 *          ../../shared/canonical-order.ts for its shape), add
 *          correlationId to processed-ids and persist the array, ack, then
 *          publish a result event to results.notification with the same
 *          correlationId header.
 *   4. Same nack/retry/DLQ pattern as payment-service on any thrown error.
 *
 * The line below only keeps the container process alive so `docker compose
 * up --wait` succeeds against this untouched scaffold — delete it once your
 * own channel.consume(...) registration is what keeps the process alive.
 */

// import { connectWithRetry, getRetryCount } from "../../shared/rabbit";


import type { ConsumeMessage } from "amqplib";
import { appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const DATA_DIR = "/data";
const PROCESSED_IDS_FILE = `${DATA_DIR}/processed-ids.json`;
const NOTIFICATION_LOG_FILE = `${DATA_DIR}/notification.log`;
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;

async function main(): Promise<void> {
  const rabbitUrl = process.env.RABBITMQ_URL;
  if (!rabbitUrl) throw new Error("RABBITMQ_URL is required");

  await mkdir(DATA_DIR, { recursive: true });

  let processedIds: string[];
  try {
    const saved = await readFile(PROCESSED_IDS_FILE, "utf8");
    const parsed: unknown = JSON.parse(saved);
    if (!Array.isArray(parsed) || !parsed.every((id) => typeof id === "string")) {
      throw new Error("processed-ids.json must contain a JSON array of strings");
    }
    processedIds = parsed;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    processedIds = [];
    await writeFile(PROCESSED_IDS_FILE, "[]\n", "utf8");
  }

  const processedSet = new Set(processedIds);
  const { channel } = await connectWithRetry(rabbitUrl);
  await channel.prefetch(1);

  await channel.assertExchange("orders.dlq.exchange", "fanout", {
    durable: true,
  });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", "orders.dlq.exchange", "");

  await channel.assertExchange("orders.invalid.exchange", "fanout", {
    durable: true,
  });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", "orders.invalid.exchange", "");

  await channel.assertExchange(RESULTS_EXCHANGE, "fanout", { durable: true });

  console.log(`[notification-service] Consuming from ${QUEUE}`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const headers = msg.properties.headers ?? {};
    const correlationId = headers.correlationId;

    let order: CanonicalOrder;
    try {
      order = JSON.parse(msg.content.toString()) as CanonicalOrder;
    } catch {
      channel.publish("orders.invalid.exchange", "", msg.content, {
        headers,
        persistent: true,
        contentType: msg.properties.contentType,
      });
      channel.ack(msg);
      console.error("[notification-service] Invalid JSON sent to invalid exchange");
      return;
    }

    if (
      !order ||
      typeof order !== "object" ||
      typeof order.orderId !== "string" ||
      typeof order.customer !== "object" ||
      order.customer === null
    ) {
      channel.publish("orders.invalid.exchange", "", msg.content, {
        headers,
        persistent: true,
        contentType: msg.properties.contentType,
      });
      channel.ack(msg);
      console.error("[notification-service] Invalid order sent to invalid exchange");
      return;
    }

    if (typeof correlationId !== "string" || correlationId.length === 0) {
      channel.publish("orders.invalid.exchange", "", msg.content, {
        headers,
        persistent: true,
        contentType: msg.properties.contentType,
      });
      channel.ack(msg);
      console.error("[notification-service] Missing correlationId sent to invalid exchange");
      return;
    }

    if (processedSet.has(correlationId)) {
      channel.ack(msg);
      console.log(`[notification-service] duplicate skipped: ${correlationId}`);
      return;
    }

    try {
      const notification = {
        correlationId,
        orderId: order.orderId,
        customerEmail:
          typeof order.customer.email === "string" ? order.customer.email : "",
        timestamp: new Date().toISOString(),
        message: "Order received",
      };

      // Persist the notification before recording the ID, so a failed log
      // append does not incorrectly mark the notification as processed.
      await appendFile(
        NOTIFICATION_LOG_FILE,
        `${JSON.stringify(notification)}\n`,
        "utf8",
      );

      processedSet.add(correlationId);
      await writeFile(
        PROCESSED_IDS_FILE,
        `${JSON.stringify([...processedSet], null, 2)}\n`,
        "utf8",
      );

      const result = {
        correlationId,
        orderId: order.orderId,
        status: "success",
        details: { message: "Order received" },
      };

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(JSON.stringify(result)),
        {
          headers: { correlationId },
          contentType: "application/json",
          persistent: true,
        },
      );

      channel.ack(msg);
      console.log(`[notification-service] Notification logged: ${correlationId}`);
    } catch (error) {
      console.error(
        "[notification-service] Processing failed:",
        error instanceof Error ? error.message : String(error),
      );

      const retryCount = getRetryCount(msg);
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(
          process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange",
          "",
          msg.content,
          {
            headers: { ...headers, originQueue: QUEUE },
            persistent: true,
            contentType: msg.properties.contentType,
          },
        );
        channel.ack(msg);
        console.error("[notification-service] Sent to DLQ");
      } else {
        channel.nack(msg, false, false);
      }
    }
  });
}

main().catch((error: unknown) => {
  console.error("[notification-service] Fatal error:", error);
  process.exit(1);
});
