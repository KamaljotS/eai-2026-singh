import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import express from "express";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { connectWithRetry } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const orders = new Map<string, CanonicalOrder>();

const ajv = new Ajv({ allErrors: true });
addFormats(ajv);

const schemaPath =
  process.env.CANONICAL_ORDER_SCHEMA ?? "/canonical/order.schema.json";

const canonicalOrderSchema = JSON.parse(
  readFileSync(schemaPath, "utf8"),
);

const validateCanonicalOrder = ajv.compile<CanonicalOrder>(
  canonicalOrderSchema,
);

function isCanonicalOrder(value: unknown): value is CanonicalOrder {
  return validateCanonicalOrder(value);
}

function withoutRetryHistory(
  headers: Record<string, unknown> | undefined,
): Record<string, unknown> {
  const result = { ...(headers ?? {}) };
  delete result["x-death"];
  delete result["x-first-death-exchange"];
  delete result["x-first-death-queue"];
  delete result["x-first-death-reason"];
  delete result["x-last-death-exchange"];
  delete result["x-last-death-queue"];
  delete result["x-last-death-reason"];
  return result;
}

async function main(): Promise<void> {
  const rabbitUrl =
    process.env.RABBITMQ_URL ?? "amqp://eai:eai-pa5@localhost:5674";
  const { channel } = await connectWithRetry(rabbitUrl);

  await channel.assertExchange("orders.exchange", "fanout", {
    durable: true,
  });

  await channel.assertExchange("orders.return.exchange", "direct", {
    durable: true,
  });

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

  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.status(200).json({ status: "ok" });
  });

  app.post("/orders", (req, res) => {
    if (!isCanonicalOrder(req.body)) {
      res.status(400).json({ error: "Invalid canonical order" });
      return;
    }

    const order = req.body;
    const correlationId = randomUUID();

    try {
      channel.publish(
        "orders.exchange",
        "",
        Buffer.from(JSON.stringify(order)),
        {
          headers: { correlationId },
          contentType: "application/json",
          persistent: true,
        },
      );

      orders.set(correlationId, order);
      res.status(201).json({ correlationId, status: "accepted" });
    } catch (error) {
      console.error("[order-service] Failed to publish order:", error);
      res.status(500).json({ error: "Failed to publish order" });
    }
  });

  app.get("/orders/:correlationId", (req, res) => {
    const correlationId = req.params.correlationId;
    const order = orders.get(correlationId);

    if (!order) {
      res.status(404).json({ error: "Order not found" });
      return;
    }

    res.status(200).json({ correlationId, order });
  });

  app.post("/dlq/replay", async (_req, res) => {
    let replayed = 0;

    try {
      while (true) {
        const msg = await channel.get("orders.dlq", { noAck: false });
        if (!msg) break;

        const headers = withoutRetryHistory(
          msg.properties.headers as Record<string, unknown> | undefined,
        );
        const originQueue = headers.originQueue;

        if (
          typeof originQueue !== "string" ||
          ![
            "payments.queue",
            "inventory.queue",
            "notifications.queue",
          ].includes(originQueue)
        ) {
          channel.nack(msg, false, true);
          console.error(
            "[order-service] DLQ message has missing or invalid originQueue",
          );
          break;
        }

        channel.publish(
          "orders.return.exchange",
          originQueue,
          msg.content,
          {
            headers,
            contentType: msg.properties.contentType,
            contentEncoding: msg.properties.contentEncoding,
            correlationId: msg.properties.correlationId,
            messageId: msg.properties.messageId,
            timestamp: msg.properties.timestamp,
            type: msg.properties.type,
            appId: msg.properties.appId,
            persistent: true,
          },
        );

        channel.ack(msg);
        replayed += 1;
      }

      res.status(200).json({ replayed });
    } catch (error) {
      console.error("[order-service] DLQ replay failed:", error);
      res.status(500).json({ error: "Failed to replay DLQ messages", replayed });
    }
  });

  app.listen(PORT, () => {
    console.log(`[order-service] Listening on port ${PORT}`);
  });
}

main().catch((error: unknown) => {
  console.error("[order-service] Failed to start:", error);
  process.exit(1);
});
