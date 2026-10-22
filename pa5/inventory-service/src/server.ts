/**
 * PA5 inventory-service — EMPTY. Student implements everything below.
 *
 * Same pattern as payment-service (see ../../payment-service/src/server.ts
 * for the worked reference implementation of the connect/consume/retry/DLQ
 * scaffolding) with these differences:
 *   - Consumes from `inventory.queue`, not `payments.queue`.
 *   - Uses INVENTORY_FAIL_RATE, not PAYMENT_FAIL_RATE.
 *   - Publishes result events to `results.inventory`, not `results.payment`.
 *   - Simulates a stock check instead of a payment validation — mechanically
 *     identical, just a different `details.message`.
 *
 * Build:
 *   1. connectWithRetry(process.env.RABBITMQ_URL) from ../../shared/rabbit,
 *      then channel.prefetch(1).
 *   2. channel.consume("inventory.queue", async (msg) => { ... }).
 *   3. Read correlationId from msg.properties.headers?.correlationId and
 *      retryCount from getRetryCount(msg) (also in ../../shared/rabbit).
 *   4. Roll a random number against INVENTORY_FAIL_RATE; on success, ack and
 *      publish a result event to results.inventory with the same
 *      correlationId header; on failure, throw so the retry/DLQ logic below
 *      (copy payment-service's) can catch it.
 *   5. On MAX_RETRIES exceeded: publish to process.env.DLQ_EXCHANGE (fanout,
 *      routing key ""), then channel.ack(msg) — ack, never leave it both in
 *      the consumer queue and the DLQ. Otherwise: channel.nack(msg, false,
 *      false) (never requeue=true — that skips the retry delay entirely).
 *
 * The line below only keeps the container process alive so `docker compose
 * up --wait` succeeds against this untouched scaffold — delete it once your
 * own channel.consume(...) registration is what keeps the process alive.
 */

// import { connectWithRetry, getRetryCount } from "../../shared/rabbit";


import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "inventory.queue";
const RESULTS_EXCHANGE = "results.inventory";
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;
const FAIL_RATE = Number(process.env.INVENTORY_FAIL_RATE) || 10;

async function main(): Promise<void> {
  const rabbitUrl = process.env.RABBITMQ_URL;
  if (!rabbitUrl) {
    throw new Error("RABBITMQ_URL is required");
  }

  const { channel } = await connectWithRetry(rabbitUrl);
  await channel.prefetch(1);

  // Declare the required invalid-message and dead-letter topology.
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

  await channel.assertExchange(RESULTS_EXCHANGE, "fanout", {
    durable: true,
  });

  console.log(
    `[inventory-service] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`,
  );

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const headers = msg.properties.headers ?? {};
    const correlationId = headers.correlationId;
    const retryCount = getRetryCount(msg);

    let order: CanonicalOrder;

    // Malformed JSON is permanent: preserve the original message and headers.
    try {
      order = JSON.parse(msg.content.toString()) as CanonicalOrder;
    } catch {
      channel.publish(
        "orders.invalid.exchange",
        "",
        msg.content,
        {
          headers,
          persistent: true,
          contentType: msg.properties.contentType,
        },
      );
      channel.ack(msg);
      console.error("[inventory-service] Invalid JSON sent to invalid exchange");
      return;
    }

    try {
      if (
        !order ||
        typeof order !== "object" ||
        typeof order.orderId !== "string" ||
        !Array.isArray(order.items)
      ) {
        channel.publish("orders.invalid.exchange", "", msg.content, {
          headers,
          persistent: true,
          contentType: msg.properties.contentType,
        });
        channel.ack(msg);
        console.error("[inventory-service] Invalid order sent to invalid exchange");
        return;
      }

      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("Simulated transient inventory failure");
      }

      const result = {
        correlationId,
        orderId: order.orderId,
        status: "success",
        details: {
          message: "Stock check passed",
        },
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
      console.log(`[inventory-service] Stock check passed: ${String(correlationId)}`);
    } catch (error) {
      console.error(
        `[inventory-service] Processing failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );

      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(
          process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange",
          "",
          msg.content,
          {
            headers: {
              ...headers,
              originQueue: QUEUE,
            },
            persistent: true,
            contentType: msg.properties.contentType,
          },
        );

        channel.ack(msg);
        console.error(
          `[inventory-service] Sent to DLQ after ${retryCount + 1} attempts`,
        );
      } else {
        channel.nack(msg, false, false);
      }
    }
  });
}

main().catch((error: unknown) => {
  console.error("[inventory-service] Fatal error:", error);
  process.exit(1);
});
