
import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "payments.queue";
const RESULTS_EXCHANGE = "results.payment";
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;
const FAIL_RATE = Number(process.env.PAYMENT_FAIL_RATE) || 20;

async function main(): Promise<void> {
  const rabbitUrl = process.env.RABBITMQ_URL;
  if (!rabbitUrl) throw new Error("RABBITMQ_URL is required");

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

  await channel.assertExchange(RESULTS_EXCHANGE, "fanout", {
    durable: true,
  });

  console.log(`[Payment] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const headers = msg.properties.headers ?? {};
    const correlationId = headers.correlationId;
    const retryCount = getRetryCount(msg);

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
      console.error("[Payment] Invalid JSON sent to invalid exchange");
      return;
    }

    if (
      !order ||
      typeof order !== "object" ||
      typeof order.orderId !== "string" ||
      !Array.isArray(order.items) ||
      typeof correlationId !== "string" ||
      correlationId.length === 0
    ) {
      channel.publish("orders.invalid.exchange", "", msg.content, {
        headers,
        persistent: true,
        contentType: msg.properties.contentType,
      });
      channel.ack(msg);
      console.error("[Payment] Invalid order sent to invalid exchange");
      return;
    }

    console.log(
      `[Payment] Processing ${correlationId} (attempt ${retryCount + 1})`,
    );

    try {
      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("Simulated transient payment failure");
      }

      const result = {
        correlationId,
        source: "payment",
        status: "success",
        timestamp: new Date().toISOString(),
        details: { message: "Payment processed successfully" },
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
      console.log(`[Payment] Success: ${correlationId}`);
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : String(err);

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
        console.log(
          `[Payment] -> DLQ after ${retryCount + 1} attempts: ${errorMessage}`,
        );
      } else {
        channel.nack(msg, false, false);
        console.log(
          `[Payment] -> Retry (attempt ${retryCount + 1}): ${errorMessage}`,
        );
      }
    }
  });
}

main().catch((error: unknown) => {
  console.error("[Payment] Fatal error:", error);
  process.exit(1);
});
