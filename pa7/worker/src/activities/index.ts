/**
 * The six activities — YOURS TO WRITE.
 *
 * An activity is ordinary Node code: it may do I/O, read the environment and
 * fail. Each one here makes one HTTP call to one mock service, exactly the
 * calls your PA6 orchestrator made. README.md §4 has the table.
 *
 * What an activity must add is a decision the PA6 orchestrator never had to
 * make: when a call fails, may Temporal try it again?
 *
 *   - A 4xx is the service saying no (payment_declined, inventory_unavailable).
 *     It will say no again. Throw ApplicationFailure.nonRetryable(message, code)
 *     with the mock's own `code` from the response body as the type.
 *   - No answer at all, or a 5xx, is temporary. Throw
 *     ApplicationFailure.retryable(message, code), or any ordinary Error, and
 *     the retry policy may try again.
 *   - A call that takes too long you do not handle here: Temporal fails the
 *     attempt itself when startToCloseTimeout runs out.
 *
 * The signatures are fixed: the workflow calls these by name, with these
 * arguments. Keep them.
 */

import { ApplicationFailure } from "@temporalio/activity";
import type { CanonicalItem } from "../types";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

const PAYMENT_URL = requiredEnv("PAYMENT_URL");
const INVENTORY_URL = requiredEnv("INVENTORY_URL");
const SHIPPING_URL = requiredEnv("SHIPPING_URL");
const NOTIFICATION_URL = requiredEnv("NOTIFICATION_URL");

async function post(
  url: string,
  body: Record<string, unknown>
): Promise<void> {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (response.ok) {
    return;
  }

  let errorBody: { code?: string; message?: string } = {};

  try {
    errorBody = await response.json() as typeof errorBody;
  } catch {
    // The service may return an empty or non-JSON error response.
  }

  const code = errorBody.code ?? `http_${response.status}`;
  const message = errorBody.message ?? `HTTP ${response.status}: ${code}`;

  if (response.status >= 400 && response.status < 500) {
    throw ApplicationFailure.nonRetryable(message, code);
  }

  throw ApplicationFailure.retryable(message, code);
}

export async function authorizePayment(
  orderId: string,
  amount: string
): Promise<void> {
  await post(`${PAYMENT_URL}/payment/authorize`, {
    orderId,
    amount,
  });
}

export async function reserveInventory(
  orderId: string,
  items: CanonicalItem[]
): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/reserve`, {
    orderId,
    items,
  });
}

export async function createShipment(
  orderId: string
): Promise<void> {
  await post(`${SHIPPING_URL}/shipping/create`, {
    orderId,
  });
}

export async function sendNotification(
  orderId: string,
  recipient: string
): Promise<void> {
  await post(`${NOTIFICATION_URL}/notification/send`, {
    orderId,
    recipient,
  });
}

export async function refundPayment(
  orderId: string
): Promise<void> {
  await post(`${PAYMENT_URL}/payment/refund`, {
    orderId,
  });
}

export async function releaseInventory(
  orderId: string
): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/release`, {
    orderId,
  });
}
