/**
 * The checkout workflow — YOURS TO WRITE.
 *
 * This is your PA6 saga, ported: the same four steps, the same reverse-order
 * compensation, the same outcomes (README.md §4). What you no longer write is
 * everything PA6 made you build around it — the timeout plumbing, the retry
 * loop, the in-progress record and the "where was I?" state. Temporal keeps
 * every step's result in the workflow's history.
 *
 * Workflow code runs in Temporal's deterministic sandbox, and is replayed
 * from that history whenever a worker has to rebuild the workflow's state:
 *
 *   - No I/O here. No fetch, no axios, no fs, no process.env. Every call to
 *     the outside world is an activity, through `act` below.
 *   - Import only types from ../activities, never the functions themselves.
 *   - `new Date()` is fine: inside a workflow it returns the workflow's own
 *     time, which is the same on every replay. Use it for the trace.
 *
 * An activity that fails for good reaches you as an ActivityFailure. Its
 * `cause` says why: a TimeoutFailure when the attempts timed out, or an
 * ApplicationFailure whose `type` is the code your activity threw.
 */

import {
  proxyActivities,
  ActivityFailure,
  ApplicationFailure,
  TimeoutFailure,
} from "@temporalio/workflow";
import type * as activities from "../activities";
import type {
  CanonicalOrder,
  CheckoutResult,
  TraceItem,
} from "../types";

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: {
    maximumAttempts: 3,
    initialInterval: "200 ms",
    backoffCoefficient: 2,
  },
});

function calculateAmount(order: CanonicalOrder): string {
  const totalCents = order.items.reduce((sum, item) => {
    const [whole, fraction] = item.unitPrice.split(".");
    const unitCents = Number(whole) * 100 + Number(fraction);
    return sum + unitCents * item.quantity;
  }, 0);

  return `${Math.floor(totalCents / 100)}.${String(
    totalCents % 100
  ).padStart(2, "0")}`;
}

function failureCode(error: unknown): string {
  let current: unknown = error;

  while (current instanceof ActivityFailure) {
    current = current.cause;
  }

  if (current instanceof TimeoutFailure) {
    return "timeout";
  }

  if (current instanceof ApplicationFailure) {
    return current.type || "activity_failed";
  }

  return "activity_failed";
}

export async function checkout(
  order: CanonicalOrder
): Promise<CheckoutResult> {
  const trace: TraceItem[] = [];
  const completed: string[] = [];

  async function runStep(
    step: string,
    operation: () => Promise<void>
  ): Promise<void> {
    const started = new Date();

    try {
      await operation();

      const finished = new Date();

      trace.push({
        step,
        status: "success",
        startedAt: started.toISOString(),
        finishedAt: finished.toISOString(),
        durationMs: finished.getTime() - started.getTime(),
      });
    } catch (error) {
      const finished = new Date();
      const code = failureCode(error);

      trace.push({
        step,
        status: code === "timeout" ? "timeout" : "failed",
        startedAt: started.toISOString(),
        finishedAt: finished.toISOString(),
        durationMs: finished.getTime() - started.getTime(),
      });

      throw error;
    }
  }

  const orderId = order.orderId;

  try {
    await runStep("payment", () =>
      act.authorizePayment(orderId, calculateAmount(order))
    );
    completed.push("payment");

    await runStep("inventory", () =>
      act.reserveInventory(orderId, order.items)
    );
    completed.push("inventory");

    await runStep("shipping", () =>
      act.createShipment(orderId)
    );
    completed.push("shipping");

    await runStep("notification", () =>
      act.sendNotification(orderId, order.customer.email)
    );
    completed.push("notification");

    return {
      orderId,
      status: "completed",
      trace,
    };
  } catch (error) {
    const code = failureCode(error);
    let compensationFailed = false;

    // Undo completed compensatable steps in reverse order.
    if (completed.includes("inventory")) {
      try {
        await runStep("inventory_release", () =>
          act.releaseInventory(orderId)
        );
      } catch {
        compensationFailed = true;
      }
    }

    if (completed.includes("payment")) {
      try {
        await runStep("payment_refund", () =>
          act.refundPayment(orderId)
        );
      } catch {
        compensationFailed = true;
      }
    }

    if (compensationFailed) {
      return {
        orderId,
        status: "failed",
        code: "compensation_failed",
        trace,
      };
    }

    return {
      orderId,
      status: completed.length > 0 ? "compensated" : "failed",
      code,
      trace,
    };
  }
}
