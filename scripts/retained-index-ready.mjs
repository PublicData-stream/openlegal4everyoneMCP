// Offline image fixture only. Readiness does not mean outbox replay has converged.
import assert from 'node:assert/strict';

export async function waitForRetainedIndex(readStatus, deadline, {
  now = Date.now,
  pause = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)),
} = {}) {
  assert.ok(Number.isFinite(deadline), 'index readiness requires an absolute deadline');
  while (now() < deadline) {
    const status = await readStatus();
    assert.equal(typeof status?.index_ready, 'boolean', 'missing durable index readiness');
    if (status.index_ready) {
      assert.ok(now() < deadline, 'retained corpus index did not converge before its deadline');
      return;
    }
    await pause(500);
  }
  assert.fail('retained corpus index did not converge before its deadline');
}
