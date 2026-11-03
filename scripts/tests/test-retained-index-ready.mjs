import assert from 'node:assert/strict';
import test from 'node:test';
import { waitForRetainedIndex } from '../retained-index-ready.mjs';

function clock() {
  let time = 0;
  const pauses = [];
  return {
    now: () => time,
    advance: (milliseconds) => { time += milliseconds; },
    pause: async (milliseconds) => { pauses.push(milliseconds); time += milliseconds; },
    pauses,
  };
}

test('search is admitted once only after the finite fixture durable ACK', async () => {
  const time = clock();
  const events = [];
  let statuses = 0;
  const readStatus = async () => {
    const index_ready = ++statuses === 3;
    events.push(index_ready ? 'ready' : 'replaying');
    return { index_ready };
  };
  await waitForRetainedIndex(readStatus, 2000, time);
  events.push('query');
  assert.deepEqual(events, ['replaying', 'replaying', 'ready', 'query']);
  assert.deepEqual(time.pauses, [500, 500]);
});

test('the original deadline is never rebased by incomplete status', async () => {
  const time = clock();
  let calls = 0;
  await assert.rejects(waitForRetainedIndex(async () => { calls++; return { index_ready: false }; },
    1000, time), /did not converge/);
  assert.equal(calls, 2);
  assert.equal(time.now(), 1000);
});

test('an already expired deadline performs no RPC', async () => {
  const time = clock();
  await assert.rejects(waitForRetainedIndex(async () => assert.fail('unexpected RPC'), 0, time),
    /did not converge/);
});

test('a status arriving after the original deadline cannot admit a query', async () => {
  const time = clock();
  await assert.rejects(waitForRetainedIndex(async () => {
    time.advance(1000); return { index_ready: true };
  }, 1000, time), /did not converge/);
});

test('an RPC error propagates immediately without retry', async () => {
  const time = clock();
  const original = new Error('storage_unavailable');
  let calls = 0;
  await assert.rejects(waitForRetainedIndex(async () => { calls++; throw original; }, 1000, time),
    (error) => error === original);
  assert.equal(calls, 1);
  assert.deepEqual(time.pauses, []);
});

test('malformed status is not treated as readiness or retried', async () => {
  for (const status of [null, {}, { index_ready: 'true' }, { index_ready: 1 }]) {
    const time = clock();
    await assert.rejects(waitForRetainedIndex(async () => status, 1000, time), /missing durable/);
    assert.deepEqual(time.pauses, []);
  }
});

test('nonfinite deadlines are refused before an RPC', async () => {
  for (const deadline of [Infinity, NaN]) {
    await assert.rejects(waitForRetainedIndex(async () => assert.fail('unexpected RPC'), deadline),
      /absolute deadline/);
  }
});
