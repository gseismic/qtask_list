// 从真实 TypeScript 模块加载纯逻辑，验证运维场景与接口契约，避免另写一份实现。
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import ts from 'typescript';
import vm from 'node:vm';

async function sourceModule(name) {
  const source = readFileSync(new URL(`../src/${name}.ts`, import.meta.url), 'utf8');
  const compiled = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ES2020, target: ts.ScriptTarget.ES2020 },
  }).outputText;
  return import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`);
}
const { taskState, canReplayTask, taskError, taskDeadline, replayIds } = await sourceModule('taskView');
const { RateSampler } = await sourceModule('sampler');
const { AlertEngine, resolveAlert, clearResolved, loadResolved } = await sourceModule('alerts');
const { api } = await sourceModule('api');
const { summarizeBulkResult } = await sourceModule('operations');

test('所有队列操作和任务详情通过查询参数无损传输特殊名称与 ID', async (t) => {
  const calls = [];
  t.mock.method(globalThis, 'fetch', async (path) => {
    calls.push(new URL(path, 'http://localhost'));
    return new Response(JSON.stringify({ tasks: [] }), { status: 200 });
  });
  const name = '中文/queue%2F?&+';
  const id = '旧任务/ID%2F?&+';
  await api.queueTasks({ queue: name, state: 'ready' });
  await api.pushTask(name, { payload: { action: 'test' } });
  await api.requeueDlq(name);
  await api.clearQueue(name, true, false, false);
  await api.deleteQueue(name);
  await api.task(id);
  await api.taskPayload(id, name);
  await api.replayTask(id, { queue: name });
  await api.deleteTask(id, name);
  for (const url of calls.slice(0, 5)) assert.equal(url.searchParams.get('name'), name);
  for (const url of calls.slice(5)) assert.equal(url.searchParams.get('task_id'), id);
});

test('批量部分完成、空批次和终止均明确表达结果', () => {
  assert.equal(summarizeBulkResult({ moved: 3 }), null);
  const success = summarizeBulkResult({ moved: 3, skipped: 0, errors: [] });
  assert.equal(success.warning, false);
  assert.match(success.message, /已处理 3 条/);
  const partial = summarizeBulkResult({ moved: 1, skipped: 1, not_attempted: 5,
    errors: [{ task_id: 'old', reason: 'start_deadline_at 已过' }], errors_truncated: true });
  assert.equal(partial.warning, true);
  for (const part of ['已处理 1 条', '跳过 1 条', '未尝试 5 条', 'old', 'start_deadline_at', '保留']) {
    assert.ok(partial.message.includes(part));
  }
  assert.equal(summarizeBulkResult({ moved: 0, skipped: 0 }).warning, false);
});

test('成功状态中的无效 JSON 不能被管理操作当成成功响应', async (t) => {
  t.mock.method(globalThis, 'fetch', async () => new Response('<html>bad gateway</html>', { status: 200 }));
  await assert.rejects(api.deleteQueue('test'), /无法识别的响应/);
});

test('真实登录模板仅允许同源跳转，反斜杠和跨源 URL 回退首页', async () => {
  const html = readFileSync(new URL('../../dashboard/templates/login.html', import.meta.url), 'utf8');
  const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
  for (const [next, expected] of [
    ['/queues?q=test#rows', '/queues?q=test#rows'],
    ['https://dashboard.test/tasks', '/tasks'],
    ['/\\evil.test/', '/'], ['//evil.test/', '/'], ['https://evil.test/', '/'],
    ['javascript:alert(1)', '/'], ['http://dashboard.test/', '/'],
  ]) {
    let submit;
    let destination;
    const button = { disabled: false, textContent: '登录' };
    const form = { querySelector: () => button, addEventListener: (_name, callback) => { submit = callback; } };
    const location = { origin: 'https://dashboard.test', search: `?next=${encodeURIComponent(next)}`,
      replace: value => { destination = value; } };
    vm.runInNewContext(script, {
      document: { getElementById: id => id === 'login-form' ? form : { textContent: '' } },
      window: { location }, URL, URLSearchParams,
      FormData: class { get() { return 'fixture'; } },
      fetch: async () => ({ ok: true, json: async () => ({ enabled: true, authenticated: false }) }),
    });
    await submit({ preventDefault() {} });
    assert.equal(destination, expected, next);
    assert.equal(button.disabled, false);
  }
});

test('登录网络失败显示反馈、恢复按钮并阻止等待期间重复提交', async () => {
  const html = readFileSync(new URL('../../dashboard/templates/login.html', import.meta.url), 'utf8');
  const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
  let submit;
  let release;
  let calls = 0;
  const pending = new Promise(resolve => { release = resolve; });
  const button = { disabled: false, textContent: '登录' };
  const errorBox = { textContent: '' };
  const form = { querySelector: () => button, addEventListener: (_name, callback) => { submit = callback; } };
  vm.runInNewContext(script, {
    document: { getElementById: id => id === 'login-form' ? form : errorBox },
    window: { location: { origin: 'https://dashboard.test', search: '' } }, URL, URLSearchParams,
    FormData: class { get() { return 'fixture'; } },
    fetch: async path => {
      if (path === '/api/auth') return { json: async () => ({ enabled: true, authenticated: false }) };
      calls++;
      await pending;
      throw new Error('network unavailable');
    },
  });
  const first = submit({ preventDefault() {} });
  await submit({ preventDefault() {} });
  assert.equal(calls, 1);
  assert.equal(button.disabled, true);
  release();
  await first;
  assert.match(errorBox.textContent, /连接失败.*重试/);
  assert.equal(button.disabled, false);
  assert.equal(button.textContent, '登录');
});

function clock(t) {
  let now = 1_000_000;
  t.mock.method(Date, 'now', () => now);
  return (milliseconds = 0) => (now += milliseconds);
}
function storage(t) {
  const values = new Map();
  const previous = Object.getOwnPropertyDescriptor(globalThis, 'localStorage');
  const instance = {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key),
  };
  Object.defineProperty(globalThis, 'localStorage', { value: instance, configurable: true });
  t.after(() => previous ? Object.defineProperty(globalThis, 'localStorage', previous) : delete globalThis.localStorage);
  return instance;
}
const queue = (patch = {}) => ({ name: 'review:fetch', queue: 20, active_workers: 1,
  completed_total: 0, observation_indexed: 1, processing: 0, retry: 0, delay: 0, ...patch });
const failed = { outcome: 'failed', status: 'failed', location: 'dlq', action: 'fetch_quote',
  payload_descriptor: { kind: 'inline', data: { symbol: 'AAPL' } } };

test('按 ID 打开执行中任务时使用最新 location，忽略旧列表状态', () => {
  assert.equal(taskState({ status: 'pending', outcome: '', location: 'processing' }, 'ready'), 'processing');
  assert.equal(taskState({ outcome: 'completed', location: 'history' }, 'processing'), 'completed');
});
test('执行中的任务即使已过开始截止，也不能变成可重放的过期视图', () => {
  assert.equal(taskState({ location: 'processing', start_deadline_at: 900 }, 'deadline_missed', 1000), 'processing');
  assert.equal(taskState({ location: 'ready', start_deadline_at: 900 }, '', 1000), 'deadline_missed');
  assert.equal(taskState({ location: 'delay', delay_reason: 'retry', available_at: 1100 }, '', 1000), 'retry_wait');
});
test('终态与可重放条件符合后端契约，保留旧记录状态回退', () => {
  assert.equal(taskState(failed), 'dlq');
  assert.equal(taskState({ outcome: 'skipped', location: 'history' }, 'ready'), 'skipped');
  assert.equal(taskState({ status: 'processing' }), 'processing');
  assert.equal(taskState({}, 'dlq'), 'dlq');
  assert.equal(taskState({}), 'unknown');
  assert.equal(canReplayTask(failed), true);
  for (const outcome of ['failed', 'skipped', 'cancelled']) {
    assert.equal(canReplayTask({ ...failed, outcome, start_deadline_at: 1 }), true);
  }
  assert.equal(canReplayTask({ ...failed, outcome: 'completed' }), false);
  assert.equal(canReplayTask({ status: 'failed', action: 'legacy' }), false);
});
test('错误与截止读取真实 V2 字段，并兼容旧错误对象', () => {
  assert.equal(taskError({ last_error: 'HTTP 429', error: 'old error' }), 'HTTP 429');
  assert.equal(taskError({ last_error: { code: 'timeout', reason: '连接超时' } }), '连接超时');
  assert.equal(taskError({ reason: 'deadline_missed' }), 'deadline_missed');
  assert.equal(taskDeadline({ start_deadline_at: '', expires_at: '1234' }), 1234);
  assert.equal(taskDeadline({}), null);
});
test('血缘同时支持逗号文本和旧 JSON 数组，保留每个新任务 ID', () => {
  assert.deepEqual(replayIds('new-a,new-b,new-a'), ['new-a', 'new-b']);
  assert.deepEqual(replayIds('["new-a","new-b"]'), ['new-a', 'new-b']);
  assert.deepEqual(replayIds(['new-a', 'new-b']), ['new-a', 'new-b']);
});
test('全局与单队列查询都保留 epoch 零边界和精确 action', async (t) => {
  const requests = [];
  t.mock.method(globalThis, 'fetch', async (path) => {
    requests.push(new URL(path, 'http://localhost'));
    return new Response(JSON.stringify({ tasks: [] }), { status: 200 });
  });
  const range = { state: 'all', action: 'fetch_quote', createdAfter: 0, completedBefore: 0 };
  await api.tasks(range);
  await api.queueTasks({ queue: 'review:fetch', ...range });
  for (const request of requests) {
    assert.equal(request.searchParams.get('created_after'), '0');
    assert.equal(request.searchParams.get('completed_before'), '0');
    assert.equal(request.searchParams.get('action'), 'fetch_quote');
  }
});
test('Worker 行恢复请求明确携带 Worker 标识并保持安全恢复语义', async (t) => {
  let body;
  t.mock.method(globalThis, 'fetch', async (_path, options) => {
    body = JSON.parse(options.body);
    return new Response(JSON.stringify({ recovered: 1 }), { status: 200 });
  });
  await api.recoverQueue('review:fetch', false, 'lost-a');
  assert.equal(body.worker_id, 'lost-a');
  assert.equal(body.include_active, false);
  assert.equal(body.confirm_active, false);
});
test('采满 30 秒才显示速率，待处理量不重复计算 retry_wait', (t) => {
  const advance = clock(t);
  const sampler = new RateSampler();
  sampler.observe([queue({ delay: 5, retry_wait: 4 })]);
  assert.equal(sampler.progress('review:fetch', queue()).ratePerMin, null);
  for (let index = 1; index <= 6; index++) { advance(5000); sampler.observe([queue({ completed_total: index })]); }
  const progress = sampler.progress('review:fetch', queue({ delay: 5, retry_wait: 4 }));
  assert.equal(progress.ratePerMin, 12);
  assert.equal(progress.remaining, 25);
  assert.equal(progress.etaText, null);
});
test('空闲后刚来任务不会继承过去的空闲时间而误报停滞', (t) => {
  const advance = clock(t);
  const sampler = new RateSampler();
  sampler.observe([queue({ queue: 0 })]);
  advance(120000); sampler.observe([queue({ queue: 0 })]);
  advance(5000); sampler.observe([queue()]);
  assert.equal(sampler.progress('review:fetch', queue()).stalled, false);
});
test('连续有积压两分钟且无完成才提示停滞，进展和观测中断会重置', (t) => {
  const advance = clock(t);
  const sampler = new RateSampler();
  sampler.observe([queue()]);
  for (let index = 0; index < 4; index++) { advance(30000); sampler.observe([queue()]); }
  assert.equal(sampler.progress('review:fetch', queue()).stalled, true);
  advance(5000); sampler.observe([queue({ completed_total: 1 })]);
  assert.equal(sampler.progress('review:fetch', queue({ completed_total: 1 })).stalled, false);
  advance(180000); sampler.observe([queue({ completed_total: 1 })]);
  assert.equal(sampler.progress('review:fetch', queue({ completed_total: 1 })).stalled, false);
});
test('无 Worker、纯延迟与未知旧指标不会被推断为消费停滞', (t) => {
  const advance = clock(t);
  for (const patch of [{ active_workers: 0 }, { queue: 0, delay: 20 }, { observation_indexed: 0 }]) {
    const sampler = new RateSampler();
    sampler.observe([queue(patch)]);
    for (let index = 0; index < 6; index++) { advance(30000); sampler.observe([queue(patch)]); }
    const progress = sampler.progress('review:fetch', queue(patch));
    assert.equal(progress.stalled, false);
    if (patch.observation_indexed === 0) assert.equal(progress.ratePerMin, null);
  }
});
test('清理或指标重建导致累计计数回退时重新采样', (t) => {
  const advance = clock(t);
  const sampler = new RateSampler();
  sampler.observe([queue({ completed_total: 100 })]);
  advance(30000); sampler.observe([queue({ completed_total: 110 })]);
  assert.equal(sampler.progress('review:fetch', queue()).completedObserved, 10);
  advance(5000); sampler.observe([queue({ completed_total: 0 })]);
  assert.equal(sampler.progress('review:fetch', queue()).completedObserved, null);
});
test('告警恢复的同一轮计算即移除旧告警，确认立即改变输出', (t) => {
  clock(t); storage(t);
  const engine = new AlertEngine();
  const bad = queue({ dlq: 1 });
  assert.equal(engine.compute([bad], [], null).length, 1);
  resolveAlert('dlq:review:fetch');
  assert.equal(engine.compute([bad], [], null).length, 0);
  assert.equal(Object.keys(loadResolved()).length, 1);
  assert.equal(engine.compute([queue()], [], null).length, 0);
  assert.equal(engine.compute([bad], [], null).length, 1);
});
test('页面重载保留持续故障的确认，恢复后再复发才重新提示', (t) => {
  const advance = clock(t); storage(t);
  const bad = queue({ dlq: 1 });
  new AlertEngine().compute([bad], [], null);
  resolveAlert('dlq:review:fetch');
  advance(5000);
  const reloaded = new AlertEngine();
  assert.equal(reloaded.compute([bad], [], null).length, 0);
  reloaded.compute([queue()], [], null);
  assert.equal(reloaded.compute([bad], [], null).length, 1);
  clearResolved();
  assert.deepEqual(loadResolved(), {});
});
test('保留旧确认记录，非法本地存储安全降级，写入失败明确抛出', (t) => {
  clock(t); const local = storage(t);
  local.setItem('qtask.alerts.resolved.v1', '{"dlq:review:fetch":900000}');
  assert.equal(new AlertEngine().compute([queue({ dlq: 1 })], [], null).length, 0);
  local.setItem('qtask.alerts.resolved.v2', 'null');
  assert.equal(new AlertEngine().compute([queue({ dlq: 1 })], [], null).length, 1);
  local.setItem = () => { throw new Error('storage denied'); };
  assert.throws(() => resolveAlert('dlq:review:fetch'), /storage denied/);
});
