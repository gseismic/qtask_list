async page => {
  const origin = await page.evaluate(() => location.origin);
  const folder = '/home/lsl/macbook/pai-studio-fin/projects/stockev.qtask_list/docs/test/20261017-17-WORKTREE/browser-final';
  const checks = [];
  const failures = [];
  const assert = (value, message) => { if (!value) throw new Error(message); };
  page.on('pageerror', error => failures.push(error.message));
  page.on('response', response => { if (response.status() >= 400) failures.push(`${response.status()} ${response.url()}`); });
  const openQueue = async (name, state) => {
    await page.goto(`${origin}/queues/${encodeURIComponent(name)}?state=${state}`);
    await page.locator('.topbar select').selectOption('0');
    await page.locator('tbody tr').first().waitFor();
  };
  const drawer = page.getByRole('dialog', { name: '任务详情', exact: true });
  await openQueue('review:replay', 'dlq');
  const originalId = await page.locator('tbody tr td:first-child').getAttribute('title');
  await page.locator('tbody tr').first().click();
  await drawer.getByRole('button', { name: '重放为新任务', exact: true }).waitFor();
  const opener = drawer.getByRole('button', { name: '重放为新任务', exact: true });
  await opener.click();
  const confirm = page.getByRole('dialog', { name: '重放为新任务', exact: true });
  await confirm.waitFor();
  await page.keyboard.press('Shift+Tab');
  assert(await confirm.evaluate(element => element.contains(document.activeElement)), '嵌套确认焦点逃逸');
  await page.keyboard.press('Escape');
  await confirm.waitFor({ state: 'hidden' });
  assert(await drawer.count() === 1 && await opener.evaluate(element => element === document.activeElement), 'Escape 未只关闭顶层或未恢复焦点');
  checks.push({ name: '嵌套确认独占 Tab / Esc，关闭后恢复抽屉按钮' });

  await opener.click();
  let release;
  let heldResolve;
  const gate = new Promise(resolve => { release = resolve; });
  const held = new Promise(resolve => { heldResolve = resolve; });
  const delayedReplay = async route => { heldResolve(); await gate; await route.continue(); };
  await page.route('**/api/tasks/replay?*', delayedReplay);
  const responseWait = page.waitForResponse(response => response.url().includes('/api/tasks/replay?') && response.request().method() === 'POST');
  await confirm.getByRole('button', { name: '重放', exact: true }).click();
  await held;
  await confirm.getByRole('button', { name: '执行中…', exact: true }).waitFor();
  await page.keyboard.press('Escape');
  await page.keyboard.press('Tab');
  assert(await confirm.count() === 1 && await confirm.evaluate(element => element.contains(document.activeElement)), '忙碌确认可以退出或焦点逃逸');
  await page.locator('.topbar select').evaluate(element => element.focus());
  assert(await confirm.evaluate(element => element.contains(document.activeElement)), '忙碌状态允许背景焦点');
  await page.screenshot({ path: `${folder}/nested-busy-focus.png` });
  release();
  const response = await responseWait;
  const first = await response.json();
  assert(response.status() === 200 && first.accepted, JSON.stringify(first));
  await confirm.waitFor({ state: 'hidden' });
  await page.unroute('**/api/tasks/replay?*', delayedReplay);
  await drawer.getByRole('button', { name: '查看新任务', exact: true }).waitFor();
  const secondWait = page.waitForResponse(result => result.url().includes('/api/tasks/replay?') && result.request().method() === 'POST');
  await opener.click();
  await confirm.getByRole('button', { name: '重放', exact: true }).click();
  const second = await (await secondWait).json();
  await confirm.waitFor({ state: 'hidden' });
  await drawer.locator(`button[title="${second.task_id}"]`).waitFor();
  const records = await page.evaluate(async ids => await Promise.all(ids.map(async id =>
    await (await fetch(`/api/tasks/detail?task_id=${encodeURIComponent(id)}`)).json()
  )), [originalId, first.task_id, second.task_id]);
  assert(records[0].replayed_by.split(',').join(',') === `${first.task_id},${second.task_id}`, '血缘被覆盖');
  for (const record of records.slice(1)) assert(record.max_attempts === 7 && record.attempt === 0 && record.replay_of === originalId, JSON.stringify(record));
  await drawer.getByRole('button', { name: '查看新任务', exact: true }).click();
  await drawer.locator(`[title="${second.task_id}"]`).waitFor();
  await drawer.locator(`button[title="${originalId}"]`).waitFor();
  assert((await drawer.innerText()).includes('0/7'), '新任务抽屉预算不一致');
  await drawer.locator(`button[title="${originalId}"]`).click();
  await drawer.locator(`[title="${originalId}"]`).first().waitFor();
  checks.push({ name: '忙碌保护；重复重放保留预算 7 和全部血缘，支持来回导航', originalId, newIds: [first.task_id, second.task_id] });
  await page.screenshot({ path: `${folder}/replay-lineage.png` });

  for (const [name, state, endpoint, expectedBudget] of [
    ['review:overdue', 'ready', '/api/queues/requeue-expired?', 3],
    ['review:bulk', 'dlq', '/api/tasks/replay?', 1],
  ]) {
    await openQueue(name, state);
    const id = await page.locator('tbody tr td:first-child').first().getAttribute('title');
    await page.locator('tbody tr').first().click();
    await drawer.getByRole('button', { name: '更新截止并重放', exact: true }).click();
    const update = page.getByRole('dialog', { name: '更新截止并重放', exact: true });
    await update.waitFor();
    const local = await page.evaluate(() => {
      const date = new Date(Date.now() + 3600000);
      return new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
    });
    await update.getByLabel('新的执行截止时间', { exact: true }).fill(local);
    const updated = page.waitForResponse(result => result.url().includes(endpoint) && result.request().method() === 'POST');
    await update.getByRole('button', { name: '重放', exact: true }).click();
    const result = await (await updated).json();
    await update.waitFor({ state: 'hidden' });
    await drawer.getByRole('button', { name: '查看新任务', exact: true }).waitFor();
    const newId = result.new_task_id ?? result.task_id;
    const record = await page.evaluate(async id => await (await fetch(`/api/tasks/detail?task_id=${encodeURIComponent(id)}`)).json(), newId);
    assert(record.max_attempts === expectedBudget && record.start_deadline_at > Date.now()/1000 && record.replay_of === id, JSON.stringify(record));
    checks.push({ name: `${name} 更新截止并重放保持预算 ${expectedBudget}`, id, newId });
  }
  assert(failures.length === 0, JSON.stringify(failures));
  return { passed: checks.length, checks, failures };
}
