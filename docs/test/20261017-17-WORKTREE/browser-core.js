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
    await page.waitForFunction(() => !document.querySelector('.skel'));
  };
  const drawer = page.getByRole('dialog', { name: '任务详情', exact: true });
  await openQueue('review:table', 'ready');
  const row = page.locator('tbody tr').first();
  await row.waitFor();
  const cells = await row.locator('td').allTextContents();
  assert(cells[2] === '0' && cells[4] !== '—' && cells[5] !== '—', JSON.stringify(cells));
  await row.focus();
  await page.keyboard.press('Enter');
  await drawer.waitFor();
  for (const key of ['Shift+Tab', 'Tab', 'Tab', 'Shift+Tab']) {
    await page.keyboard.press(key);
    assert(await drawer.evaluate(element => element.contains(document.activeElement)), '抽屉焦点逃逸');
  }
  await page.locator('.topbar select').evaluate(element => element.focus());
  assert(await drawer.evaluate(element => element.contains(document.activeElement)), '背景程序焦点未被限制');
  await page.keyboard.press('Escape');
  await drawer.waitFor({ state: 'hidden' });
  assert(await row.evaluate(element => element === document.activeElement), '关闭抽屉未恢复任务行焦点');
  checks.push({ name: '列表完整元数据；抽屉进入、循环、限制背景焦点与恢复', cells });

  for (const [name, state, expected] of [['review:overdue', 'ready', '过期'], ['review:retry', 'delay', '等待重试']]) {
    await openQueue(name, state);
    const item = page.locator('tbody tr').first();
    await item.waitFor();
    assert(await item.locator('.badge').innerText() === expected, `${name} 表格状态不一致`);
    await item.click();
    await drawer.locator('.drawer-head .badge').filter({ hasText: expected }).waitFor();
    checks.push({ name: `${name} 列表与抽屉均为 ${expected}` });
    await page.screenshot({ path: `${folder}/${state}-state-consistency.png` });
  }

  await openQueue('review:large', 'ready');
  await page.waitForFunction(() => document.querySelectorAll('tbody tr').length === 50);
  for (let expected = 100; expected <= 500; expected += 50) {
    await page.getByRole('button', { name: /^加载更多/ }).click();
    await page.waitForFunction(count => document.querySelectorAll('tbody tr').length === count, expected);
  }
  await page.getByRole('button', { name: /^加载更多/ }).click();
  await page.waitForFunction(() => document.querySelectorAll('tbody tr').length === 501);
  // 最后一页可能还需要完成无历史消息诊断扫描。
  while (await page.getByRole('button', { name: /^加载更多/ }).count()) {
    await page.getByRole('button', { name: /^加载更多/ }).click();
    await page.waitForFunction(() => ![...document.querySelectorAll('button')].some(button => button.textContent === '加载中…'));
  }
  const ids = await page.locator('tbody tr td:first-child').evaluateAll(elements => elements.map(element => element.title));
  assert(ids.length === 501 && new Set(ids).size === 501, '501 条分页不完整或重复');
  assert(!await page.getByRole('checkbox', { name: '自动刷新' }).isChecked(), '续页没有暂停自动刷新');
  checks.push({ name: '501 条真实任务完整分页且无重复', displayed: ids.length });
  await page.screenshot({ path: `${folder}/pagination-501.png` });

  await openQueue('review:sparse', 'all');
  await page.getByRole('button', { name: /^高级筛选/ }).click();
  await page.getByLabel('发布起', { exact: false }).fill('1970-01-01T00:00');
  const filtered = page.waitForResponse(response => response.url().includes('/api/queues/tasks?') && response.url().includes('action=rare'));
  await page.getByLabel('精确 action', { exact: false }).fill('rare');
  assert((await filtered).status() === 200, '稀疏时间筛选失败');
  await page.waitForFunction(() => document.body.textContent.includes('本页已达到扫描上限'));
  await page.getByRole('button', { name: /^加载更多/ }).click();
  await page.locator('tbody tr').first().waitFor();
  assert(await page.locator('tbody tr').count() === 1, '稀疏续页返回数量错误');
  assert(await page.locator('tbody tr td:first-child').getAttribute('title') === 'review-sparse-5000', '稀疏续页漏掉末尾记录');
  checks.push({ name: '5001 条历史的时间与精确 action 稀疏筛选可继续扫描' });
  await page.screenshot({ path: `${folder}/sparse-filter-found.png` });

  await openQueue('review:bulk', 'dlq');
  await page.waitForFunction(() => document.querySelectorAll('tbody tr').length === 2);
  await page.getByRole('button', { name: '操作 ▾', exact: true }).click();
  await page.getByRole('button', { name: '重放全部 DLQ', exact: true }).click();
  const confirm = page.getByRole('dialog', { name: '重放全部 DLQ', exact: true });
  await confirm.waitFor();
  const replayed = page.waitForResponse(response => response.url().includes('/api/queues/requeue-dlq?') && response.request().method() === 'POST');
  await confirm.getByRole('button', { name: '重放', exact: true }).click();
  const response = await replayed;
  const result = await response.json();
  assert(response.status() === 200 && result.moved === 1 && result.skipped === 1, JSON.stringify(result));
  await confirm.getByRole('alert').waitFor();
  const message = await confirm.getByRole('alert').innerText();
  assert(message.includes('已处理 1 条，跳过 1 条') && message.includes('start_deadline_at'), message);
  assert(await confirm.count() === 1, '部分完成被关闭隐藏');
  await page.screenshot({ path: `${folder}/bulk-partial-visible.png` });
  await page.keyboard.press('Escape');
  await confirm.waitFor({ state: 'hidden' });
  assert(await page.getByRole('button', { name: '操作 ▾', exact: true }).evaluate(element => element === document.activeElement), '批量弹窗未恢复菜单触发器');
  checks.push({ name: '批量部分完成保留弹窗与消息，恢复触发器焦点', result, message });
  assert(failures.length === 0, JSON.stringify(failures));
  return { passed: checks.length, checks, failures };
}
