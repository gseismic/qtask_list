async page => {
  const origin = 'http://127.0.0.1:33147';
  const artifacts = '/home/lsl/macbook/pai-studio-fin/projects/stockev.qtask_list/docs/test/20261017-16-WORKTREE/browser';
  const results = {};
  const pageErrors = [];
  page.on('pageerror', error => pageErrors.push(error.message));
  const openQueue = async (name, state) => {
    await page.goto(`${origin}/queues/${encodeURIComponent(name)}?state=${state}`);
    await page.locator('.topbar select').selectOption('0');
    await page.waitForFunction(() => !document.querySelector('.skel'));
  };
  const screenshot = async name => page.screenshot({ path: `${artifacts}/${name}.png` });

  await openQueue('review:table', 'ready');
  await page.locator('tbody tr').first().waitFor();
  results.metadata = { cells: await page.locator('tbody tr').first().locator('td').allTextContents() };
  await screenshot('review-table-missing-times');
  await page.locator('tbody tr').first().focus();
  await page.keyboard.press('Enter');
  await page.getByRole('dialog', { name: '任务详情' }).waitFor();
  await page.keyboard.press('Shift+Tab');
  results.drawerFocus = await page.evaluate(() => ({
    focusInside: document.querySelector('.drawer')?.contains(document.activeElement),
    activeElement: document.activeElement?.outerHTML.slice(0, 250),
  }));

  await openQueue('review:overdue', 'ready');
  await page.locator('tbody tr').first().waitFor();
  const overdueRowLabel = await page.locator('tbody tr').first().locator('.badge').innerText();
  await page.locator('tbody tr').first().click();
  const drawer = page.getByRole('dialog', { name: '任务详情' });
  await drawer.locator('.drawer-head .badge').filter({ hasText: '过期' }).waitFor();
  results.overdueState = { rowLabel: overdueRowLabel, drawerLabel: await drawer.locator('.drawer-head .badge').innerText() };
  await screenshot('review-overdue-row-vs-drawer');

  await openQueue('review:retry', 'delay');
  await page.locator('tbody tr').first().waitFor();
  const retryRowLabel = await page.locator('tbody tr').first().locator('.badge').innerText();
  await page.locator('tbody tr').first().click();
  await drawer.locator('.drawer-head .badge').filter({ hasText: '等待重试' }).waitFor();
  results.retryState = { rowLabel: retryRowLabel, drawerLabel: await drawer.locator('.drawer-head .badge').innerText() };

  await openQueue('review:large', 'ready');
  await page.waitForFunction(() => document.querySelectorAll('tbody tr').length === 50);
  for (let count = 100; count <= 500; count += 50) {
    await page.getByRole('button', { name: /^加载更多/ }).click();
    await page.waitForFunction(expected => document.querySelectorAll('tbody tr').length === expected, count);
  }
  results.rawPagination = {
    displayed: await page.locator('tbody tr').count(),
    loadMoreButtons: await page.getByRole('button', { name: /^加载更多/ }).count(),
    readyCount: await page.locator('.faint').filter({ hasText: '当前该状态 501 条' }).count(),
  };
  await screenshot('review-list-stops-at-500');

  await openQueue('review:sparse', 'ready');
  await page.getByRole('button', { name: /^高级筛选/ }).click();
  const sparseResponse = page.waitForResponse(response => response.url().includes('/api/queue/review%3Asparse/tasks') &&
    response.url().includes('created_after='));
  await page.getByLabel('发布起', { exact: false }).fill('1970-01-01T00:00');
  const response = await sparseResponse;
  await page.getByRole('alert').waitFor();
  results.sparseTimeFilter = { status: response.status(), alert: await page.getByRole('alert').allTextContents() };
  await screenshot('review-time-filter-500');

  await openQueue('review/slash', 'ready');
  await page.getByRole('alert').waitFor();
  results.slashQueue = { title: await page.locator('h1').innerText(), errors: await page.getByRole('alert').allTextContents() };

  await openQueue('review:bulk', 'dlq');
  await page.locator('tbody tr').first().waitFor();
  await page.getByRole('button', { name: '操作 ▾', exact: true }).click();
  await page.getByRole('button', { name: '重放全部 DLQ', exact: true }).click();
  const confirm = page.getByRole('dialog', { name: '重放全部 DLQ', exact: true });
  await confirm.waitFor();
  const replayed = page.waitForResponse(result => result.url().endsWith('/requeue-dlq') && result.request().method() === 'POST');
  await confirm.getByRole('button', { name: '重放', exact: true }).click();
  const replayResponse = await replayed;
  await confirm.waitFor({ state: 'hidden' });
  results.bulkReplay = {
    status: replayResponse.status(), result: await replayResponse.json(),
    dialogClosed: await confirm.count() === 0, errors: await page.getByRole('alert').allTextContents(),
  };
  await screenshot('review-bulk-replay-silent-skip');

  const smoke = [];
  for (const path of ['/', '/queues', '/tasks', '/workers', '/alerts', '/guide']) {
    await page.goto(`${origin}${path}`);
    await page.locator('h1').waitFor();
    await page.locator('.topbar select').selectOption('0');
    await page.waitForFunction(() => !document.querySelector('.skel'));
    smoke.push({ path, title: await page.locator('h1').innerText() });
  }
  results.smoke = smoke;

  await page.route('https://review-origin.invalid/**', route => route.fulfill({
    status: 200, contentType: 'text/html', body: '<!doctype html><title>Review origin</title><p>隔离跨源验证</p>',
  }));
  await page.goto('https://review-origin.invalid/');
  results.crossOrigin = await page.evaluate(async target => {
    try {
      const response = await fetch(`${target}/api/queue/review%3Across-origin/tasks`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ payload: { action: 'review_action', source: 'cross-origin-browser' } }),
        signal: AbortSignal.timeout(5000),
      });
      return { pageOrigin: location.origin, status: response.status, body: await response.json(), userAgent: navigator.userAgent };
    } catch (error) {
      return { pageOrigin: location.origin, error: String(error), userAgent: navigator.userAgent };
    }
  }, origin);
  results.pageErrors = pageErrors;
  return results;
}
