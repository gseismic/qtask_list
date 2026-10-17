async page => {
  const origin = await page.evaluate(() => location.origin);
  const folder = '/home/lsl/macbook/pai-studio-fin/projects/stockev.qtask_list/docs/test/20261017-17-WORKTREE/browser-3';
  const checks = [];
  const failures = [];
  const assert = (value, message) => { if (!value) throw new Error(message); };
  page.on('pageerror', error => failures.push(error.message));
  page.on('response', response => { if (response.status() >= 400) failures.push(`${response.status()} ${response.url()}`); });
  await page.goto(`${origin}/workers`);
  await page.locator('.topbar select').selectOption('0');
  const selected = page.locator('tbody tr').filter({ hasText: 'review-stale-a' });
  await selected.getByRole('button', { name: '恢复', exact: true }).click();
  const confirm = page.getByRole('dialog', { name: '恢复失联 Worker review-stale-a', exact: true });
  const recovered = page.waitForResponse(response => response.url().includes('/api/queues/recover?') && response.request().method() === 'POST');
  await confirm.getByRole('button', { name: '恢复', exact: true }).click();
  const response = await recovered;
  const result = await response.json();
  assert(result.recovered === 1 && response.request().postDataJSON().worker_id === 'review-stale-a', JSON.stringify(result));
  await confirm.waitFor({ state: 'hidden' });
  const workers = await page.evaluate(async () => await (await fetch('/api/workers?queue=review%3Aworkers')).json());
  assert(workers.length === 1 && workers[0].worker_id === 'review-stale-b' && workers[0].processing === 1, JSON.stringify(workers));
  await page.getByText('内存告警未启用', { exact: true }).waitFor();
  await page.screenshot({ path: `${folder}/worker-recovery-memory-unconfigured.png` });
  checks.push({ name: '仅恢复选中的失联 Worker，其他 processing 保留；未配置内存告警明确显示', result, workers });

  await page.goto(`${origin}/`);
  await page.locator('.topbar select').selectOption('0');
  await page.getByRole('button', { name: /^活跃队列 / }).click();
  await page.waitForURL(`${origin}/queues?filter=active`);
  await page.goto(`${origin}/`);
  await page.locator('.topbar select').selectOption('0');
  await page.getByRole('button', { name: /^总待处理 / }).click();
  await page.waitForURL(`${origin}/queues?filter=active`);
  await page.goto(`${origin}/`);
  await page.locator('.topbar select').selectOption('0');
  await page.getByRole('button', { name: /^异常队列 / }).click();
  assert(await page.locator('#overview-matrix .seg button.on').innerText() === '仅异常', '异常 KPI 未切换筛选');
  assert(await page.locator('.matrix-group .group-head[aria-expanded="false"]').count() === 0, '异常分组未展开');
  assert(await page.getByRole('button', { name: /^合计观测速率/ }).count() === 0, '速率卡被错误做成按钮');
  checks.push({ name: '三个总览 KPI 的跳转、筛选、展开行为正确，速率卡保持静态' });

  const smoke = [];
  for (const width of [1366, 1024, 375]) {
    await page.setViewportSize({ width, height: 900 });
    for (const path of ['/', '/queues', '/tasks', '/workers', '/alerts', '/guide']) {
      await page.goto(`${origin}${path}`);
      await page.locator('.topbar select').selectOption('0');
      await page.locator('h1').waitFor();
      await page.waitForFunction(() => !document.querySelector('.skel'));
      const dimensions = await page.evaluate(() => ({ viewport: innerWidth, page: document.documentElement.scrollWidth }));
      assert(dimensions.page <= dimensions.viewport + 1, `${path} ${width}px 页面整体溢出：${JSON.stringify(dimensions)}`);
      smoke.push({ path, width, title: await page.locator('h1').innerText(), dimensions });
      if (width === 375 && path === '/') await page.screenshot({ path: `${folder}/overview-mobile.png`, fullPage: true });
      if (width === 1366 && path === '/queues') await page.screenshot({ path: `${folder}/queues-desktop.png` });
    }
  }
  await page.setViewportSize({ width: 1366, height: 900 });
  checks.push({ name: '六个页面在桌面、1024 和 375 宽度均可用，整体无横向溢出', smoke });
  await page.goto(`${origin}/alerts`);
  await page.locator('.topbar select').selectOption('0');
  const resolve = page.getByRole('button', { name: '标记已处理', exact: true }).first();
  await resolve.waitFor();
  await resolve.click();
  await page.getByRole('button', { name: /^已处理 / }).click();
  await page.locator('.page code.mono').first().waitFor();
  const key = await page.locator('.page code.mono').first().innerText();
  await page.reload();
  await page.locator('.topbar select').selectOption('0');
  await page.getByRole('button', { name: /^已处理 / }).click();
  await page.locator('.page code.mono').filter({ hasText: key }).waitFor();
  assert(!await page.locator('.page').innerText().then(text => text.includes('Invalid Date')), '告警处理时间显示错误');
  checks.push({ name: '告警确认即时生效且重新加载后保留', key });
  assert(failures.length === 0, JSON.stringify(failures));
  return { passed: checks.length, checks, failures };
}
