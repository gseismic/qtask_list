async page => {
  const origin = await page.evaluate(() => location.origin);
  const folder = '/home/lsl/macbook/pai-studio-fin/projects/stockev.qtask_list/docs/test/20261017-17-WORKTREE/browser-3';
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(`${origin}/workers`);
  await page.locator('.topbar select').selectOption('0');
  await page.getByText('达到告警阈值', { exact: true }).waitFor();
  const memory = await page.evaluate(async () => (await (await fetch('/api/health')).json()).memory);
  if (!(memory.status === 'warning' && memory.threshold_source === 'maxmemory' && memory.used_memory > memory.maxmemory)) throw new Error(JSON.stringify(memory));
  await page.screenshot({ path: `${folder}/memory-over-limit-warning.png` });
  await page.goto(`${origin}/alerts`);
  await page.locator('.topbar select').selectOption('0');
  await page.locator('.page .badge').filter({ hasText: 'Redis 内存超限' }).waitFor();
  if (errors.length) throw new Error(JSON.stringify(errors));
  return { passed: 2, checks: ['真实 Redis 超过 maxmemory 时显示达到阈值', '告警页展示内存超限'], memory, errors };
}
