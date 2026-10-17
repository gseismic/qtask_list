async page => {
  const origin = 'http://127.0.0.1:33147';
  await page.goto(`${origin}/workers`);
  await page.locator('.topbar select').selectOption('0');
  await page.waitForFunction(() => document.querySelectorAll('.kv dd').length === 4);
  const health = await page.evaluate(() => fetch('/api/health').then(response => response.json()));
  const state = await page.locator('.kv dd').last().innerText();
  const nextUrl = '/\\review-redirect.invalid/';
  const redirectCheck = await page.evaluate(nextUrl => {
    const safeNext = nextUrl.startsWith('/') && !nextUrl.startsWith('//') ? nextUrl : '/';
    return { next: nextUrl, safeNext, target: new URL(safeNext, location.origin).href };
  }, nextUrl);
  await page.screenshot({ path: '/home/lsl/macbook/pai-studio-fin/projects/stockev.qtask_list/docs/test/20261017-16-WORKTREE/browser/review-memory-over-limit-healthy.png' });
  return { memory: health.memory, displayedState: state, redirectCheck };
}
