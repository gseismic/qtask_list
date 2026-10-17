async page => {
  const origin = await page.evaluate(() => location.origin);
  const checks = [];
  const errors = [];
  const external = [];
  const assert = (value, message) => { if (!value) throw new Error(message); };
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/review-origin.invalid/**', async route => { external.push(route.request().url()); await route.fulfill({ body: '<title>unexpected redirect</title>' }); });
  await page.goto(`${origin}/login?next=${encodeURIComponent('/queues?filter=active#scope')}`);
  await page.getByLabel('用户名', { exact: true }).fill('回归管理员');
  await page.getByLabel('密码', { exact: true }).fill('测试🔑密码');
  let release;
  let heldResolve;
  let calls = 0;
  const gate = new Promise(resolve => { release = resolve; });
  const held = new Promise(resolve => { heldResolve = resolve; });
  const networkError = async route => { calls++; heldResolve(); await gate; await route.abort('failed'); };
  await page.route('**/api/login', networkError);
  await page.getByRole('button', { name: '登录', exact: true }).click();
  await held;
  assert(!await page.getByRole('button', { name: '登录中…', exact: true }).isEnabled(), '登录等待期间可重复提交');
  await page.locator('#login-form').evaluate(form => form.dispatchEvent(new SubmitEvent('submit', { bubbles: true, cancelable: true })));
  assert(calls === 1, '重复提交产生第二次请求');
  release();
  await page.locator('#login-error').filter({ hasText: '连接失败' }).waitFor();
  assert(await page.getByRole('button', { name: '登录', exact: true }).isEnabled(), '失败后无法重试');
  await page.unroute('**/api/login', networkError);
  checks.push('网络中断显示反馈、恢复按钮并阻止重复提交');

  await page.getByLabel('密码', { exact: true }).fill('错误密码');
  const rejected = page.waitForResponse(response => response.url().endsWith('/api/login'));
  await page.getByRole('button', { name: '登录', exact: true }).click();
  assert((await rejected).status() === 401, '错误凭据未被拒绝');
  await page.locator('#login-error').filter({ hasText: '用户名或密码错误' }).waitFor();
  await page.getByLabel('密码', { exact: true }).fill('测试🔑密码');
  await page.getByRole('button', { name: '登录', exact: true }).click();
  await page.waitForURL(`${origin}/queues?filter=active#scope`);
  await page.locator('.topbar select').selectOption('0');
  const cookie = (await page.context().cookies()).find(item => item.name === 'qtask_dashboard_session');
  assert(cookie?.httpOnly && cookie.sameSite === 'Lax', '会话 Cookie 属性错误');
  checks.push('Unicode 用户名和密码登录；拒绝错误凭据；同源 next 保留参数和锚点；Cookie 生效');
  for (const next of ['/\\review-origin.invalid/steal', '//review-origin.invalid/steal', 'https://review-origin.invalid/steal']) {
    await page.context().clearCookies();
    await page.goto(`${origin}/login?next=${encodeURIComponent(next)}`);
    await page.getByLabel('密码', { exact: true }).fill('测试🔑密码');
    await page.getByRole('button', { name: '登录', exact: true }).click();
    await page.waitForURL(`${origin}/`);
    await page.locator('.topbar select').selectOption('0');
    checks.push(`跨源 next 回退首页：${next}`);
  }
  await page.getByRole('button', { name: '退出', exact: true }).click();
  await page.waitForURL(`${origin}/login`);
  const unauthorized = await page.evaluate(async () => (await fetch('/api/queues')).status);
  assert(unauthorized === 401, '退出后仍可访问管理接口');
  checks.push('退出会话后管理 API 返回 401');
  assert(errors.length === 0 && external.length === 0, JSON.stringify({ errors, external }));
  await page.screenshot({ path: '/home/lsl/macbook/pai-studio-fin/projects/stockev.qtask_list/docs/test/20261017-17-WORKTREE/browser-auth/login-reviewed.png' });
  return { passed: checks.length, checks, errors, external };
}
