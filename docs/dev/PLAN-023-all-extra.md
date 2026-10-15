# PLAN-023：增加 all 安装选项

## 目标

为 qtask_list 增加 `all` 可选依赖组，使使用者可以通过 `pip install -e ".[all]"` 一次安装
基础依赖与所有可选依赖。

## 设计

采用 `all` 覆盖 `dev`、`dashboard`、`storage` 三组可选依赖的去重并集。该组包含开发工具，
README 会明确说明。方案比较和三轮查漏补缺记录在
`docs/design/install-extras-20261016-all.md`。

## 实施范围

1. 更新 `pyproject.toml`，增加依赖去重后的 `all` extra。
2. 更新 README 安装示例，展示 `all` 命令及包含范围。
3. 复核依赖并集、文档差异和最终变更，记录结果并更新 `docs/dev/INDEX.md`。

## 验收条件

- `all` 覆盖当前 `dev`、`dashboard`、`storage` 的全部依赖。
- README 中的安装命令与 extras 名称一致，并注明 `all` 含开发依赖。
- 计划、结果和设计文件进入索引；变更以中文提交并推送。
