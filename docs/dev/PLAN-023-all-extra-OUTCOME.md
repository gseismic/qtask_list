# PLAN-023 结果：增加 all 安装选项

## 完成情况

- 在 `pyproject.toml` 中增加 `all` extra，列出 `dev`、`dashboard`、`storage` 三组依赖的
  去重并集，共 10 项，并沿用各组现有版本下限。
- 在 README 安装说明中增加 `pip install -e ".[all]"`，注明它包含开发工具。
- 新增设计记录，比较显式组合、全部可选依赖聚合、仅运行时依赖聚合三种方案，并完成三轮
  语义、范围和一致性检查。
- 更新 `docs/dev/INDEX.md`。

## 复核与验证

- 人工逐项对照三个原有 extra：`dev` 的 6 项、`dashboard` 的 3 项和 `storage` 的 3 项均被
  覆盖；重叠的 FastAPI 与 Uvicorn 在 `all` 中各保留一项。
- `git diff --check` 通过。
- 本次只修改安装元数据与文档，未运行测试。
