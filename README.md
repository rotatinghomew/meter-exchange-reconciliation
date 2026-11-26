# 租赁流量计的换表资格与历史凭证补核

本项目用 Python 3.11 标准库把四件事连在同一个服务状态中：

1. **使用资格**：使用点、介质、计量单位、所需量程、预计使用时间；主机号 + 探头号的证书精确匹配；更正通知对时间/量程的限制。
2. **并发领用与交接**：锁内检查已确认/可执行领用，避免同一台仪器在重叠区间被两家确认；独立保存返还记录与库房验收；记录新旧表交接时刻和计量段组成。
3. **凭证复核**：只承认有实测边界的读数差，不按时间或最大/最小流量推算比例；跨换表、证书不适用区间的数量进入补核；匹配的并行参考表最多作为同一区间的一个来源，绝不与原表相加。
4. **双方确认与权限回查**：协议中的供方/需方共同确认后生成可引用确认；原凭证、当初读数、更正后有效量分别回查；仪器还库不撤销历史凭证、历史工单或其他已确认领用。

无第三方运行依赖。合成资料在 `data/source_records.json`。

## 运行与测试

```bash
# 全部测试（原有资料检查 + 本服务行为/并发/HTTP 测试）
python3 -m unittest discover -s tests -v

# 直接查看本地合成场景的关键结论
python3 server.py --demo
# 或
python3 -m meter_service --demo

# 启动 JSON HTTP 服务，默认 127.0.0.1:8000
python3 server.py --host 0.0.0.0 --port 8000
```

也可以直接在 Python 中使用：

```python
from meter_service import MeterService

service = MeterService()
print(service.recommend_for_request("W2"))
print(service.analyze_receipt("V1", actor="plant-a"))
service.propose_review("RV-V1-F17", actor="plant-a")
print(service.confirm_review("RV-V1-F17", actor="buyer-b"))
```

根目录的 `service.py`、`app.py`、`server.py`、`main.py` 是导入/启动兼容入口；主要实现位于 `meter_service/core.py` 和 `meter_service/server.py`。

## 合成资料中的确定结论

- 新工单 **W2**（`line-b`，`food-water`，量程 `[5,60]`，2026-11-28 10:00–18:00）首选 **FM18/B89/C51**，实际/证书量程 `[5,150]`，可交付窗口为 10:00–18:00。FM19/B90/C52 也合格，但量程更大，排在后面。
- H1 规定 `line-a` 在 `2026-11-27T11:00:00+08:00` 由 FM18 换成 FM19。WO-W1 的仪器链为：
  - FM18/B89：09:30–11:00；
  - FM19/B90：11:00–12:00。
- H2 的现场返还记录为 11:15，库房验收入库为 12:00。`[11:15,12:00)` 是两个来源之间的保管争议区间；下一家整段领用最早从 12:00 开始安排。
- V1 原选读数差合计：
  - S18：`2870 - 350 = 2520 L`，其中相邻实测边界为 `870 + 1650`；
  - S19：`1802 - 102 = 1700 L`；
  - 原凭证合计 **4220 L**。
- F17 只更正 C51/FM18/B89 在 11-27 10:00–12:00、量程 70–150 的历史范围。S18 的汇总量程为 `[51,119]`，跨越 70 边界；最大/最小瞬时值不能证明每个累计增量中有多少低于 70，因此 S18 的 `870 L` 和 `1650 L` 都不能直接作为无争议据证量。
- R7 与 S18 第一小段具有相同使用点、介质和精确读数边界 `10:15–10:30`，并有独立 C58 证书，参考量为 `960 - 100 = 860 L`。R8 的使用点是 `line-b`，来源不匹配；即使它有 `1560 L` 的区间差，也不能补 V1 的证。
- S19/FM19/B90/C52 不受 F17 影响，`1700 L` 可直接引用。
- 系统默认补核建议为：R7 支持的第一段取参考量 **860 L**，第二段 `1650 L` 继续补核，S19 取 **1700 L**；双方确认后的可引用有效量为 **2560 L**，另有 **1650 L** 递延待证。双方也可在复核手续中明确约定接受原表第一段 `870 L`，此时确认有效量为 **2570 L**。
- F17 报告时（11-28 08:08）FM18 已经库房验收入库，当前没有重叠的现场使用，因此不需要现在停用 FM18；历史 W1/V1 仍需按当时实际用法复核。

## 主要 HTTP 接口

敏感凭证接口用 `X-Actor-Id` 表达当前业务身份。本地联调身份可用 `plant-a`（CA6 出具方/供方）、`buyer-b`（合同相对方/需方）和 `library`（租赁库）。未带头时按可信本地 `system` 调用处理；显式使用 `library` 不能读取或确认 CA6 的历史计量凭证。最小授权集成还可传 `X-Actor-Scopes: receipt:readings` 或 `receipt:original,receipt:corrected`，非协议方只能访问被明确授予的投影。

| 方法与路径 | 说明 |
| --- | --- |
| `POST /v1/quotes`；`GET /v1/availability?site=...&medium=...&lo=...&hi=...&start=...&end=...` | 提交/查询使用条件并返回候选主机、探头、证书、实际量程、限制原因和可交付窗口 |
| `GET /v1/requests` / `POST /v1/requests` | 查询/提交厂方领用请求 |
| `GET /v1/requests/{id}/recommendation` | 对已有请求给出推荐仪器套 |
| `POST /v1/requests/{id}/confirm` | 锁内把合格请求变成 `executable` 领用并生成工单；别名 `/execute`、`/allocate` |
| `GET /v1/work-orders/{id}` | 查询工单、仪器链、交接点和记录要求；`W1` 或 `WO-W1` 均可 |
| `GET /v1/meters/{id}` | 查询仪器的证书、更正、交接、领用段和返还验收时间线 |
| `POST /v1/handoffs` | 记录新旧仪器交接并拆分在执行领用段 |
| `POST /v1/returns` | 录入独立来源的返还记录 |
| `POST /v1/return-cases/{id}/accept` | 录入库房验收时间 |
| `POST /v1/meters/{id}/probe` | 更换主机上的探头；更换后必须重新核对精确主机+探头证书 |
| `POST /v1/certificates` / `POST /v1/certificate-revisions` | 检定机构录入证书或更正通知 |
| `GET /v1/corrections/{id}/impact` | 查询更正影响的工单、当前是否需停用、已影响凭证和补核责任方 |
| `POST /v1/receipts` | 合同方用有实测边界的 sample/from/to 选择已出具凭证行 |
| `GET /v1/receipts/{id}?view=summary\|original\|readings\|corrected` | 分别回查影响分析、原件、当初读数、更正后确认 |
| `GET /v1/reviews` / `GET /v1/reviews/{id}` | 补核案件与默认分段结论 |
| `POST /v1/reviews/{id}/propose` | 协议一方提交补核结论；空 body 使用系统默认建议 |
| `POST /v1/reviews/{id}/confirm` | 另一方确认；双方齐备后生成 `CONF-...` 可引用确认 |
| `GET /v1/confirmations/{id}` | 回查双方确认后的可引用更正凭证 |
| `GET /v1/actors/{id}/tasks` | 查询某使用方待办理的补核任务 |

### 并发领用示例

```bash
curl -sS http://127.0.0.1:8000/v1/requests/W2/recommendation | python3 -m json.tool

curl -sS -X POST http://127.0.0.1:8000/v1/requests/W2/confirm \
  -H 'Content-Type: application/json' \
  -H 'X-Actor-Id: library' \
  -d '{"meter":"FM18"}'
```

两个重叠请求并发确认同一台仪器时，服务在同一把可重入锁内完成资格检查和分配写入：只有一个请求得到 `executable`/`confirmed`，其余得到 `409 conflict` 和冲突明细。

### 补核与权限示例

```bash
curl -sS -X POST \
  http://127.0.0.1:8000/v1/reviews/RV-V1-F17/propose \
  -H 'X-Actor-Id: plant-a' -H 'Content-Type: application/json' -d '{}'

curl -sS -X POST \
  http://127.0.0.1:8000/v1/reviews/RV-V1-F17/confirm \
  -H 'X-Actor-Id: buyer-b' -H 'Content-Type: application/json' -d '{}'

curl -sS \
  'http://127.0.0.1:8000/v1/receipts/V1?view=corrected' \
  -H 'X-Actor-Id: plant-a' | python3 -m json.tool
```

## 关键不变量

- 时间区间统一为带偏移 ISO 8601，按 `[start,end)` 判断。
- 只使用采样 `cuts` 中两个实测时点的读数差；不按时间均摊，也不用 `flow_range` 的最大/最小值推算比例。
- 证书按 `(meter, probe)` 精确匹配，型号相同不共享证书。
- 参考记录必须同时满足独立证书、相同介质、相同计量点、精确匹配的实测区间边界；同一区间最多选择一个来源计列。
- 原 receipt 不被覆盖；双方确认产生新的版本和确认编号。
- 返还/再分配只影响之后的可用性，不级联删除历史 receipt、confirmation、work order 或 allocation。
