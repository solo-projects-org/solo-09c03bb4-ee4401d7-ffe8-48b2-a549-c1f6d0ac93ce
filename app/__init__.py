"""裂纹事件流契约排程与日报发布 API。

三方角色：
- producer  采集端（发布端，滚动升级时 old/mixed/new 三态）
- qc        质检订阅端
- report    日报订阅端

另含裂纹事件整批补传与按巡测日发布可复核日报的能力。
"""

__all__ = [
    "models",
    "compat",
    "planner",
    "report_models",
    "db",
    "report_api",
    "main",
]
