from string import Template
from typing import List, Tuple

from sql_formatter.core import format_sql

"""This functions auto generates SQL statements for generating features in timeseries prediction"""
def ts_feature_sql(
    table_name: str,
    ts: str,
    target: str,
    by: List[str] = [],
    ar_range: Tuple[int, int] = (1, 10),
    schema: str = "public",
) -> str:
    table_name = f"{schema}.{table_name}"

    datepart_func = ["year", "month", "day", "dayofyear", "week", "quarter"]
    datepart_alias = ["y", "m", "dow", "doy", "w", "q"]

    columns = []
    columns += [
        f"{f}({ts}) as {alias}" for f, alias in zip(datepart_func, datepart_alias)
    ]
    ar_min, ar_max = ar_range

    if len(by) > 0:
        columns += [
            f"lag({target}, {l}) over(partition by {','.join(by)} order by {ts}) as lag_{l}"
            for l in range(ar_min, ar_max)
        ]
    else:
        columns += [
            f"lag({target}, {l}) over(order by {ts}) as lag_{l}"
            for l in range(ar_min, ar_max)
        ]

    if len(by) >0:
        columns.append(
            f"avg({target}) over (partition by {','.join(by)} order by {ts} range between interval '{ar_max} days' preceding and interval '{ar_min} days' preceding) mean{ar_min}_{ar_max}"
        )
        columns.append(
            f"min({target}) over (partition by {','.join(by)} order by {ts} range between interval '{ar_max} days' preceding and interval '{ar_min} days' preceding) min{ar_min}_{ar_max}"
        )
        columns.append(
            f"max({target}) over (partition by {','.join(by)} order by {ts} range between interval '{ar_max} days' preceding and interval '{ar_min} days' preceding) max{ar_min}_{ar_max}"
        )
    else:
        columns.append(
            f"avg({target}) over (order by {ts} range between interval '{ar_max} days' preceding and interval '{ar_min} days' preceding) mean{ar_min}_{ar_max}"
        )
        columns.append(
            f"min({target}) over (order by {ts} range between interval '{ar_max} days' preceding and interval '{ar_min} days' preceding) min{ar_min}_{ar_max}"
        )
        columns.append(
            f"max({target}) over (order by {ts} range between interval '{ar_max} days' preceding and interval '{ar_min} days' preceding) max{ar_min}_{ar_max}"
        )

    if len(by) > 0:
        sql = (
            f"select {','.join(by)}, {ts} as ts, {target} as y_true, {','.join(columns)} from {table_name}"
        )
    else:
        sql = (
            f"select {ts} as ts, {target} as y_true, {','.join(columns)} from {table_name}"
        )

    return format_sql(sql)
