"""Get physical-plan text out of a PySpark DataFrame.

Works with classic PySpark (py4j) and with Spark Connect, because both
implement ``DataFrame.explain`` by *printing from Python*.
"""

from __future__ import annotations

import contextlib
import io
import warnings
from typing import Any

__all__ = ["explain_text", "is_dataframe"]


def is_dataframe(obj: Any) -> bool:
    return hasattr(obj, "explain") and hasattr(obj, "schema") and callable(obj.explain)


def explain_text(df: Any, *, executed: bool = False, mode: str = "simple") -> str:
    """Return the physical plan of ``df`` as text.

    :param executed: execute the query first so that an adaptive (AQE) plan
        reports ``isFinalPlan=true`` and shows the joins and partitioning AQE
        actually chose at runtime. Rows are counted on the executors, never
        collected to the driver. Classic PySpark only; on Spark Connect the
        static plan is returned with a warning.
    :param mode: explain mode passed to ``df.explain``.
    """
    if not is_dataframe(df):
        raise TypeError(f"Expected a pyspark DataFrame, explain text or planlock.Plan, got {type(df).__name__}")
    if executed:
        jdf = getattr(df, "_jdf", None)
        if jdf is not None:
            # QueryExecution.toRdd executes *this* DataFrame's own physical plan,
            # so its AdaptiveSparkPlanExec is finalised in place.
            jdf.queryExecution().toRdd().count()
        else:
            warnings.warn(
                "planlock: executed=True is not supported on Spark Connect; "
                "capturing the static (pre-AQE) plan instead.",
                stacklevel=3,
            )

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()
