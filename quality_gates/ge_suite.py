from typing import Any, Dict, List

import pandas as pd
import great_expectations as gx


def run_ge_suite(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Run a Great Expectations validation suite against a batch of product records."""
    if not records:
        return {"success": True, "statistics": {"evaluated_expectations": 0}}

    df = pd.DataFrame(records)
    context = gx.get_context(mode="ephemeral")
    validator = context.sources.pandas_default.read_dataframe(df)

    validator.expect_column_values_to_not_be_null("product_id")
    validator.expect_column_values_to_not_be_null("name")
    validator.expect_column_values_to_be_between("price", min_value=0)
    validator.expect_column_values_to_be_between("stock_quantity", min_value=0)

    result = validator.validate()
    return {
        "success": result.success,
        "statistics": result.statistics,
    }