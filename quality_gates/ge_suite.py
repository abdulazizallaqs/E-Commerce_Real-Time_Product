from typing import Any, Dict, List

import pandas as pd
import great_expectations as gx


def run_ge_suite(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Run a Great Expectations validation suite against a batch of product records."""
    if not records:
        return {"success": True, "statistics": {"evaluated_expectations": 0}}

    df = pd.DataFrame(records)
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("ecommerce_products")
    data_asset = data_source.add_dataframe_asset(name="products")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("full_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(gx.ExpectationSuite(name="product_quality_suite"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="name"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="stock_quantity", min_value=0))

    results = batch.validate(suite)
    return {
        "success": results.success,
        "statistics": results.statistics,
    }