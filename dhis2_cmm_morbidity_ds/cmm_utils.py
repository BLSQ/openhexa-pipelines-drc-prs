import polars as pl
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_organisation_unit_groups


def get_formulas_for_extract(extract_uid: str, cmm_extracts: list) -> list:
    """Returns the list of rules for the matching extract UID.

    Args:
        extract_uid: The UID of the extract.
        cmm_extracts: The list of all cmm extract formulas.

    Returns:
        A list of rules corresponding to the given extract UID.
    """
    for rule in cmm_extracts:
        if rule.get("EXTRACT_UID") == extract_uid:
            return rule.get("FORMULAS", [])
    return []


def get_fosa_descendants_of_zs(pyramid: pl.DataFrame, dhis2_client: DHIS2, oug_id: str) -> list:
    """Retrieves the list of FOSA organisation units that are descendants of urban Zones de sante.

    Args:
        pyramid: The organisation units pyramid as a Polars DataFrame.
        dhis2_client: The DHIS2 client instance.
        oug_id: The organisation unit group ID for urban Zones de sante.

    Returns:
        List of level 5 organisation unit IDs that are descendants of urban Zones de sante.
    """
    ou_groups = get_organisation_unit_groups(dhis2_client)
    zs_urban = ou_groups.filter(pl.col("id") == oug_id)
    zs_urban_list = zs_urban["organisation_units"].explode().to_list()
    parent_map = dict(
        zip(
            pyramid["id"],
            pyramid["parent"].map_elements(lambda x: x["id"] if isinstance(x, dict) else None, return_dtype=pl.Utf8),
            strict=True,
        )
    )

    level5 = pyramid.filter(pl.col("level") == 5)["id"]

    def get_zs_parent(ou: str) -> str | None:
        """Climb 5 → 4 → 3.

        Returns:
          level 3 parent of level 5 org unit.
        """
        p4 = parent_map.get(ou)
        if not p4:
            return None
        return parent_map.get(p4)

    return [ou for ou in level5 if get_zs_parent(ou) in zs_urban_list]


def apply_formulas_to_extract(
    data: pl.DataFrame,
    formulas: list,
    ou_urban: list,
) -> pl.DataFrame:
    """Applies the given rules to the extract data and computes the results.

    Args:
        data: The extract data as a Polars DataFrame.
        formulas: The list of rules to apply.
        ou_urban: A list of org units which are considered Urban.

    Returns:
        The resulting DataFrame after applying the rules.
    """
    results = []
    for indicator, formula in formulas.items():
        expr = build_expr(formula, ou_urban=ou_urban)

        df = (
            data.group_by(["period", "org_unit"])
            .agg(expr.sum().alias("value"))
            .with_columns(pl.lit(indicator).alias("indicator"))
        )

        results.append(df)

    return pl.concat(results)


def build_expr(node: dict, ou_urban: list) -> pl.Expr:
    """Recursively builds a Polars expression from a formula node.

    Args:
        node: A dictionary representing a formula node, which can be a data element, sum, multiply, or constant.
        ou_urban: A list of org units which are considered Urban.

    Returns:
        A Polars expression representing the computation defined by the node.
    """
    if ou_urban is None:
        ou_urban = []

    # Leaf: data element
    if "dataElement" in node:
        return (
            pl.when(
                (pl.col("dx") == node["dataElement"]) & (pl.col("category_option_combo") == node["categoryOptionCombo"])
            )
            .then(pl.col("value"))
            .otherwise(0)
        )

    node_type = node["type"]

    if node_type == "sum":
        return sum(build_expr(item, ou_urban) for item in node["items"])

    if node_type == "multiply":
        return build_expr(node["left"], ou_urban) * build_expr(node["right"], ou_urban)

    if node_type == "constant":
        return pl.lit(node["value"])

    if node_type == "if":
        cond = build_condition(node["condition"], ou_check=ou_urban)
        return pl.when(cond).then(build_expr(node["then"], ou_urban)).otherwise(build_expr(node["else"], ou_urban))

    raise NotImplementedError(f"Unsupported node type: {node_type}")


def build_condition(cond: dict, ou_check: list) -> pl.Expr | None:
    """Build a Polars expression for a given condition.

    Args:
        cond: The condition dictionary.
        ou_check: A list of org units.

    Returns:
        The Polars expression representing the condition, or None if not applicable.
    """
    if cond["type"] == "orgUnitInGroupDescendant":
        return pl.col("org_unit").is_in(ou_check)
    return None


def compute_mean_and_format_results(period_results: pl.DataFrame, period: str) -> pl.DataFrame:
    """Computes the mean of indicator values for each organisation unit and formats the results for output.

    Args:
        period_results: DataFrame containing per-period indicator values to aggregate.
        period: The period string to stamp on the formatted output rows.

    Returns:
        Formatted DataFrame with mean values and required columns for output.
    """
    return (
        period_results.group_by(["org_unit", "indicator"])
        .agg(pl.col("value").mean().alias("value"))
        .with_columns(
            [
                pl.lit("cmm_indicator").alias("data_type"),
                pl.lit(period).alias("period"),
                pl.lit(None).alias("dx"),
                pl.lit(None).alias("category_option_combo"),
                pl.lit(None).alias("attribute_option_combo"),
                pl.col("indicator").str.to_uppercase().alias("indicator"),
            ]
        )
        .select(
            [
                "data_type",
                "dx",
                "period",
                "category_option_combo",
                "attribute_option_combo",
                "org_unit",
                "value",
                "indicator",
            ]
        )
    )
