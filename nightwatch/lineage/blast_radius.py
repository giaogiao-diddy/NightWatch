from typing import Any


MOCK_UNIFIED_CATALOG: dict[str, dict[str, Any]] = {
    "ods.user_profile": {
        "upstream_owner": "team.profile_platform",
        "affected_paimon_tables": [
            "dwd_user_profile",
            "dws_user_growth_daily",
        ],
        "affected_starrocks_dashboards": [
            {"name": "Growth KPI Overview", "owner": "bi.growth_oncall"},
            {"name": "Retention Funnel", "owner": "bi.crm_owner"},
        ],
        "dependent_dags": [
            "dag_dwd_user_profile_sync",
            "dag_dws_user_growth_daily",
        ],
        "cdc_jobs": ["flink_cdc_user_profile"],
    },
    "ods.orders": {
        "upstream_owner": "team.ordering_platform",
        "affected_paimon_tables": [
            "dwd_orders",
            "dws_order_revenue_hourly",
        ],
        "affected_starrocks_dashboards": [
            {"name": "Order Revenue Realtime", "owner": "bi.finance_oncall"},
            {"name": "GMV Cockpit", "owner": "bi.exec_dashboard"},
        ],
        "dependent_dags": [
            "dag_dwd_orders_sync",
            "dag_dws_order_revenue_hourly",
        ],
        "cdc_jobs": ["flink_cdc_orders"],
    },
}


def analyze_blast_radius(drifted_table_name: str) -> dict[str, Any]:
    normalized_table = drifted_table_name.lower().strip()
    catalog_entry = MOCK_UNIFIED_CATALOG.get(normalized_table, None)

    if catalog_entry is None:
        return {
            "drifted_table": normalized_table,
            "upstream_owner": "team.unknown_owner",
            "affected_paimon_tables": [],
            "affected_starrocks_dashboards": [],
            "affected_dashboard_owners": [],
            "dependent_dags": [],
            "cdc_jobs": [],
            "summary": "No catalog hit found; owner escalation required",
        }

    dashboards = catalog_entry["affected_starrocks_dashboards"]
    dashboard_owners = sorted({item["owner"] for item in dashboards})

    return {
        "drifted_table": normalized_table,
        "upstream_owner": catalog_entry["upstream_owner"],
        "affected_paimon_tables": catalog_entry["affected_paimon_tables"],
        "affected_starrocks_dashboards": dashboards,
        "affected_dashboard_owners": dashboard_owners,
        "dependent_dags": catalog_entry["dependent_dags"],
        "cdc_jobs": catalog_entry["cdc_jobs"],
        "summary": (
            f"{normalized_table} drift impacts {len(catalog_entry['affected_paimon_tables'])} "
            f"Paimon tables and {len(dashboards)} StarRocks dashboards"
        ),
    }
