-- Clone-Xs Execution Plan
-- Source: edp_dev -> Destination: edp_dev_00
-- Generated: 2026-03-15 22:00:36
-- Total statements: 203

-- [CREATE_SCHEMA] Statement 1
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`staging`;

-- [CREATE_SCHEMA] Statement 2
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`mcp_tools`;

-- [CREATE_SCHEMA] Statement 3
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`test_reports`;

-- [CREATE_SCHEMA] Statement 4
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`demo`;

-- [CREATE_SCHEMA] Statement 5
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`bronze`;

-- [CREATE_SCHEMA] Statement 6
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`dbu_analysis`;

-- [CREATE_SCHEMA] Statement 7
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`;

-- [CREATE_SCHEMA] Statement 8
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`assessment`;

-- [CREATE_SCHEMA] Statement 9
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`logs`;

-- [CREATE_SCHEMA] Statement 10
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`synthetic_data_schema`;

-- [CLONE] Statement 11
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`synthetic_data_schema`.`synthetic_sales_data` DEEP CLONE `edp_dev`.`synthetic_data_schema`.`synthetic_sales_data`;

-- [CLONE] Statement 12
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`staging`.`customers_replica` DEEP CLONE `edp_dev`.`staging`.`customers_replica`;

-- [CLONE] Statement 13
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`test_reports`.`junit_results` DEEP CLONE `edp_dev`.`test_reports`.`junit_results`;

-- [CLONE] Statement 14
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`assessment`.`assessment_runs` DEEP CLONE `edp_dev`.`assessment`.`assessment_runs`;

-- [CLONE] Statement 15
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`assessment`.`pillar_scores` DEEP CLONE `edp_dev`.`assessment`.`pillar_scores`;

-- [CLONE] Statement 16
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`assessment`.`scores_trend` DEEP CLONE `edp_dev`.`assessment`.`scores_trend`;

-- [CLONE] Statement 17
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`assessment`.`findings` DEEP CLONE `edp_dev`.`assessment`.`findings`;

-- [CLONE] Statement 18
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`logs`.`clone_operations` DEEP CLONE `edp_dev`.`logs`.`clone_operations`;

-- [CLONE] Statement 19
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`logs`.`run_logs` DEEP CLONE `edp_dev`.`logs`.`run_logs`;

-- [CLONE] Statement 20
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`demo`.`orders` DEEP CLONE `edp_dev`.`demo`.`orders`;

-- [CLONE] Statement 21
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`query_type_mix` DEEP CLONE `edp_dev`.`fin_ops_reports`.`query_type_mix`;

-- [CLONE] Statement 22
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`daily_trend` DEEP CLONE `edp_dev`.`fin_ops_reports`.`daily_trend`;

-- [CLONE] Statement 23
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`audit_events` DEEP CLONE `edp_dev`.`fin_ops_reports`.`audit_events`;

-- [CLONE] Statement 24
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`analysis_queries` DEEP CLONE `edp_dev`.`fin_ops_reports`.`analysis_queries`;

-- [CLONE] Statement 25
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`sku_utilization` DEEP CLONE `edp_dev`.`fin_ops_reports`.`sku_utilization`;

-- [CLONE] Statement 26
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`serverless_vs_classic` DEEP CLONE `edp_dev`.`fin_ops_reports`.`serverless_vs_classic`;

-- [CLONE] Statement 27
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`daily_cost_by_type` DEEP CLONE `edp_dev`.`fin_ops_reports`.`daily_cost_by_type`;

-- [CLONE] Statement 28
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`executed_queries` DEEP CLONE `edp_dev`.`fin_ops_reports`.`executed_queries`;

-- [CLONE] Statement 29
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`query_spill` DEEP CLONE `edp_dev`.`fin_ops_reports`.`query_spill`;

-- [CLONE] Statement 30
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`cost_anomaly` DEEP CLONE `edp_dev`.`fin_ops_reports`.`cost_anomaly`;

-- [CLONE] Statement 31
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`query_volume_trend` DEEP CLONE `edp_dev`.`fin_ops_reports`.`query_volume_trend`;

-- [CLONE] Statement 32
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`tag_cost_chargeback` DEEP CLONE `edp_dev`.`fin_ops_reports`.`tag_cost_chargeback`;

-- [CLONE] Statement 33
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`photon_utilization` DEEP CLONE `edp_dev`.`fin_ops_reports`.`photon_utilization`;

-- [CLONE] Statement 34
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`clusters` DEEP CLONE `edp_dev`.`fin_ops_reports`.`clusters`;

-- [CLONE] Statement 35
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`report_metadata` DEEP CLONE `edp_dev`.`fin_ops_reports`.`report_metadata`;

-- [CLONE] Statement 36
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`query_by_user` DEEP CLONE `edp_dev`.`fin_ops_reports`.`query_by_user`;

-- [CLONE] Statement 37
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`user_cost` DEEP CLONE `edp_dev`.`fin_ops_reports`.`user_cost`;

-- [CLONE] Statement 38
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`top_cost_drivers` DEEP CLONE `edp_dev`.`fin_ops_reports`.`top_cost_drivers`;

-- [CLONE] Statement 39
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`job_cost` DEEP CLONE `edp_dev`.`fin_ops_reports`.`job_cost`;

-- [CLONE] Statement 40
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`warehouse_query_stats` DEEP CLONE `edp_dev`.`fin_ops_reports`.`warehouse_query_stats`;

-- [CLONE] Statement 41
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`idle_clusters` DEEP CLONE `edp_dev`.`fin_ops_reports`.`idle_clusters`;

-- [CLONE] Statement 42
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`sku_user_consumer` DEEP CLONE `edp_dev`.`fin_ops_reports`.`sku_user_consumer`;

-- [CLONE] Statement 43
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`storage_optimization` DEEP CLONE `edp_dev`.`fin_ops_reports`.`storage_optimization`;

-- [CLONE] Statement 44
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`cluster_cost_attribution` DEEP CLONE `edp_dev`.`fin_ops_reports`.`cluster_cost_attribution`;

-- [CLONE] Statement 45
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`peak_hour_load` DEEP CLONE `edp_dev`.`fin_ops_reports`.`peak_hour_load`;

-- [CLONE] Statement 46
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`consumer_cost` DEEP CLONE `edp_dev`.`fin_ops_reports`.`consumer_cost`;

-- [CLONE] Statement 47
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`usage_type_breakdown` DEEP CLONE `edp_dev`.`fin_ops_reports`.`usage_type_breakdown`;

-- [CLONE] Statement 48
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`spend_heatmap` DEEP CLONE `edp_dev`.`fin_ops_reports`.`spend_heatmap`;

-- [CLONE] Statement 49
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`top_slow_queries` DEEP CLONE `edp_dev`.`fin_ops_reports`.`top_slow_queries`;

-- [CLONE] Statement 50
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`fin_ops_reports`.`failed_queries` DEEP CLONE `edp_dev`.`fin_ops_reports`.`failed_queries`;

-- [CLONE] Statement 51
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`date_dim` DEEP CLONE `edp_dev`.`bronze`.`date_dim`;

-- [CLONE] Statement 52
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`kafka_dummy_events` DEEP CLONE `edp_dev`.`bronze`.`kafka_dummy_events`;

-- [CLONE] Statement 53
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`region` DEEP CLONE `edp_dev`.`bronze`.`region`;

-- [CLONE] Statement 54
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`typicode_albums` DEEP CLONE `edp_dev`.`bronze`.`typicode_albums`;

-- [CLONE] Statement 55
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`typicode_users` DEEP CLONE `edp_dev`.`bronze`.`typicode_users`;

-- [CLONE] Statement 56
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`web_page` DEEP CLONE `edp_dev`.`bronze`.`web_page`;

-- [CLONE] Statement 57
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`sftp_invoice_pdf` DEEP CLONE `edp_dev`.`bronze`.`sftp_invoice_pdf`;

-- [CLONE] Statement 58
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`poc_payload` DEEP CLONE `edp_dev`.`bronze`.`poc_payload`;

-- [CLONE] Statement 59
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`call_center` DEEP CLONE `edp_dev`.`bronze`.`call_center`;

-- [CLONE] Statement 60
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customer_data` DEEP CLONE `edp_dev`.`bronze`.`customer_data`;

-- [CLONE] Statement 61
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`sales_invoices_documents` DEEP CLONE `edp_dev`.`bronze`.`sales_invoices_documents`;

-- [CLONE] Statement 62
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_connection` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_connection`;

-- [CLONE] Statement 63
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_catalog` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_catalog`;

-- [CLONE] Statement 64
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_group` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_group`;

-- [CLONE] Statement 65
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`gov_bus_tracking` DEEP CLONE `edp_dev`.`bronze`.`gov_bus_tracking`;

-- [CLONE] Statement 66
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`household_demographics` DEEP CLONE `edp_dev`.`bronze`.`household_demographics`;

-- [CLONE] Statement 67
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_dbfs_file` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_dbfs_file`;

-- [CLONE] Statement 68
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_sql_endpoint` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_sql_endpoint`;

-- [CLONE] Statement 69
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`primary_key_detection_results` DEEP CLONE `edp_dev`.`bronze`.`primary_key_detection_results`;

-- [CLONE] Statement 70
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`reason` DEEP CLONE `edp_dev`.`bronze`.`reason`;

-- [CLONE] Statement 71
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_secret_scope` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_secret_scope`;

-- [CLONE] Statement 72
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_table` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_table`;

-- [CLONE] Statement 73
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`typicode_photos` DEEP CLONE `edp_dev`.`bronze`.`typicode_photos`;

-- [CLONE] Statement 74
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customer_comparison_positional` DEEP CLONE `edp_dev`.`bronze`.`customer_comparison_positional`;

-- [CLONE] Statement 75
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`catalog_page` DEEP CLONE `edp_dev`.`bronze`.`catalog_page`;

-- [CLONE] Statement 76
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_workspace_file` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_workspace_file`;

-- [CLONE] Statement 77
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`nations` DEEP CLONE `edp_dev`.`bronze`.`nations`;

-- [CLONE] Statement 78
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`store` DEEP CLONE `edp_dev`.`bronze`.`store`;

-- [CLONE] Statement 79
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`azure_sql_poc_customers` DEEP CLONE `edp_dev`.`bronze`.`azure_sql_poc_customers`;

-- [CLONE] Statement 80
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`orders` DEEP CLONE `edp_dev`.`bronze`.`orders`;

-- [CLONE] Statement 81
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`part_supp` DEEP CLONE `edp_dev`.`bronze`.`part_supp`;

-- [CLONE] Statement 82
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`gemini_pricefeed` DEEP CLONE `edp_dev`.`bronze`.`gemini_pricefeed`;

-- [CLONE] Statement 83
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_dashboard` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_dashboard`;

-- [CLONE] Statement 84
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`web_site` DEEP CLONE `edp_dev`.`bronze`.`web_site`;

-- [CLONE] Statement 85
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`typicode_todos` DEEP CLONE `edp_dev`.`bronze`.`typicode_todos`;

-- [CLONE] Statement 86
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customers` DEEP CLONE `edp_dev`.`bronze`.`customers`;

-- [CLONE] Statement 87
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_cluster` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_cluster`;

-- [CLONE] Statement 88
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`warehouse` DEEP CLONE `edp_dev`.`bronze`.`warehouse`;

-- [CLONE] Statement 89
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customer` DEEP CLONE `edp_dev`.`bronze`.`customer`;

-- [CLONE] Statement 90
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`workspace_scan_summary` DEEP CLONE `edp_dev`.`bronze`.`workspace_scan_summary`;

-- [CLONE] Statement 91
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`gemini_free_promos` DEEP CLONE `edp_dev`.`bronze`.`gemini_free_promos`;

-- [CLONE] Statement 92
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`invoices` DEEP CLONE `edp_dev`.`bronze`.`invoices`;

-- [CLONE] Statement 93
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`promotion` DEEP CLONE `edp_dev`.`bronze`.`promotion`;

-- [CLONE] Statement 94
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`invoice_pdf` DEEP CLONE `edp_dev`.`bronze`.`invoice_pdf`;

-- [CLONE] Statement 95
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`item` DEEP CLONE `edp_dev`.`bronze`.`item`;

-- [CLONE] Statement 96
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`supplier` DEEP CLONE `edp_dev`.`bronze`.`supplier`;

-- [CLONE] Statement 97
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`bus_tracking` DEEP CLONE `edp_dev`.`bronze`.`bus_tracking`;

-- [CLONE] Statement 98
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customers_example` DEEP CLONE `edp_dev`.`bronze`.`customers_example`;

-- [CLONE] Statement 99
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customer_demographics` DEEP CLONE `edp_dev`.`bronze`.`customer_demographics`;

-- [CLONE] Statement 100
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`time_dim` DEEP CLONE `edp_dev`.`bronze`.`time_dim`;

-- [CLONE] Statement 101
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_storage_credential` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_storage_credential`;

-- [CLONE] Statement 102
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`typicode_posts` DEEP CLONE `edp_dev`.`bronze`.`typicode_posts`;

-- [CLONE] Statement 103
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_schema` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_schema`;

-- [CLONE] Statement 104
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customer_source` DEEP CLONE `edp_dev`.`bronze`.`customer_source`;

-- [CLONE] Statement 105
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`income_band` DEEP CLONE `edp_dev`.`bronze`.`income_band`;

-- [CLONE] Statement 106
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`part` DEEP CLONE `edp_dev`.`bronze`.`part`;

-- [CLONE] Statement 107
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`ship_mode` DEEP CLONE `edp_dev`.`bronze`.`ship_mode`;

-- [CLONE] Statement 108
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`aircraft_positions` DEEP CLONE `edp_dev`.`bronze`.`aircraft_positions`;

-- [CLONE] Statement 109
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customer_comparison_all_columns` DEEP CLONE `edp_dev`.`bronze`.`customer_comparison_all_columns`;

-- [CLONE] Statement 110
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_workspace_conf` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_workspace_conf`;

-- [CLONE] Statement 111
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`lineitems` DEEP CLONE `edp_dev`.`bronze`.`lineitems`;

-- [CLONE] Statement 112
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customer_comparison_log` DEEP CLONE `edp_dev`.`bronze`.`customer_comparison_log`;

-- [CLONE] Statement 113
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customer_address` DEEP CLONE `edp_dev`.`bronze`.`customer_address`;

-- [CLONE] Statement 114
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`customer_target` DEEP CLONE `edp_dev`.`bronze`.`customer_target`;

-- [CLONE] Statement 115
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_job` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_job`;

-- [CLONE] Statement 116
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_global_init_script` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_global_init_script`;

-- [CLONE] Statement 117
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_external_location` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_external_location`;

-- [CLONE] Statement 118
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`typicode_comments` DEEP CLONE `edp_dev`.`bronze`.`typicode_comments`;

-- [CLONE] Statement 119
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_cluster_policy` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_cluster_policy`;

-- [CLONE] Statement 120
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`bronze`.`raw_databricks_pipeline` DEEP CLONE `edp_dev`.`bronze`.`raw_databricks_pipeline`;

-- [CLONE] Statement 121
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_recommendations` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_recommendations`;

-- [CLONE] Statement 122
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_data_quality` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_data_quality`;

-- [CLONE] Statement 123
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_governance` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_governance`;

-- [CLONE] Statement 124
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_cluster_utilization` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_cluster_utilization`;

-- [CLONE] Statement 125
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_summary_metrics` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_summary_metrics`;

-- [CLONE] Statement 126
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_monitoring` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_monitoring`;

-- [CLONE] Statement 127
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`metrics_history` DEEP CLONE `edp_dev`.`dbu_analysis`.`metrics_history`;

-- [CLONE] Statement 128
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_job_analysis` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_job_analysis`;

-- [CLONE] Statement 129
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`analysis_results` DEEP CLONE `edp_dev`.`dbu_analysis`.`analysis_results`;

-- [CLONE] Statement 130
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_capacity` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_capacity`;

-- [CLONE] Statement 131
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_cost_trend` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_cost_trend`;

-- [CLONE] Statement 132
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_security` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_security`;

-- [CLONE] Statement 133
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`ai_forecast_cache` DEEP CLONE `edp_dev`.`dbu_analysis`.`ai_forecast_cache`;

-- [CLONE] Statement 134
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_cost_analysis` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_cost_analysis`;

-- [CLONE] Statement 135
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_delta_health` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_delta_health`;

-- [CLONE] Statement 136
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_finops` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_finops`;

-- [CLONE] Statement 137
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_storage_analysis` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_storage_analysis`;

-- [CLONE] Statement 138
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dbu_analysis`.`dashboard_cache` DEEP CLONE `edp_dev`.`dbu_analysis`.`dashboard_cache`;

-- [CREATE_VIEW] Statement 139
CREATE OR REPLACE VIEW `edp_dev_00`.`test_reports`.`junit_duration_by_run` AS SELECT run_id,
       AVG(time_sec) AS avg_duration_sec,
       PERCENTILE(time_sec, 0.95) AS p95_duration_sec
FROM test_reports.junit_results
GROUP BY run_id;

-- [CREATE_VIEW] Statement 140
CREATE OR REPLACE VIEW `edp_dev_00`.`test_reports`.`junit_flakiness` AS WITH states AS (
  SELECT classname, test_name,
         COLLECT_SET(status) AS states
  FROM test_reports.junit_results
  GROUP BY classname, test_name
)
SELECT * FROM states WHERE array_contains(states, 'passed') AND array_contains(states, 'failed');

-- [CREATE_VIEW] Statement 141
CREATE OR REPLACE VIEW `edp_dev_00`.`test_reports`.`junit_summary_by_run` AS SELECT
  run_id,
  COUNT(1)                            AS total,
  SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) AS passed,
  SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
  SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped,
  ROUND(100.0 * SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) / NULLIF(COUNT(1),0), 2) AS pass_rate
FROM test_reports.junit_results
GROUP BY run_id;

-- [CREATE_VOLUME] Statement 142
CREATE VOLUME IF NOT EXISTS `edp_dev_00`.`test_reports`.`test_artifacts`;

-- [CREATE_SCHEMA] Statement 143
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`monitoring`;

-- [CREATE_SCHEMA] Statement 144
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`production`;

-- [CREATE_SCHEMA] Statement 145
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`sat_scan_report`;

-- [CREATE_VOLUME] Statement 146
CREATE EXTERNAL VOLUME IF NOT EXISTS `edp_dev_00`.`bronze`.`non-pii` LOCATION 'abfss://non-pii@stdpdvuks01.dfs.core.windows.net/';

-- [CREATE_VOLUME] Statement 147
CREATE EXTERNAL VOLUME IF NOT EXISTS `edp_dev_00`.`bronze`.`checkpoint` LOCATION 'abfss://checkpoint@stdpdvuks01.dfs.core.windows.net/';

-- [CREATE_VOLUME] Statement 148
CREATE EXTERNAL VOLUME IF NOT EXISTS `edp_dev_00`.`bronze`.`schema` LOCATION 'abfss://schema@stdpdvuks01.dfs.core.windows.net/';

-- [CREATE_VOLUME] Statement 149
CREATE VOLUME IF NOT EXISTS `edp_dev_00`.`bronze`.`13f9afb9-f8ab-4c56-9ce7-c056552f0666_checkpoints`;

-- [CREATE_VOLUME] Statement 150
CREATE EXTERNAL VOLUME IF NOT EXISTS `edp_dev_00`.`bronze`.`dqx` LOCATION 'abfss://dqx@stdpdvuks01.dfs.core.windows.net/';

-- [CREATE_VOLUME] Statement 151
CREATE EXTERNAL VOLUME IF NOT EXISTS `edp_dev_00`.`bronze`.`configs` LOCATION 'abfss://configs@stdpdvuks01.dfs.core.windows.net/';

-- [CREATE_VOLUME] Statement 152
CREATE EXTERNAL VOLUME IF NOT EXISTS `edp_dev_00`.`bronze`.`pii` LOCATION 'abfss://pii@stdpdvuks01.dfs.core.windows.net/';

-- [CREATE_SCHEMA] Statement 153
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`logging`;

-- [CREATE_SCHEMA] Statement 154
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`gold`;

-- [CLONE] Statement 155
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`monitoring`.`delta_maintenance_runs` DEEP CLONE `edp_dev`.`monitoring`.`delta_maintenance_runs`;

-- [CREATE_SCHEMA] Statement 156
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`target_system`;

-- [CLONE] Statement 157
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`logging`.`logging_01` DEEP CLONE `edp_dev`.`logging`.`logging_01`;

-- [CLONE] Statement 158
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`logging`.`assessment` DEEP CLONE `edp_dev`.`logging`.`assessment`;

-- [CLONE] Statement 159
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`logging`.`ingestion_events` DEEP CLONE `edp_dev`.`logging`.`ingestion_events`;

-- [CREATE_VOLUME] Statement 160
CREATE VOLUME IF NOT EXISTS `edp_dev_00`.`staging`.`__databricks_ingestion_gateway_staging_data-2e7a1ba0-3036-4960-954c-cd12e874a0f2` COMMENT 'Staging volume for ingestion gateway pipeline 2e7a1ba0-3036-4960-954c-cd12e874a0f2. Please do not delete or change its name.';

-- [CLONE] Statement 161
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`target_system`.`orders` DEEP CLONE `edp_dev`.`target_system`.`orders`;

-- [CREATE_VIEW] Statement 162
CREATE OR REPLACE VIEW `edp_dev_00`.`gold`.`job_cost_analysis` AS WITH job_timeline AS (
    SELECT 
        workspace_id,
        job_id,
        run_id,
        run_name,
        trigger_type,
        run_type,
        COALESCE(result_state, 'SUCCEEDED') AS result_state,
        period_start_time,
        period_end_time,
        TRY_ELEMENT_AT(compute_ids, 1) AS cluster_id,
        ROUND(
            (UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) / 60, 
            2
        ) AS duration_minutes
    FROM system.lakeflow.job_run_timeline
),

job_billing AS (
    SELECT 
        usage_metadata.job_id AS job_id,
        workspace_id,
        usage_metadata.cluster_id AS cluster_id,
        sku_name,
        usage_date,
        SUM(usage_quantity) AS usage_quantity,
        identity_metadata.run_as AS executed_by
    FROM system.billing.usage
    WHERE usage_metadata.job_id IS NOT NULL
    GROUP BY 
        usage_metadata.job_id,
        workspace_id,
        usage_metadata.cluster_id,
        sku_name,
        usage_date,
        identity_metadata.run_as
)

SELECT 
    jt.workspace_id,
    CASE 
        WHEN jt.workspace_id = 'YOUR_DEV_WORKSPACE_ID' THEN 'Development'
        WHEN jt.workspace_id = 'YOUR_TEST_WORKSPACE_ID' THEN 'Test'
        WHEN jt.workspace_id = 'YOUR_PROD_WORKSPACE_ID' THEN 'Production'
        ELSE 'Unknown'
    END AS environment,
    jt.job_id,
    jt.run_id,
    jt.run_name,
    jt.trigger_type,
    jt.run_type,
    jt.result_state,
    DATE(jt.period_start_time) AS execution_date,
    HOUR(jt.period_start_time) AS execution_hour,
    jt.period_start_time,
    jt.period_end_time,
    jt.duration_minutes,
    CASE 
        WHEN jt.duration_minutes <= 5 THEN 'X-Short (0-5 min)'
        WHEN jt.duration_minutes <= 15 THEN 'Short (5-15 min)'
        WHEN jt.duration_minutes <= 60 THEN 'Medium (15-60 min)'
        WHEN jt.duration_minutes <= 180 THEN 'Long (1-3 hrs)'
        ELSE 'X-Long (3+ hrs)'
    END AS duration_category,
    jt.cluster_id,
    c.cluster_name,
    c.cluster_source AS compute_type,
    jb.sku_name,
    jb.usage_quantity AS dbus_used,
    lp.pricing.default AS dbu_price,
    ROUND(jb.usage_quantity * lp.pricing.default, 2) AS job_cost,
    jb.executed_by,
    CONCAT(
        'https://adb-', jt.workspace_id, '.azuredatabricks.net/#job/',
        jt.job_id, '/run/', jt.run_id
    ) AS job_url
FROM job_timeline jt
LEFT JOIN job_billing jb 
    ON jt.job_id = jb.job_id 
    AND jt.workspace_id = jb.workspace_id
    AND jt.cluster_id = jb.cluster_id
    AND DATE(jt.period_start_time) = jb.usage_date
LEFT JOIN system.compute.clusters c
    ON jt.cluster_id = c.cluster_id
    AND jt.workspace_id = c.workspace_id
LEFT JOIN system.billing.list_prices lp 
    ON jb.sku_name = lp.sku_name 
    AND lp.price_end_time IS NULL
WHERE jt.period_start_time >= CURRENT_DATE() - INTERVAL 60 DAY;

-- [CREATE_SCHEMA] Statement 163
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`source_system`;

-- [CREATE_SCHEMA] Statement 164
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`test`;

-- [CREATE_SCHEMA] Statement 165
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`metrics`;

-- [CLONE] Statement 166
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`sat_scan_report`.`api_endpoints` DEEP CLONE `edp_dev`.`sat_scan_report`.`api_endpoints`;

-- [CLONE] Statement 167
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`sat_scan_report`.`findings` DEEP CLONE `edp_dev`.`sat_scan_report`.`findings`;

-- [CLONE] Statement 168
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`sat_scan_report`.`scan_runs` DEEP CLONE `edp_dev`.`sat_scan_report`.`scan_runs`;

-- [CLONE] Statement 169
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`sat_scan_report`.`reports` DEEP CLONE `edp_dev`.`sat_scan_report`.`reports`;

-- [CLONE] Statement 170
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`sat_scan_report`.`category_scores` DEEP CLONE `edp_dev`.`sat_scan_report`.`category_scores`;

-- [CLONE] Statement 171
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`sat_scan_report`.`scan_changes` DEEP CLONE `edp_dev`.`sat_scan_report`.`scan_changes`;

-- [CLONE] Statement 172
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`sat_scan_report`.`all_checks` DEEP CLONE `edp_dev`.`sat_scan_report`.`all_checks`;

-- [CLONE] Statement 173
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`sat_scan_report`.`prioritised_recommendations` DEEP CLONE `edp_dev`.`sat_scan_report`.`prioritised_recommendations`;

-- [CLONE] Statement 174
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`source_system`.`orders` DEEP CLONE `edp_dev`.`source_system`.`orders`;

-- [CLONE] Statement 175
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`production`.`customers` DEEP CLONE `edp_dev`.`production`.`customers`;

-- [CREATE_SCHEMA] Statement 176
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`audit`;

-- [CREATE_SCHEMA] Statement 177
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`dqx_rules`;

-- [CLONE] Statement 178
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`metrics`.`clone_metrics` DEEP CLONE `edp_dev`.`metrics`.`clone_metrics`;

-- [CREATE_SCHEMA] Statement 179
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`finance`;

-- [CLONE] Statement 180
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`dqx_rules`.`dqx_quality_checks_configs` DEEP CLONE `edp_dev`.`dqx_rules`.`dqx_quality_checks_configs`;

-- [CLONE] Statement 181
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`test`.`dataset_v2` DEEP CLONE `edp_dev`.`test`.`dataset_v2`;

-- [CLONE] Statement 182
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`test`.`dataset_v1` DEEP CLONE `edp_dev`.`test`.`dataset_v1`;

-- [CLONE] Statement 183
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`finance`.`transactions_source` DEEP CLONE `edp_dev`.`finance`.`transactions_source`;

-- [CLONE] Statement 184
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`finance`.`transactions_target` DEEP CLONE `edp_dev`.`finance`.`transactions_target`;

-- [CREATE_SCHEMA] Statement 185
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`demo_schema`;

-- [CREATE_SCHEMA] Statement 186
CREATE SCHEMA IF NOT EXISTS `edp_dev_00`.`silver`;

-- [CLONE] Statement 187
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`typicode_photos_clean` DEEP CLONE `edp_dev`.`silver`.`typicode_photos_clean`;

-- [CLONE] Statement 188
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`sales_invoices_documents_clean` DEEP CLONE `edp_dev`.`silver`.`sales_invoices_documents_clean`;

-- [CLONE] Statement 189
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_window_functions_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_window_functions_typicode_photos`;

-- [CLONE] Statement 190
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_date_operations_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_date_operations_typicode_photos`;

-- [CLONE] Statement 191
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`typicode_photos` DEEP CLONE `edp_dev`.`silver`.`typicode_photos`;

-- [CLONE] Statement 192
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_numeric_operations_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_numeric_operations_typicode_photos`;

-- [CLONE] Statement 193
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_null_handling_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_null_handling_typicode_photos`;

-- [CLONE] Statement 194
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_array_operations_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_array_operations_typicode_photos`;

-- [CLONE] Statement 195
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_data_quality_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_data_quality_typicode_photos`;

-- [CLONE] Statement 196
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_conditional_logic_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_conditional_logic_typicode_photos`;

-- [CLONE] Statement 197
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`customer_data` DEEP CLONE `edp_dev`.`silver`.`customer_data`;

-- [CLONE] Statement 198
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_type_casting_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_type_casting_typicode_photos`;

-- [CLONE] Statement 199
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_string_operations_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_string_operations_typicode_photos`;

-- [CLONE] Statement 200
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_deduplication_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_deduplication_typicode_photos`;

-- [CLONE] Statement 201
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_basic_columns_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_basic_columns_typicode_photos`;

-- [CLONE] Statement 202
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`example_depends_on_typicode_photos` DEEP CLONE `edp_dev`.`silver`.`example_depends_on_typicode_photos`;

-- [CLONE] Statement 203
CREATE TABLE IF NOT EXISTS `edp_dev_00`.`silver`.`kafka_dummy_events_silver` DEEP CLONE `edp_dev`.`silver`.`kafka_dummy_events_silver`;

