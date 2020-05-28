CREATE VIEW [marketing].[v_dim_reporting_hierarchy_history] AS select dim_reporting_hierarchy_history.dim_reporting_hierarchy_key dim_reporting_hierarchy_key,
       dim_reporting_hierarchy_history.effective_dim_date_key effective_dim_date_key,
       dim_reporting_hierarchy_history.expiration_dim_date_key expiration_dim_date_key,
       dim_reporting_hierarchy_history.reporting_department reporting_department,
       dim_reporting_hierarchy_history.reporting_division reporting_division,
       dim_reporting_hierarchy_history.reporting_product_group reporting_product_group,
       dim_reporting_hierarchy_history.reporting_region_type reporting_region_type,
       dim_reporting_hierarchy_history.reporting_sub_division reporting_sub_division
  from dbo.dim_reporting_hierarchy_history;