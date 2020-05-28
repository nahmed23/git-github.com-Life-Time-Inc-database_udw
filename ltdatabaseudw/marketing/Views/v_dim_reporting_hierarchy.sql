CREATE VIEW [marketing].[v_dim_reporting_hierarchy]
AS select d_udwcloudsync_product_master.dim_reporting_hierarchy_key dim_reporting_hierarchy_key,
       d_udwcloudsync_product_master.reporting_department reporting_department,
       d_udwcloudsync_product_master.reporting_division reporting_division,
       d_udwcloudsync_product_master.reporting_product_group reporting_product_group,
       d_udwcloudsync_product_master.reporting_region_type reporting_region_type,
       d_udwcloudsync_product_master.reporting_sub_division reporting_sub_division
from d_udwcloudsync_product_master group by dim_reporting_hierarchy_key, reporting_division, reporting_sub_division, reporting_department, reporting_product_group, reporting_region_type;