﻿CREATE VIEW [marketing].[v_dim_boss_product] AS select dim_boss_product.category_id category_id,
       dim_boss_product.color color,
       dim_boss_product.department_code department_code,
       dim_boss_product.department_description department_description,
       dim_boss_product.dim_boss_product_key dim_boss_product_key,
       dim_boss_product.dim_mms_product_key dim_mms_product_key,
       dim_boss_product.display_flag display_flag,
       dim_boss_product.product_description product_description,
       dim_boss_product.product_format_id product_format_id,
       dim_boss_product.product_format_long_desc product_format_long_desc,
       dim_boss_product.product_format_short_desc product_format_short_desc,
       dim_boss_product.product_hierarchy_level_1 product_hierarchy_level_1,
       dim_boss_product.product_hierarchy_level_2 product_hierarchy_level_2,
       dim_boss_product.product_hierarchy_level_3 product_hierarchy_level_3,
       dim_boss_product.product_interest_id product_interest_id,
       dim_boss_product.product_interest_long_desc product_interest_long_desc,
       dim_boss_product.product_interest_short_desc product_interest_short_desc,
       dim_boss_product.product_line product_line,
       dim_boss_product.product_updated_timestamp product_updated_timestamp,
       dim_boss_product.size size,
       dim_boss_product.sku sku,
       dim_boss_product.style style,
       dim_boss_product.upc_code upc_code
  from dbo.dim_boss_product;