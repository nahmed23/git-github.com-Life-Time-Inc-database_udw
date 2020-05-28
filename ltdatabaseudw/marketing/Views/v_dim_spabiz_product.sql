﻿CREATE VIEW [marketing].[v_dim_spabiz_product] AS select dim_spabiz_product.dim_spabiz_product_key dim_spabiz_product_key,
       dim_spabiz_product.product_id product_id,
       dim_spabiz_product.store_number store_number,
       dim_spabiz_product.avg_cost avg_cost,
       dim_spabiz_product.back_bar back_bar,
       dim_spabiz_product.category category,
       dim_spabiz_product.commission_mapping commission_mapping,
       dim_spabiz_product.cost cost,
       dim_spabiz_product.cost2 cost2,
       dim_spabiz_product.cost2_quantity cost2_quantity,
       dim_spabiz_product.created_date_time created_date_time,
       dim_spabiz_product.current_quantity current_quantity,
       dim_spabiz_product.deleted_date_time deleted_date_time,
       dim_spabiz_product.deleted_flag deleted_flag,
       dim_spabiz_product.dim_spabiz_category_key dim_spabiz_category_key,
       dim_spabiz_product.dim_spabiz_manufacturer_key dim_spabiz_manufacturer_key,
       dim_spabiz_product.dim_spabiz_staff_key dim_spabiz_staff_key,
       dim_spabiz_product.dim_spabiz_store_key dim_spabiz_store_key,
       dim_spabiz_product.dim_spabiz_sub_category_key dim_spabiz_sub_category_key,
       dim_spabiz_product.dim_spabiz_vendor_key dim_spabiz_vendor_key,
       dim_spabiz_product.economic_order_quantity economic_order_quantity,
       dim_spabiz_product.edit_date_time edit_date_time,
       dim_spabiz_product.gl_account gl_account,
       dim_spabiz_product.label_name label_name,
       dim_spabiz_product.last_count_date_time last_count_date_time,
       dim_spabiz_product.last_purchased_date_time last_purchased_date_time,
       dim_spabiz_product.last_sold_date_time last_sold_date_time,
       dim_spabiz_product.location location,
       dim_spabiz_product.manufacturer_code manufacturer_code,
       dim_spabiz_product.maximum_inventory_count maximum_inventory_count,
       dim_spabiz_product.minimum_inventory_count minimum_inventory_count,
       dim_spabiz_product.on_order on_order,
       dim_spabiz_product.p_spabiz_product_id p_spabiz_product_id,
       dim_spabiz_product.print_label_flag print_label_flag,
       dim_spabiz_product.print_on_ticket print_on_ticket,
       dim_spabiz_product.product_name product_name,
       dim_spabiz_product.product_type_dim_description_key product_type_dim_description_key,
       dim_spabiz_product.product_type_id product_type_id,
       dim_spabiz_product.quick_id quick_id,
       dim_spabiz_product.retail_price retail_price,
       dim_spabiz_product.segment segment,
       dim_spabiz_product.taxable_flag taxable_flag,
       dim_spabiz_product.vendor_code vendor_code
  from dbo.dim_spabiz_product;