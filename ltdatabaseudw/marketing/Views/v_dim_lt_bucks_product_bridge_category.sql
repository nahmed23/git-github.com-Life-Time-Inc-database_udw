CREATE VIEW [marketing].[v_dim_lt_bucks_product_bridge_category] AS select p_lt_bucks_products.bk_hash dim_lt_bucks_product_key,
       case when s_lt_bucks_categories.category_active = 1 then 'Y' else 'N' end category_active_flag,
       case when s_lt_bucks_categories.category_isdeleted = 1 then 'Y' else 'N' end category_deleted_flag,
       b_lt_bucks_product_category.category_id category_id,
       isnull(s_lt_bucks_categories.category_name,'') category_name,
       b_lt_bucks_product_category.product_id product_id
  from dbo.b_lt_bucks_product_category
  join dbo.h_lt_bucks_category_items
    on b_lt_bucks_product_category.citem_id = h_lt_bucks_category_items.citem_id
  join dbo.p_lt_bucks_categories
    on b_lt_bucks_product_category.category_id = p_lt_bucks_categories.category_id
   and p_lt_bucks_categories.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
  join dbo.s_lt_bucks_categories
    on p_lt_bucks_categories.s_lt_bucks_categories_id = s_lt_bucks_categories.s_lt_bucks_categories_id
  join p_lt_bucks_products
    on b_lt_bucks_product_category.product_id = p_lt_bucks_products.product_id
   and p_lt_bucks_products.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
 where b_lt_bucks_product_category.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
   and h_lt_bucks_category_items.dv_deleted = 0
 group by p_lt_bucks_products.bk_hash,
          b_lt_bucks_product_category.product_id,
          b_lt_bucks_product_category.category_id,
          isnull(s_lt_bucks_categories.category_name,''),
          case when s_lt_bucks_categories.category_active = 1 then 'Y' else 'N' end,
          case when s_lt_bucks_categories.category_isdeleted = 1 then 'Y' else 'N' end;