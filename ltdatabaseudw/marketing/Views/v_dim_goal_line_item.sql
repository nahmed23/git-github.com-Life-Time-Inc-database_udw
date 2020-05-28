CREATE VIEW [marketing].[v_dim_goal_line_item]
AS select dim_goal_line_item.category_description category_description,
       dim_goal_line_item.description description,
       dim_goal_line_item.dim_goal_line_item_key dim_goal_line_item_key,
       dim_goal_line_item.dollar_amount_flag dollar_amount_flag,
       dim_goal_line_item.percentage_flag percentage_flag,
       dim_goal_line_item.quantity_flag quantity_flag,
       dim_goal_line_item.quota_flag quota_flag,
       dim_goal_line_item.region_type region_type,
       dim_goal_line_item.sort_order sort_order,
       dim_goal_line_item.subcategory_description subcategory_description
  from dbo.dim_goal_line_item;