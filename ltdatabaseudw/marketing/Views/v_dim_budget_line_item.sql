CREATE VIEW [marketing].[v_dim_budget_line_item] AS select d_budget_line_item.d_budget_line_item_id,
       d_budget_line_item.dim_budget_line_item_key,
       d_budget_line_item.budget_line_item_id,
       d_budget_line_item.description,
       d_budget_line_item.sub_category_description,
       d_budget_line_item.category_description,
       d_budget_line_item.quantity_flag,
	   d_budget_line_item.dollar_amount_flag
  from dbo.d_budget_line_item;