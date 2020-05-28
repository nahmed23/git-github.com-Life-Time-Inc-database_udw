CREATE VIEW [marketing].[v_dim_cafe_profit_center]
AS select d_ig_ig_dimension_profit_center_dimension.dummy_bk_hash_key dummy_bk_hash_key,
       d_ig_ig_dimension_profit_center_dimension.customer_id customer_id,
       d_ig_ig_dimension_profit_center_dimension.dim_cafe_profit_center_key dim_cafe_profit_center_key,
       d_ig_ig_dimension_profit_center_dimension.ent_id ent_id,
       d_ig_ig_dimension_profit_center_dimension.profit_center_dim_id profit_center_dim_id,
       d_ig_ig_dimension_profit_center_dimension.profit_center_id profit_center_id,
       d_ig_ig_dimension_profit_center_dimension.profit_center_name profit_center_name,
       d_ig_ig_dimension_profit_center_dimension.store_id store_id,
       d_ig_ig_dimension_profit_center_dimension.store_name store_name
  from dbo.d_ig_ig_dimension_profit_center_dimension;