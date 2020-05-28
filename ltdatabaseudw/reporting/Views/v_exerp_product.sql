CREATE VIEW [reporting].[v_exerp_product]
AS select a.*,
  case when b.product_id is null then 'N' else 'Y' end comp_product_flag

  from [marketing].[v_dim_exerp_product] a
  left join [marketing].[v_dim_exerp_product_product_group] b
      on a.[dim_exerp_product_key] = b.dim_exerp_product_key and b.product_group_name='Gratis Clip Cards';