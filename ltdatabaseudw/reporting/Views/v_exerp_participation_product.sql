CREATE VIEW [reporting].[v_exerp_participation_product]
AS SELECT
par.participation_id,
coalesce (p.comp_product_flag,p2.comp_product_flag) as comp_product_flag,
coalesce (case when p.dim_exerp_product_key = '-998' then null else p.dim_exerp_product_key end ,case when p2.dim_exerp_product_key = '-998' then null else p2.dim_exerp_product_key end) as dim_exerp_product_key,
coalesce (p.product_name,p2.product_name) as product_name
from
[marketing].[v_fact_exerp_participation] par
left join [marketing].[v_dim_exerp_subscription] s
	on par.dim_exerp_subscription_key = s.dim_exerp_subscription_key
inner join [reporting].[v_exerp_product] p 
	on s.dim_exerp_product_key = p.dim_exerp_product_key
left join [marketing].[v_dim_exerp_clipcard] cc
	on par.dim_exerp_clipcard_key = cc.dim_exerp_clipcard_key
inner join [reporting].[v_exerp_product] p2 
	on cc.dim_exerp_product_key = p2.dim_exerp_product_key;