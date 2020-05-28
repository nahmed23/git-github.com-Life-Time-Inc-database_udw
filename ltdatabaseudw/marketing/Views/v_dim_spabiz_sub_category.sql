CREATE VIEW [marketing].[v_dim_spabiz_sub_category] AS with cat_hier (dim_spabiz_category_key,category_name,dim_spabiz_data_type_key,quick_id,dim_spabiz_store_key,gl_account,sub_category_flag,parent_dim_spabiz_category_key,data_type_id,data_type_name)
as (
select d1.dim_spabiz_category_key,
       d1.category_name,
       d1.dim_spabiz_data_type_key,
       s.quick_id,
       d1.dim_spabiz_store_key,
       l.gl_account,
       d1.sub_category_flag,
       parent_category_bk_hash parent_dim_spabiz_category_key,
       dt1.data_type_id,
       dt1.data_type_name
from d_spabiz_category d1
join d_spabiz_data_type dt1
  on d1.dim_spabiz_data_type_key = dt1.dim_spabiz_data_type_key
 --and dt1.data_type_id = 4 --product
join p_spabiz_category p
  on d1.dim_spabiz_category_key = p.bk_hash
join s_spabiz_category s
  on p.s_spabiz_category_id = s.s_spabiz_category_id
join l_spabiz_category l
  on p.l_spabiz_category_id = l.l_spabiz_category_id
)
select c1.dim_spabiz_category_key,
       c1.category_name level_1_name,
       c1.gl_account level_1_gl_account,
       c2.dim_spabiz_category_key sub_dim_spabiz_category_key,
       c2.category_name level_2_name,
       c2.gl_account level_2_gl_account,
       c1.data_type_id,
       c1.data_type_name
from cat_hier c1
join cat_hier c2 
  on c2.parent_dim_spabiz_category_key = c1.dim_spabiz_category_key
 and c2.sub_category_flag = 'Y'
where c1.sub_category_flag = 'N';