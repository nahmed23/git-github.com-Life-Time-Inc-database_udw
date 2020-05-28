CREATE VIEW [dbo].[v_d_sort_order] AS select d.target_object,
       d.target_column, 
       d.data_type,
	   d.source_sql,
       so.business_key_sort_order,
       null distribution_type,
       so.column_rank
  from dv_d_etl_map d
  join dbo.v_d_table_column_sort_order so
    on d.source_sql like '%'+so.target_object+'.'+so.target_column+'%'
 where d.target_object like 'v[_]%'
   and d.target_column <> 'v_clause'
union
select *
from dbo.v_d_table_column_sort_order so;