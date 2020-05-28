CREATE VIEW [dbo].[v_d_table_column_sort_order] AS select d.target_object,
       d.target_column, 
       d.data_type,
	   d.source_sql,
       e.business_key_sort_order,
       d.distribution_type,
       rank() over (partition by d.target_object
                        order by case when source_sql like 'p[_]%bk_hash' then 'aaaaaaaaaaaaaaaaaaa0'
                                      when e.business_key_sort_order is not null then 'aaaaaaaaaaaaaaaaaaa1'+cast(e.business_key_sort_order as varchar(10))
                                      when d.target_column = 'effective_date_time' then 'aaaaaaaaaaaaaaaaaaa2'
                                      when d.target_column = 'expiration_date_time' then 'aaaaaaaaaaaaaaaaaaa3'
                                      when e.business_key_sort_order is null and e.dv_etl_map_id is not null then 'zzzzzzzzzzzzzzzzzzzz'+d.target_column
                                      when d.target_column like 'p[_]%[_]id' and (d.source_sql like 'p%.p[_]%id' or d.source_sql is null) then 'zzzzzzzzzzzzzzzzzzzz'+d.target_column
                                      else replace(replace(target_column,'[',''),']','') end) column_rank
  from dbo.dv_d_etl_map d
  left join dbo.dv_etl_map e
    on (d.source_sql = e.dv_table+'.'+e.dv_column and e.business_key_sort_order is not null) --business keys
        or d.target_column = e.dv_table+'_'+e.dv_column --natural keys
 where target_column <> 'd_where_clause'
   and d.target_object not like 'v[_]%';