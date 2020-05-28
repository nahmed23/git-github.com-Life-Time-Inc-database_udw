CREATE PROC [dbo].[proc_dim_cafe_profit_center] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_cafe_profit_center)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#dim_cafe_profit_center') is not null drop table #dim_cafe_profit_center
create table dbo.#dim_cafe_profit_center with(distribution=hash(dim_cafe_profit_center_key), location=user_db, heap) as
select d_ig_ig_dimension_profit_center_dimension.dim_cafe_profit_center_key,
       case when isnull(r_mms_val_ig_profit_center.auto_reconcile_tips_flag, 0) = 1 then 'Y'
            else 'N' 
        end auto_reconcile_tips_flag,
       case when d_ig_it_cfg_profit_center_master.profit_center_id is null
             and dim_cafe_profit_center.bistro_flag is not null
            then dim_cafe_profit_center.bistro_flag
            else case when bistro_d_ig_it_cfg_profit_center_group_join.profit_center_group_bistro_flag = 'Y' then 'Y' 
                      else 'N' 
                  end 
        end bistro_flag,
       case when d_ig_it_cfg_profit_center_master.profit_center_id is null
             and dim_cafe_profit_center.cafe_flag is not null
            then dim_cafe_profit_center.cafe_flag
            else case when cafe_d_ig_it_cfg_profit_center_group_join.profit_center_group_cafe_flag = 'Y' then 'Y' 
                      else 'N' 
                  end 
        end cafe_flag,
       d_ig_ig_dimension_profit_center_dimension.profit_center_id,
       d_ig_ig_dimension_profit_center_dimension.profit_center_name,
       d_ig_ig_dimension_profit_center_dimension.store_id,
       d_ig_ig_dimension_profit_center_dimension.store_name,
       case when d_ig_ig_dimension_profit_center_dimension.dv_load_date_time >= isnull(r_mms_val_ig_profit_center.dv_load_date_time,'jan 1, 1753')
                 then d_ig_ig_dimension_profit_center_dimension.dv_load_date_time
            else r_mms_val_ig_profit_center.dv_load_date_time
        end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when d_ig_ig_dimension_profit_center_dimension.dv_batch_id >= isnull(r_mms_val_ig_profit_center.dv_batch_id,-1)
                 then d_ig_ig_dimension_profit_center_dimension.dv_batch_id
            else r_mms_val_ig_profit_center.dv_batch_id
        end dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
  from d_ig_ig_dimension_profit_center_dimension
  left join r_mms_val_ig_profit_center
    on d_ig_ig_dimension_profit_center_dimension.profit_center_id = r_mms_val_ig_profit_center.profit_center_number
   and r_mms_val_ig_profit_center.dv_load_end_date_time = 'dec 31, 9999'
 -- Investigated if we can eliminate the dual join.  No - there is one instance where there are both a 1 and 2 so both flags would be set.  A single join with a filter would result in two records.
  left join d_ig_it_cfg_profit_center_group_join bistro_d_ig_it_cfg_profit_center_group_join
    on d_ig_ig_dimension_profit_center_dimension.profit_center_id = bistro_d_ig_it_cfg_profit_center_group_join.profit_center_id
   and bistro_d_ig_it_cfg_profit_center_group_join.profit_center_group_bistro_flag = 'Y'
  left join d_ig_it_cfg_profit_center_group_join cafe_d_ig_it_cfg_profit_center_group_join
    on d_ig_ig_dimension_profit_center_dimension.profit_center_id = cafe_d_ig_it_cfg_profit_center_group_join.profit_center_id
   and cafe_d_ig_it_cfg_profit_center_group_join.profit_center_group_cafe_flag = 'Y'
  left join d_ig_it_cfg_profit_center_master
    on d_ig_ig_dimension_profit_center_dimension.profit_center_id = d_ig_it_cfg_profit_center_master.profit_center_id
   and d_ig_ig_dimension_profit_center_dimension.store_id = d_ig_it_cfg_profit_center_master.store_id
  left join dim_cafe_profit_center
    on d_ig_ig_dimension_profit_center_dimension.profit_center_id = dim_cafe_profit_center.profit_center_id
   and d_ig_ig_dimension_profit_center_dimension.store_id = dim_cafe_profit_center.store_id
 where (d_ig_ig_dimension_profit_center_dimension.dv_batch_id >= @load_dv_batch_id
        or r_mms_val_ig_profit_center.dv_batch_id >= @load_dv_batch_id
       )

-- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

begin tran

  delete dbo.dim_cafe_profit_center
   where dim_cafe_profit_center_key in (select dim_cafe_profit_center_key from dbo.#dim_cafe_profit_center) 

  insert into dim_cafe_profit_center
    (dim_cafe_profit_center_key,
     auto_reconcile_tips_flag,
     bistro_flag,
     cafe_flag,
     profit_center_id,
     profit_center_name,
     store_id,
     store_name,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
     )
  select dim_cafe_profit_center_key,
         auto_reconcile_tips_flag,
         bistro_flag,
         cafe_flag,
         profit_center_id,
         profit_center_name,
         store_id,
         store_name,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user
    from #dim_cafe_profit_center

commit tran

end
