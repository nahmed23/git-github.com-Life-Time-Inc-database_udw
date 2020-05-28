CREATE PROC [dbo].[proc_dim_cafe_payment_type] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

 -- This data for this table is unusual and rather small so it makes sense to just rebuild the whole table each run
 --    The pattern is not all that unusual, but the way data changes in the source ig_dimension.dbo.tender_dimension makes it unusual as that data is fully reloaded every day

 -- There are duplicate tender_ids in ig_dimension_tender_dimension with more than one record with no expiration_dim_date_key
 --   so just take the record with the latest effective_dim_date_key and max tender_dim_id
if object_id('tempdb..#rank_d_ig_ig_dimension_tender_dimension') is not null drop table #rank_d_ig_ig_dimension_tender_dimension
create table dbo.#rank_d_ig_ig_dimension_tender_dimension with(distribution=round_robin, location=user_db) as
select row_number() over (partition by d_ig_ig_dimension_tender_dimension.tender_id
                              order by d_ig_ig_dimension_tender_dimension.effective_dim_date_key desc,
                                       d_ig_ig_dimension_tender_dimension.tender_dim_id desc) r,
       d_ig_ig_dimension_tender_dimension.bk_hash
  from d_ig_ig_dimension_tender_dimension
 where d_ig_ig_dimension_tender_dimension.expiration_dim_date_key = '99991231'
    or d_ig_ig_dimension_tender_dimension.bk_hash in ('-997','-998','-999')
update #rank_d_ig_ig_dimension_tender_dimension
   set r = 1
 where bk_hash in ('-997','-998','-999')

 -- main query
if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(dim_cafe_payment_type_key), location=user_db) as
select d_ig_ig_dimension_tender_dimension.dim_cafe_payment_type_key,
      	d_ig_ig_dimension_tender_dimension.tender_id,
        isnull(d_ig_it_cfg_tender_master.payment_type, d_ig_ig_dimension_tender_dimension.payment_type) payment_type,
      	d_ig_ig_dimension_tender_dimension.payment_class,
      	case when d_ig_ig_dimension_tender_dimension.dv_load_date_time < isnull(d_ig_it_cfg_tender_master.dv_load_date_time ,'Jan 1, 1753')
            then d_ig_it_cfg_tender_master.dv_load_date_time
            else d_ig_ig_dimension_tender_dimension.dv_load_date_time end as dv_load_date_time,
       'dec 31, 9999' dv_load_end_date_time,
      	case when d_ig_ig_dimension_tender_dimension.dv_batch_id < isnull(d_ig_it_cfg_tender_master.dv_batch_id,-1)
            then d_ig_it_cfg_tender_master.dv_batch_id 
            else d_ig_ig_dimension_tender_dimension.dv_batch_id end as dv_batch_id
  from d_ig_ig_dimension_tender_dimension
  join #rank_d_ig_ig_dimension_tender_dimension
    on d_ig_ig_dimension_tender_dimension.bk_hash = #rank_d_ig_ig_dimension_tender_dimension.bk_hash
   and #rank_d_ig_ig_dimension_tender_dimension.r = 1
  left join d_ig_it_cfg_tender_master
    on d_ig_ig_dimension_tender_dimension.dim_cafe_payment_type_key = d_ig_it_cfg_tender_master.dim_cafe_payment_type_key
   and d_ig_it_cfg_tender_master.ent_id = 1
 -- Note that there is no where clause on dv_batch_id to limit the records as the table is always fully rebuilt

begin tran

  delete dbo.dim_cafe_payment_type

  insert into dim_cafe_payment_type
  (
   dim_cafe_payment_type_key,
   tender_id,
   payment_type,
   payment_class,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
   dv_inserted_date_time,
   dv_insert_user
  )
  select dim_cafe_payment_type_key,
         tender_id,
         payment_type,
         payment_class,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #etl_step_1

commit tran

end
