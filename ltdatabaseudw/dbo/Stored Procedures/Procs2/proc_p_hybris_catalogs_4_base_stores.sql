CREATE PROC [dbo].[proc_p_hybris_catalogs_4_base_stores] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_hybris_catalogs_4_base_stores'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from l_hybris_catalogs_4_base_stores
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_hybris_catalogs_4_base_stores
 where dv_batch_id >= @process_dv_batch_id

delete from p_hybris_catalogs_4_base_stores where bk_hash in (select bk_hash from #process)

insert into dbo.p_hybris_catalogs_4_base_stores(
        bk_hash,
        catalogs_4_base_stores_pk,
        l_hybris_catalogs_4_base_stores_id,
        s_hybris_catalogs_4_base_stores_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.catalogs_4_base_stores_pk,
       l_hybris_catalogs_4_base_stores.l_hybris_catalogs_4_base_stores_id,
       s_hybris_catalogs_4_base_stores.s_hybris_catalogs_4_base_stores_id,
       getdate(),
       suser_sname(),
       case when l_hybris_catalogs_4_base_stores.dv_load_date_time >= s_hybris_catalogs_4_base_stores.dv_load_date_time then l_hybris_catalogs_4_base_stores.dv_load_date_time
            else s_hybris_catalogs_4_base_stores.dv_load_date_time end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_hybris_catalogs_4_base_stores.dv_batch_id >= s_hybris_catalogs_4_base_stores.dv_batch_id then l_hybris_catalogs_4_base_stores.dv_batch_id
            else s_hybris_catalogs_4_base_stores.dv_batch_id end dv_batch_id
  from h_hybris_catalogs_4_base_stores h
  join (select bk_hash, l_hybris_catalogs_4_base_stores_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_hybris_catalogs_4_base_stores_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from l_hybris_catalogs_4_base_stores where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_hybris_catalogs_4_base_stores
    on h.bk_hash = l_hybris_catalogs_4_base_stores.bk_hash
  join (select bk_hash, s_hybris_catalogs_4_base_stores_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_hybris_catalogs_4_base_stores_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from s_hybris_catalogs_4_base_stores where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_hybris_catalogs_4_base_stores
    on h.bk_hash = s_hybris_catalogs_4_base_stores.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end