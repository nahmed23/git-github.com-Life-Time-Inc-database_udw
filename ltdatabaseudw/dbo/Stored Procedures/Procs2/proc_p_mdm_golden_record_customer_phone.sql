﻿CREATE PROC [dbo].[proc_p_mdm_golden_record_customer_phone] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_mdm_golden_record_customer_phone'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from l_mdm_golden_record_customer_phone
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_mdm_golden_record_customer_phone
 where dv_batch_id >= @process_dv_batch_id

delete from p_mdm_golden_record_customer_phone where bk_hash in (select bk_hash from #process)

insert into dbo.p_mdm_golden_record_customer_phone(
        bk_hash,
        phone_type,
        phone,
        entity_id,
        l_mdm_golden_record_customer_phone_id,
        s_mdm_golden_record_customer_phone_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.phone_type,
       h.phone,
       h.entity_id,
       l_mdm_golden_record_customer_phone.l_mdm_golden_record_customer_phone_id,
       s_mdm_golden_record_customer_phone.s_mdm_golden_record_customer_phone_id,
       getdate(),
       suser_sname(),
       case when l_mdm_golden_record_customer_phone.dv_load_date_time >= s_mdm_golden_record_customer_phone.dv_load_date_time then l_mdm_golden_record_customer_phone.dv_load_date_time
            else s_mdm_golden_record_customer_phone.dv_load_date_time end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_mdm_golden_record_customer_phone.dv_batch_id >= s_mdm_golden_record_customer_phone.dv_batch_id then l_mdm_golden_record_customer_phone.dv_batch_id
            else s_mdm_golden_record_customer_phone.dv_batch_id end dv_batch_id
  from h_mdm_golden_record_customer_phone h
  join (select bk_hash, l_mdm_golden_record_customer_phone_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_mdm_golden_record_customer_phone_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from l_mdm_golden_record_customer_phone where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_mdm_golden_record_customer_phone
    on h.bk_hash = l_mdm_golden_record_customer_phone.bk_hash
  join (select bk_hash, s_mdm_golden_record_customer_phone_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_mdm_golden_record_customer_phone_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from s_mdm_golden_record_customer_phone where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_mdm_golden_record_customer_phone
    on h.bk_hash = s_mdm_golden_record_customer_phone.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end