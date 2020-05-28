﻿CREATE PROC [dbo].[proc_p_magento_catalog_rule_customer_group] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_magento_catalog_rule_customer_group'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from s_magento_catalog_rule_customer_group
 where dv_batch_id >= @process_dv_batch_id

delete from p_magento_catalog_rule_customer_group where bk_hash in (select bk_hash from #process)

insert into dbo.p_magento_catalog_rule_customer_group(
        bk_hash,
        row_id,
        customer_group_id,
        s_magento_catalog_rule_customer_group_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.row_id,
       h.customer_group_id,
       s_magento_catalog_rule_customer_group.s_magento_catalog_rule_customer_group_id,
       getdate(),
       suser_sname(),
       s_magento_catalog_rule_customer_group.dv_load_date_time dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       s_magento_catalog_rule_customer_group.dv_batch_id dv_batch_id
  from h_magento_catalog_rule_customer_group h
  join (select bk_hash, s_magento_catalog_rule_customer_group_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_magento_catalog_rule_customer_group_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from s_magento_catalog_rule_customer_group where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_magento_catalog_rule_customer_group
    on h.bk_hash = s_magento_catalog_rule_customer_group.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end