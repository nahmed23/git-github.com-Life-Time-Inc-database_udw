CREATE PROC [dbo].[proc_p_mms_subsidy_company_reimbursement_program] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_mms_subsidy_company_reimbursement_program'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from l_mms_subsidy_company_reimbursement_program
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_mms_subsidy_company_reimbursement_program
 where dv_batch_id >= @process_dv_batch_id

delete from p_mms_subsidy_company_reimbursement_program where bk_hash in (select bk_hash from #process)

insert into dbo.p_mms_subsidy_company_reimbursement_program(
        bk_hash,
        subsidy_company_reimbursement_program_id,
        l_mms_subsidy_company_reimbursement_program_id,
        s_mms_subsidy_company_reimbursement_program_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.subsidy_company_reimbursement_program_id,
       l_mms_subsidy_company_reimbursement_program.l_mms_subsidy_company_reimbursement_program_id,
       s_mms_subsidy_company_reimbursement_program.s_mms_subsidy_company_reimbursement_program_id,
       getdate(),
       suser_sname(),
       case when l_mms_subsidy_company_reimbursement_program.dv_load_date_time >= s_mms_subsidy_company_reimbursement_program.dv_load_date_time then l_mms_subsidy_company_reimbursement_program.dv_load_date_time
            else s_mms_subsidy_company_reimbursement_program.dv_load_date_time end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_mms_subsidy_company_reimbursement_program.dv_batch_id >= s_mms_subsidy_company_reimbursement_program.dv_batch_id then l_mms_subsidy_company_reimbursement_program.dv_batch_id
            else s_mms_subsidy_company_reimbursement_program.dv_batch_id end dv_batch_id
  from h_mms_subsidy_company_reimbursement_program h
  join (select bk_hash, l_mms_subsidy_company_reimbursement_program_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_mms_subsidy_company_reimbursement_program_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from l_mms_subsidy_company_reimbursement_program where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_mms_subsidy_company_reimbursement_program
    on h.bk_hash = l_mms_subsidy_company_reimbursement_program.bk_hash
  join (select bk_hash, s_mms_subsidy_company_reimbursement_program_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_mms_subsidy_company_reimbursement_program_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from s_mms_subsidy_company_reimbursement_program where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_mms_subsidy_company_reimbursement_program
    on h.bk_hash = s_mms_subsidy_company_reimbursement_program.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end