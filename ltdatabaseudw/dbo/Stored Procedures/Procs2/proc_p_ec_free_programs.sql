CREATE PROC [dbo].[proc_p_ec_free_programs] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_ec_free_programs'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from l_ec_free_programs
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_ec_free_programs
 where dv_batch_id >= @process_dv_batch_id

delete from p_ec_free_programs where bk_hash in (select bk_hash from #process)

insert into dbo.p_ec_free_programs(
        bk_hash,
        free_program_id,
        l_ec_free_programs_id,
        s_ec_free_programs_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.free_program_id,
       isnull(l_ec_free_programs.l_ec_free_programs_id,'-998'),
       isnull(s_ec_free_programs.s_ec_free_programs_id,'-998'),
       getdate(),
       suser_sname(),
       case when l_ec_free_programs.dv_load_date_time >= isnull(s_ec_free_programs.dv_load_date_time,'jan 1, 1763') then l_ec_free_programs.dv_load_date_time
            else isnull(s_ec_free_programs.dv_load_date_time,'jan 1, 1763') end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_ec_free_programs.dv_batch_id >= isnull(s_ec_free_programs.dv_batch_id,-2) then l_ec_free_programs.dv_batch_id
            else isnull(s_ec_free_programs.dv_batch_id,-2) end dv_batch_id
  from h_ec_free_programs h
  left join (select bk_hash, l_ec_free_programs_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_ec_free_programs_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,l_ec_free_programs_id desc) r from l_ec_free_programs where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_ec_free_programs
    on h.bk_hash = l_ec_free_programs.bk_hash
  left join (select bk_hash, s_ec_free_programs_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_ec_free_programs_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_ec_free_programs_id desc) r from s_ec_free_programs where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_ec_free_programs
    on h.bk_hash = s_ec_free_programs.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end