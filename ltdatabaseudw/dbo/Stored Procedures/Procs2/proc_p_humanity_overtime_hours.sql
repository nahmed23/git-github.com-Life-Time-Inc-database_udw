CREATE PROC [dbo].[proc_p_humanity_overtime_hours] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_humanity_overtime_hours'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from l_humanity_overtime_hours
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_humanity_overtime_hours
 where dv_batch_id >= @process_dv_batch_id

delete from p_humanity_overtime_hours where bk_hash in (select bk_hash from #process)

insert into dbo.p_humanity_overtime_hours(
        bk_hash,
        userid,
        employee_id,
        date_formatted,
        hours_regular,
        hours_overtime,
        hours_d_overtime,
        hours_position_id,
        hours_location_id,
        company_id,
        start_time,
        end_time,
        ltf_file_name,
        l_humanity_overtime_hours_id,
        s_humanity_overtime_hours_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.userid,
       h.employee_id,
       h.date_formatted,
       h.hours_regular,
       h.hours_overtime,
       h.hours_d_overtime,
       h.hours_position_id,
       h.hours_location_id,
       h.company_id,
       h.start_time,
       h.end_time,
       h.ltf_file_name,
       isnull(l_humanity_overtime_hours.l_humanity_overtime_hours_id,'-998'),
       isnull(s_humanity_overtime_hours.s_humanity_overtime_hours_id,'-998'),
       getdate(),
       suser_sname(),
       case when l_humanity_overtime_hours.dv_load_date_time >= isnull(s_humanity_overtime_hours.dv_load_date_time,'jan 1, 1763') then l_humanity_overtime_hours.dv_load_date_time
            else isnull(s_humanity_overtime_hours.dv_load_date_time,'jan 1, 1763') end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_humanity_overtime_hours.dv_batch_id >= isnull(s_humanity_overtime_hours.dv_batch_id,-2) then l_humanity_overtime_hours.dv_batch_id
            else isnull(s_humanity_overtime_hours.dv_batch_id,-2) end dv_batch_id
  from h_humanity_overtime_hours h
  left join (select bk_hash, l_humanity_overtime_hours_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_humanity_overtime_hours_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,l_humanity_overtime_hours_id desc) r from l_humanity_overtime_hours where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_humanity_overtime_hours
    on h.bk_hash = l_humanity_overtime_hours.bk_hash
  left join (select bk_hash, s_humanity_overtime_hours_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_humanity_overtime_hours_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_humanity_overtime_hours_id desc) r from s_humanity_overtime_hours where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_humanity_overtime_hours
    on h.bk_hash = s_humanity_overtime_hours.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end