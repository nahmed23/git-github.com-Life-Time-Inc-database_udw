﻿CREATE PROC [dbo].[proc_p_ltfeb_party_relationship_role_assignment] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_ltfeb_party_relationship_role_assignment'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from l_ltfeb_party_relationship_role_assignment
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_ltfeb_party_relationship_role_assignment
 where dv_batch_id >= @process_dv_batch_id

delete from p_ltfeb_party_relationship_role_assignment where bk_hash in (select bk_hash from #process)

insert into dbo.p_ltfeb_party_relationship_role_assignment(
        bk_hash,
        party_relationship_id,
        party_relationship_role_type,
        l_ltfeb_party_relationship_role_assignment_id,
        s_ltfeb_party_relationship_role_assignment_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.party_relationship_id,
       h.party_relationship_role_type,
       l_ltfeb_party_relationship_role_assignment.l_ltfeb_party_relationship_role_assignment_id,
       s_ltfeb_party_relationship_role_assignment.s_ltfeb_party_relationship_role_assignment_id,
       getdate(),
       suser_sname(),
       case when l_ltfeb_party_relationship_role_assignment.dv_load_date_time >= s_ltfeb_party_relationship_role_assignment.dv_load_date_time then l_ltfeb_party_relationship_role_assignment.dv_load_date_time
            else s_ltfeb_party_relationship_role_assignment.dv_load_date_time end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_ltfeb_party_relationship_role_assignment.dv_batch_id >= s_ltfeb_party_relationship_role_assignment.dv_batch_id then l_ltfeb_party_relationship_role_assignment.dv_batch_id
            else s_ltfeb_party_relationship_role_assignment.dv_batch_id end dv_batch_id
  from h_ltfeb_party_relationship_role_assignment h
  join (select bk_hash, l_ltfeb_party_relationship_role_assignment_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_ltfeb_party_relationship_role_assignment_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from l_ltfeb_party_relationship_role_assignment where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_ltfeb_party_relationship_role_assignment
    on h.bk_hash = l_ltfeb_party_relationship_role_assignment.bk_hash
  join (select bk_hash, s_ltfeb_party_relationship_role_assignment_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_ltfeb_party_relationship_role_assignment_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from s_ltfeb_party_relationship_role_assignment where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_ltfeb_party_relationship_role_assignment
    on h.bk_hash = s_ltfeb_party_relationship_role_assignment.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end