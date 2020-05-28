CREATE PROC [dbo].[proc_etl_pool_pass_courtesy_visits] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_pool_pass_CourtesyVisits

set @insert_date_time = getdate()
insert into dbo.stage_hash_pool_pass_CourtesyVisits (
       bk_hash,
       Id,
       ClubId,
       EmployeePartyId,
       MemberPartyId,
       CreatedDate,
       UpdatedDate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(Id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       Id,
       ClubId,
       EmployeePartyId,
       MemberPartyId,
       CreatedDate,
       UpdatedDate,
       isnull(cast(stage_pool_pass_CourtesyVisits.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_pool_pass_CourtesyVisits
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_pool_pass_courtesy_visits @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_pool_pass_courtesy_visits (
       bk_hash,
       courtesy_visits_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_pool_pass_CourtesyVisits.bk_hash,
       stage_hash_pool_pass_CourtesyVisits.Id courtesy_visits_id,
       isnull(cast(stage_hash_pool_pass_CourtesyVisits.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       52,
       @insert_date_time,
       @user
  from stage_hash_pool_pass_CourtesyVisits
  left join h_pool_pass_courtesy_visits
    on stage_hash_pool_pass_CourtesyVisits.bk_hash = h_pool_pass_courtesy_visits.bk_hash
 where h_pool_pass_courtesy_visits_id is null
   and stage_hash_pool_pass_CourtesyVisits.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_pool_pass_courtesy_visits
if object_id('tempdb..#l_pool_pass_courtesy_visits_inserts') is not null drop table #l_pool_pass_courtesy_visits_inserts
create table #l_pool_pass_courtesy_visits_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_pool_pass_CourtesyVisits.bk_hash,
       stage_hash_pool_pass_CourtesyVisits.Id courtesy_visits_id,
       stage_hash_pool_pass_CourtesyVisits.ClubId club_id,
       stage_hash_pool_pass_CourtesyVisits.EmployeePartyId employee_party_id,
       stage_hash_pool_pass_CourtesyVisits.MemberPartyId member_party_id,
       isnull(cast(stage_hash_pool_pass_CourtesyVisits.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_pool_pass_CourtesyVisits.Id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_pool_pass_CourtesyVisits.ClubId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_pool_pass_CourtesyVisits.EmployeePartyId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_pool_pass_CourtesyVisits.MemberPartyId as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_pool_pass_CourtesyVisits
 where stage_hash_pool_pass_CourtesyVisits.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_pool_pass_courtesy_visits records
set @insert_date_time = getdate()
insert into l_pool_pass_courtesy_visits (
       bk_hash,
       courtesy_visits_id,
       club_id,
       employee_party_id,
       member_party_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_pool_pass_courtesy_visits_inserts.bk_hash,
       #l_pool_pass_courtesy_visits_inserts.courtesy_visits_id,
       #l_pool_pass_courtesy_visits_inserts.club_id,
       #l_pool_pass_courtesy_visits_inserts.employee_party_id,
       #l_pool_pass_courtesy_visits_inserts.member_party_id,
       case when l_pool_pass_courtesy_visits.l_pool_pass_courtesy_visits_id is null then isnull(#l_pool_pass_courtesy_visits_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       52,
       #l_pool_pass_courtesy_visits_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_pool_pass_courtesy_visits_inserts
  left join p_pool_pass_courtesy_visits
    on #l_pool_pass_courtesy_visits_inserts.bk_hash = p_pool_pass_courtesy_visits.bk_hash
   and p_pool_pass_courtesy_visits.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_pool_pass_courtesy_visits
    on p_pool_pass_courtesy_visits.bk_hash = l_pool_pass_courtesy_visits.bk_hash
   and p_pool_pass_courtesy_visits.l_pool_pass_courtesy_visits_id = l_pool_pass_courtesy_visits.l_pool_pass_courtesy_visits_id
 where l_pool_pass_courtesy_visits.l_pool_pass_courtesy_visits_id is null
    or (l_pool_pass_courtesy_visits.l_pool_pass_courtesy_visits_id is not null
        and l_pool_pass_courtesy_visits.dv_hash <> #l_pool_pass_courtesy_visits_inserts.source_hash)

--calculate hash and lookup to current s_pool_pass_courtesy_visits
if object_id('tempdb..#s_pool_pass_courtesy_visits_inserts') is not null drop table #s_pool_pass_courtesy_visits_inserts
create table #s_pool_pass_courtesy_visits_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_pool_pass_CourtesyVisits.bk_hash,
       stage_hash_pool_pass_CourtesyVisits.Id courtesy_visits_id,
       stage_hash_pool_pass_CourtesyVisits.CreatedDate created_date,
       stage_hash_pool_pass_CourtesyVisits.UpdatedDate updated_date,
       isnull(cast(stage_hash_pool_pass_CourtesyVisits.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_pool_pass_CourtesyVisits.Id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_pool_pass_CourtesyVisits.CreatedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_pool_pass_CourtesyVisits.UpdatedDate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_pool_pass_CourtesyVisits
 where stage_hash_pool_pass_CourtesyVisits.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_pool_pass_courtesy_visits records
set @insert_date_time = getdate()
insert into s_pool_pass_courtesy_visits (
       bk_hash,
       courtesy_visits_id,
       created_date,
       updated_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_pool_pass_courtesy_visits_inserts.bk_hash,
       #s_pool_pass_courtesy_visits_inserts.courtesy_visits_id,
       #s_pool_pass_courtesy_visits_inserts.created_date,
       #s_pool_pass_courtesy_visits_inserts.updated_date,
       case when s_pool_pass_courtesy_visits.s_pool_pass_courtesy_visits_id is null then isnull(#s_pool_pass_courtesy_visits_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       52,
       #s_pool_pass_courtesy_visits_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_pool_pass_courtesy_visits_inserts
  left join p_pool_pass_courtesy_visits
    on #s_pool_pass_courtesy_visits_inserts.bk_hash = p_pool_pass_courtesy_visits.bk_hash
   and p_pool_pass_courtesy_visits.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_pool_pass_courtesy_visits
    on p_pool_pass_courtesy_visits.bk_hash = s_pool_pass_courtesy_visits.bk_hash
   and p_pool_pass_courtesy_visits.s_pool_pass_courtesy_visits_id = s_pool_pass_courtesy_visits.s_pool_pass_courtesy_visits_id
 where s_pool_pass_courtesy_visits.s_pool_pass_courtesy_visits_id is null
    or (s_pool_pass_courtesy_visits.s_pool_pass_courtesy_visits_id is not null
        and s_pool_pass_courtesy_visits.dv_hash <> #s_pool_pass_courtesy_visits_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_pool_pass_courtesy_visits @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_pool_pass_courtesy_visits @current_dv_batch_id

end
