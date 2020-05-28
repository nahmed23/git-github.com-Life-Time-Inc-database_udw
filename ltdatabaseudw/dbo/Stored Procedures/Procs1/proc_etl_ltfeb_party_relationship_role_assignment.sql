CREATE PROC [dbo].[proc_etl_ltfeb_party_relationship_role_assignment] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ltfeb_PartyRelationshipRoleAssignment

set @insert_date_time = getdate()
insert into dbo.stage_hash_ltfeb_PartyRelationshipRoleAssignment (
       bk_hash,
       party_relationship_id,
       party_relationship_role_type,
       assigned_id,
       update_datetime,
       update_userid,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(party_relationship_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(party_relationship_role_type,'z#@$k%&P'))),2) bk_hash,
       party_relationship_id,
       party_relationship_role_type,
       assigned_id,
       update_datetime,
       update_userid,
       isnull(cast(stage_ltfeb_PartyRelationshipRoleAssignment.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ltfeb_PartyRelationshipRoleAssignment
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ltfeb_party_relationship_role_assignment @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ltfeb_party_relationship_role_assignment (
       bk_hash,
       party_relationship_id,
       party_relationship_role_type,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ltfeb_PartyRelationshipRoleAssignment.bk_hash,
       stage_hash_ltfeb_PartyRelationshipRoleAssignment.party_relationship_id party_relationship_id,
       stage_hash_ltfeb_PartyRelationshipRoleAssignment.party_relationship_role_type party_relationship_role_type,
       isnull(cast(stage_hash_ltfeb_PartyRelationshipRoleAssignment.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       18,
       @insert_date_time,
       @user
  from stage_hash_ltfeb_PartyRelationshipRoleAssignment
  left join h_ltfeb_party_relationship_role_assignment
    on stage_hash_ltfeb_PartyRelationshipRoleAssignment.bk_hash = h_ltfeb_party_relationship_role_assignment.bk_hash
 where h_ltfeb_party_relationship_role_assignment_id is null
   and stage_hash_ltfeb_PartyRelationshipRoleAssignment.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ltfeb_party_relationship_role_assignment
if object_id('tempdb..#l_ltfeb_party_relationship_role_assignment_inserts') is not null drop table #l_ltfeb_party_relationship_role_assignment_inserts
create table #l_ltfeb_party_relationship_role_assignment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ltfeb_PartyRelationshipRoleAssignment.bk_hash,
       stage_hash_ltfeb_PartyRelationshipRoleAssignment.party_relationship_id party_relationship_id,
       stage_hash_ltfeb_PartyRelationshipRoleAssignment.party_relationship_role_type party_relationship_role_type,
       stage_hash_ltfeb_PartyRelationshipRoleAssignment.assigned_id assigned_id,
       isnull(cast(stage_hash_ltfeb_PartyRelationshipRoleAssignment.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ltfeb_PartyRelationshipRoleAssignment.party_relationship_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ltfeb_PartyRelationshipRoleAssignment.party_relationship_role_type,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ltfeb_PartyRelationshipRoleAssignment.assigned_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ltfeb_PartyRelationshipRoleAssignment
 where stage_hash_ltfeb_PartyRelationshipRoleAssignment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ltfeb_party_relationship_role_assignment records
set @insert_date_time = getdate()
insert into l_ltfeb_party_relationship_role_assignment (
       bk_hash,
       party_relationship_id,
       party_relationship_role_type,
       assigned_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ltfeb_party_relationship_role_assignment_inserts.bk_hash,
       #l_ltfeb_party_relationship_role_assignment_inserts.party_relationship_id,
       #l_ltfeb_party_relationship_role_assignment_inserts.party_relationship_role_type,
       #l_ltfeb_party_relationship_role_assignment_inserts.assigned_id,
       case when l_ltfeb_party_relationship_role_assignment.l_ltfeb_party_relationship_role_assignment_id is null then isnull(#l_ltfeb_party_relationship_role_assignment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       18,
       #l_ltfeb_party_relationship_role_assignment_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ltfeb_party_relationship_role_assignment_inserts
  left join p_ltfeb_party_relationship_role_assignment
    on #l_ltfeb_party_relationship_role_assignment_inserts.bk_hash = p_ltfeb_party_relationship_role_assignment.bk_hash
   and p_ltfeb_party_relationship_role_assignment.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ltfeb_party_relationship_role_assignment
    on p_ltfeb_party_relationship_role_assignment.bk_hash = l_ltfeb_party_relationship_role_assignment.bk_hash
   and p_ltfeb_party_relationship_role_assignment.l_ltfeb_party_relationship_role_assignment_id = l_ltfeb_party_relationship_role_assignment.l_ltfeb_party_relationship_role_assignment_id
 where l_ltfeb_party_relationship_role_assignment.l_ltfeb_party_relationship_role_assignment_id is null
    or (l_ltfeb_party_relationship_role_assignment.l_ltfeb_party_relationship_role_assignment_id is not null
        and l_ltfeb_party_relationship_role_assignment.dv_hash <> #l_ltfeb_party_relationship_role_assignment_inserts.source_hash)

--calculate hash and lookup to current s_ltfeb_party_relationship_role_assignment
if object_id('tempdb..#s_ltfeb_party_relationship_role_assignment_inserts') is not null drop table #s_ltfeb_party_relationship_role_assignment_inserts
create table #s_ltfeb_party_relationship_role_assignment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ltfeb_PartyRelationshipRoleAssignment.bk_hash,
       stage_hash_ltfeb_PartyRelationshipRoleAssignment.party_relationship_id party_relationship_id,
       stage_hash_ltfeb_PartyRelationshipRoleAssignment.party_relationship_role_type party_relationship_role_type,
       stage_hash_ltfeb_PartyRelationshipRoleAssignment.update_datetime update_date_time,
       stage_hash_ltfeb_PartyRelationshipRoleAssignment.update_userid update_user_id,
       isnull(cast(stage_hash_ltfeb_PartyRelationshipRoleAssignment.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ltfeb_PartyRelationshipRoleAssignment.party_relationship_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ltfeb_PartyRelationshipRoleAssignment.party_relationship_role_type,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfeb_PartyRelationshipRoleAssignment.update_datetime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ltfeb_PartyRelationshipRoleAssignment.update_userid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ltfeb_PartyRelationshipRoleAssignment
 where stage_hash_ltfeb_PartyRelationshipRoleAssignment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ltfeb_party_relationship_role_assignment records
set @insert_date_time = getdate()
insert into s_ltfeb_party_relationship_role_assignment (
       bk_hash,
       party_relationship_id,
       party_relationship_role_type,
       update_date_time,
       update_user_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ltfeb_party_relationship_role_assignment_inserts.bk_hash,
       #s_ltfeb_party_relationship_role_assignment_inserts.party_relationship_id,
       #s_ltfeb_party_relationship_role_assignment_inserts.party_relationship_role_type,
       #s_ltfeb_party_relationship_role_assignment_inserts.update_date_time,
       #s_ltfeb_party_relationship_role_assignment_inserts.update_user_id,
       case when s_ltfeb_party_relationship_role_assignment.s_ltfeb_party_relationship_role_assignment_id is null then isnull(#s_ltfeb_party_relationship_role_assignment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       18,
       #s_ltfeb_party_relationship_role_assignment_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ltfeb_party_relationship_role_assignment_inserts
  left join p_ltfeb_party_relationship_role_assignment
    on #s_ltfeb_party_relationship_role_assignment_inserts.bk_hash = p_ltfeb_party_relationship_role_assignment.bk_hash
   and p_ltfeb_party_relationship_role_assignment.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ltfeb_party_relationship_role_assignment
    on p_ltfeb_party_relationship_role_assignment.bk_hash = s_ltfeb_party_relationship_role_assignment.bk_hash
   and p_ltfeb_party_relationship_role_assignment.s_ltfeb_party_relationship_role_assignment_id = s_ltfeb_party_relationship_role_assignment.s_ltfeb_party_relationship_role_assignment_id
 where s_ltfeb_party_relationship_role_assignment.s_ltfeb_party_relationship_role_assignment_id is null
    or (s_ltfeb_party_relationship_role_assignment.s_ltfeb_party_relationship_role_assignment_id is not null
        and s_ltfeb_party_relationship_role_assignment.dv_hash <> #s_ltfeb_party_relationship_role_assignment_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ltfeb_party_relationship_role_assignment @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ltfeb_party_relationship_role_assignment @current_dv_batch_id

end
