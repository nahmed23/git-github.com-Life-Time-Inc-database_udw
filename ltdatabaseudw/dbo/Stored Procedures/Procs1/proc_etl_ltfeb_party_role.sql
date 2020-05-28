CREATE PROC [dbo].[proc_etl_ltfeb_party_role] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ltfeb_PartyRole

set @insert_date_time = getdate()
insert into dbo.stage_hash_ltfeb_PartyRole (
       bk_hash,
       party_role_id,
       pr_party_id,
       party_role_type,
       headquarters_facility_id,
       update_datetime,
       update_userid,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(party_role_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       party_role_id,
       pr_party_id,
       party_role_type,
       headquarters_facility_id,
       update_datetime,
       update_userid,
       isnull(cast(stage_ltfeb_PartyRole.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ltfeb_PartyRole
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ltfeb_party_role @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ltfeb_party_role (
       bk_hash,
       party_role_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ltfeb_PartyRole.bk_hash,
       stage_hash_ltfeb_PartyRole.party_role_id party_role_id,
       isnull(cast(stage_hash_ltfeb_PartyRole.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       18,
       @insert_date_time,
       @user
  from stage_hash_ltfeb_PartyRole
  left join h_ltfeb_party_role
    on stage_hash_ltfeb_PartyRole.bk_hash = h_ltfeb_party_role.bk_hash
 where h_ltfeb_party_role_id is null
   and stage_hash_ltfeb_PartyRole.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ltfeb_party_role
if object_id('tempdb..#l_ltfeb_party_role_inserts') is not null drop table #l_ltfeb_party_role_inserts
create table #l_ltfeb_party_role_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ltfeb_PartyRole.bk_hash,
       stage_hash_ltfeb_PartyRole.party_role_id party_role_id,
       stage_hash_ltfeb_PartyRole.pr_party_id pr_party_id,
       stage_hash_ltfeb_PartyRole.headquarters_facility_id headquarters_facility_id,
       isnull(cast(stage_hash_ltfeb_PartyRole.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ltfeb_PartyRole.party_role_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfeb_PartyRole.pr_party_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ltfeb_PartyRole.headquarters_facility_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ltfeb_PartyRole
 where stage_hash_ltfeb_PartyRole.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ltfeb_party_role records
set @insert_date_time = getdate()
insert into l_ltfeb_party_role (
       bk_hash,
       party_role_id,
       pr_party_id,
       headquarters_facility_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ltfeb_party_role_inserts.bk_hash,
       #l_ltfeb_party_role_inserts.party_role_id,
       #l_ltfeb_party_role_inserts.pr_party_id,
       #l_ltfeb_party_role_inserts.headquarters_facility_id,
       case when l_ltfeb_party_role.l_ltfeb_party_role_id is null then isnull(#l_ltfeb_party_role_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       18,
       #l_ltfeb_party_role_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ltfeb_party_role_inserts
  left join p_ltfeb_party_role
    on #l_ltfeb_party_role_inserts.bk_hash = p_ltfeb_party_role.bk_hash
   and p_ltfeb_party_role.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ltfeb_party_role
    on p_ltfeb_party_role.bk_hash = l_ltfeb_party_role.bk_hash
   and p_ltfeb_party_role.l_ltfeb_party_role_id = l_ltfeb_party_role.l_ltfeb_party_role_id
 where l_ltfeb_party_role.l_ltfeb_party_role_id is null
    or (l_ltfeb_party_role.l_ltfeb_party_role_id is not null
        and l_ltfeb_party_role.dv_hash <> #l_ltfeb_party_role_inserts.source_hash)

--calculate hash and lookup to current s_ltfeb_party_role
if object_id('tempdb..#s_ltfeb_party_role_inserts') is not null drop table #s_ltfeb_party_role_inserts
create table #s_ltfeb_party_role_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ltfeb_PartyRole.bk_hash,
       stage_hash_ltfeb_PartyRole.party_role_id party_role_id,
       stage_hash_ltfeb_PartyRole.party_role_type party_role_type,
       stage_hash_ltfeb_PartyRole.update_datetime update_date_time,
       stage_hash_ltfeb_PartyRole.update_userid update_user_id,
       isnull(cast(stage_hash_ltfeb_PartyRole.update_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ltfeb_PartyRole.party_role_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ltfeb_PartyRole.party_role_type,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ltfeb_PartyRole.update_datetime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ltfeb_PartyRole.update_userid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ltfeb_PartyRole
 where stage_hash_ltfeb_PartyRole.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ltfeb_party_role records
set @insert_date_time = getdate()
insert into s_ltfeb_party_role (
       bk_hash,
       party_role_id,
       party_role_type,
       update_date_time,
       update_user_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ltfeb_party_role_inserts.bk_hash,
       #s_ltfeb_party_role_inserts.party_role_id,
       #s_ltfeb_party_role_inserts.party_role_type,
       #s_ltfeb_party_role_inserts.update_date_time,
       #s_ltfeb_party_role_inserts.update_user_id,
       case when s_ltfeb_party_role.s_ltfeb_party_role_id is null then isnull(#s_ltfeb_party_role_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       18,
       #s_ltfeb_party_role_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ltfeb_party_role_inserts
  left join p_ltfeb_party_role
    on #s_ltfeb_party_role_inserts.bk_hash = p_ltfeb_party_role.bk_hash
   and p_ltfeb_party_role.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ltfeb_party_role
    on p_ltfeb_party_role.bk_hash = s_ltfeb_party_role.bk_hash
   and p_ltfeb_party_role.s_ltfeb_party_role_id = s_ltfeb_party_role.s_ltfeb_party_role_id
 where s_ltfeb_party_role.s_ltfeb_party_role_id is null
    or (s_ltfeb_party_role.s_ltfeb_party_role_id is not null
        and s_ltfeb_party_role.dv_hash <> #s_ltfeb_party_role_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ltfeb_party_role @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ltfeb_party_role @current_dv_batch_id

end
