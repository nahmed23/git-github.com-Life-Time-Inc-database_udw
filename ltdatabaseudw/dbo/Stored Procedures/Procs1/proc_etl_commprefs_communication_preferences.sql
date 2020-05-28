CREATE PROC [dbo].[proc_etl_commprefs_communication_preferences] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_commprefs_CommunicationPreferences

set @insert_date_time = getdate()
insert into dbo.stage_hash_commprefs_CommunicationPreferences (
       bk_hash,
       [Id],
       OptIn,
       EffectiveTime,
       CreatedTime,
       UpdatedTime,
       UpdatedBy,
       CommunicationTypeChannelId,
       CommunicationValueId,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([Id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [Id],
       OptIn,
       EffectiveTime,
       CreatedTime,
       UpdatedTime,
       UpdatedBy,
       CommunicationTypeChannelId,
       CommunicationValueId,
       isnull(cast(stage_commprefs_CommunicationPreferences.CreatedTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_commprefs_CommunicationPreferences
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_commprefs_communication_preferences @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_commprefs_communication_preferences (
       bk_hash,
       communication_preferences_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_commprefs_CommunicationPreferences.bk_hash,
       stage_hash_commprefs_CommunicationPreferences.[Id] communication_preferences_id,
       isnull(cast(stage_hash_commprefs_CommunicationPreferences.CreatedTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       28,
       @insert_date_time,
       @user
  from stage_hash_commprefs_CommunicationPreferences
  left join h_commprefs_communication_preferences
    on stage_hash_commprefs_CommunicationPreferences.bk_hash = h_commprefs_communication_preferences.bk_hash
 where h_commprefs_communication_preferences_id is null
   and stage_hash_commprefs_CommunicationPreferences.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_commprefs_communication_preferences
if object_id('tempdb..#l_commprefs_communication_preferences_inserts') is not null drop table #l_commprefs_communication_preferences_inserts
create table #l_commprefs_communication_preferences_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_commprefs_CommunicationPreferences.bk_hash,
       stage_hash_commprefs_CommunicationPreferences.[Id] communication_preferences_id,
       stage_hash_commprefs_CommunicationPreferences.CommunicationTypeChannelId communication_type_channel_id,
       stage_hash_commprefs_CommunicationPreferences.CommunicationValueId communication_value_id,
       isnull(cast(stage_hash_commprefs_CommunicationPreferences.CreatedTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationPreferences.[Id] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationPreferences.CommunicationTypeChannelId as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationPreferences.CommunicationValueId as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_commprefs_CommunicationPreferences
 where stage_hash_commprefs_CommunicationPreferences.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_commprefs_communication_preferences records
set @insert_date_time = getdate()
insert into l_commprefs_communication_preferences (
       bk_hash,
       communication_preferences_id,
       communication_type_channel_id,
       communication_value_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_commprefs_communication_preferences_inserts.bk_hash,
       #l_commprefs_communication_preferences_inserts.communication_preferences_id,
       #l_commprefs_communication_preferences_inserts.communication_type_channel_id,
       #l_commprefs_communication_preferences_inserts.communication_value_id,
       case when l_commprefs_communication_preferences.l_commprefs_communication_preferences_id is null then isnull(#l_commprefs_communication_preferences_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       28,
       #l_commprefs_communication_preferences_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_commprefs_communication_preferences_inserts
  left join p_commprefs_communication_preferences
    on #l_commprefs_communication_preferences_inserts.bk_hash = p_commprefs_communication_preferences.bk_hash
   and p_commprefs_communication_preferences.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_commprefs_communication_preferences
    on p_commprefs_communication_preferences.bk_hash = l_commprefs_communication_preferences.bk_hash
   and p_commprefs_communication_preferences.l_commprefs_communication_preferences_id = l_commprefs_communication_preferences.l_commprefs_communication_preferences_id
 where l_commprefs_communication_preferences.l_commprefs_communication_preferences_id is null
    or (l_commprefs_communication_preferences.l_commprefs_communication_preferences_id is not null
        and l_commprefs_communication_preferences.dv_hash <> #l_commprefs_communication_preferences_inserts.source_hash)

--calculate hash and lookup to current s_commprefs_communication_preferences
if object_id('tempdb..#s_commprefs_communication_preferences_inserts') is not null drop table #s_commprefs_communication_preferences_inserts
create table #s_commprefs_communication_preferences_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_commprefs_CommunicationPreferences.bk_hash,
       stage_hash_commprefs_CommunicationPreferences.[Id] communication_preferences_id,
       stage_hash_commprefs_CommunicationPreferences.OptIn opt_in,
       stage_hash_commprefs_CommunicationPreferences.EffectiveTime effective_time,
       stage_hash_commprefs_CommunicationPreferences.CreatedTime created_time,
       stage_hash_commprefs_CommunicationPreferences.UpdatedTime updated_time,
       stage_hash_commprefs_CommunicationPreferences.UpdatedBy updated_by,
       isnull(cast(stage_hash_commprefs_CommunicationPreferences.CreatedTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationPreferences.[Id] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationPreferences.OptIn as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationPreferences.EffectiveTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationPreferences.CreatedTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationPreferences.UpdatedTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_commprefs_CommunicationPreferences.UpdatedBy,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_commprefs_CommunicationPreferences
 where stage_hash_commprefs_CommunicationPreferences.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_commprefs_communication_preferences records
set @insert_date_time = getdate()
insert into s_commprefs_communication_preferences (
       bk_hash,
       communication_preferences_id,
       opt_in,
       effective_time,
       created_time,
       updated_time,
       updated_by,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_commprefs_communication_preferences_inserts.bk_hash,
       #s_commprefs_communication_preferences_inserts.communication_preferences_id,
       #s_commprefs_communication_preferences_inserts.opt_in,
       #s_commprefs_communication_preferences_inserts.effective_time,
       #s_commprefs_communication_preferences_inserts.created_time,
       #s_commprefs_communication_preferences_inserts.updated_time,
       #s_commprefs_communication_preferences_inserts.updated_by,
       case when s_commprefs_communication_preferences.s_commprefs_communication_preferences_id is null then isnull(#s_commprefs_communication_preferences_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       28,
       #s_commprefs_communication_preferences_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_commprefs_communication_preferences_inserts
  left join p_commprefs_communication_preferences
    on #s_commprefs_communication_preferences_inserts.bk_hash = p_commprefs_communication_preferences.bk_hash
   and p_commprefs_communication_preferences.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_commprefs_communication_preferences
    on p_commprefs_communication_preferences.bk_hash = s_commprefs_communication_preferences.bk_hash
   and p_commprefs_communication_preferences.s_commprefs_communication_preferences_id = s_commprefs_communication_preferences.s_commprefs_communication_preferences_id
 where s_commprefs_communication_preferences.s_commprefs_communication_preferences_id is null
    or (s_commprefs_communication_preferences.s_commprefs_communication_preferences_id is not null
        and s_commprefs_communication_preferences.dv_hash <> #s_commprefs_communication_preferences_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_commprefs_communication_preferences @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_commprefs_communication_preferences @current_dv_batch_id

end
