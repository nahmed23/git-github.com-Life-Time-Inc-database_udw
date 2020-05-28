CREATE PROC [dbo].[proc_etl_commprefs_communication_type_channels] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_commprefs_CommunicationTypeChannels

set @insert_date_time = getdate()
insert into dbo.stage_hash_commprefs_CommunicationTypeChannels (
       bk_hash,
       [Id],
       DisplayNameOverride,
       CreatedTime,
       UpdatedTime,
       DeletedTime,
       ChannelKey,
       CommunicationTypeId,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [Id],
       DisplayNameOverride,
       CreatedTime,
       UpdatedTime,
       DeletedTime,
       ChannelKey,
       CommunicationTypeId,
       isnull(cast(stage_commprefs_CommunicationTypeChannels.createdtime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_commprefs_CommunicationTypeChannels
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_commprefs_communication_type_channels @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_commprefs_communication_type_channels (
       bk_hash,
       communication_type_channels_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_commprefs_CommunicationTypeChannels.bk_hash,
       stage_hash_commprefs_CommunicationTypeChannels.[id] communication_type_channels_id,
       isnull(cast(stage_hash_commprefs_CommunicationTypeChannels.createdtime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       28,
       @insert_date_time,
       @user
  from stage_hash_commprefs_CommunicationTypeChannels
  left join h_commprefs_communication_type_channels
    on stage_hash_commprefs_CommunicationTypeChannels.bk_hash = h_commprefs_communication_type_channels.bk_hash
 where h_commprefs_communication_type_channels_id is null
   and stage_hash_commprefs_CommunicationTypeChannels.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_commprefs_communication_type_channels
if object_id('tempdb..#l_commprefs_communication_type_channels_inserts') is not null drop table #l_commprefs_communication_type_channels_inserts
create table #l_commprefs_communication_type_channels_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_commprefs_CommunicationTypeChannels.bk_hash,
       stage_hash_commprefs_CommunicationTypeChannels.[id] communication_type_channels_id,
       stage_hash_commprefs_CommunicationTypeChannels.ChannelKey channel_key,
       stage_hash_commprefs_CommunicationTypeChannels.CommunicationTypeId communication_type_id,
       isnull(cast(stage_hash_commprefs_CommunicationTypeChannels.createdtime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypeChannels.[id] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_commprefs_CommunicationTypeChannels.ChannelKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypeChannels.CommunicationTypeId as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_commprefs_CommunicationTypeChannels
 where stage_hash_commprefs_CommunicationTypeChannels.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_commprefs_communication_type_channels records
set @insert_date_time = getdate()
insert into l_commprefs_communication_type_channels (
       bk_hash,
       communication_type_channels_id,
       channel_key,
       communication_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_commprefs_communication_type_channels_inserts.bk_hash,
       #l_commprefs_communication_type_channels_inserts.communication_type_channels_id,
       #l_commprefs_communication_type_channels_inserts.channel_key,
       #l_commprefs_communication_type_channels_inserts.communication_type_id,
       case when l_commprefs_communication_type_channels.l_commprefs_communication_type_channels_id is null then isnull(#l_commprefs_communication_type_channels_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       28,
       #l_commprefs_communication_type_channels_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_commprefs_communication_type_channels_inserts
  left join p_commprefs_communication_type_channels
    on #l_commprefs_communication_type_channels_inserts.bk_hash = p_commprefs_communication_type_channels.bk_hash
   and p_commprefs_communication_type_channels.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_commprefs_communication_type_channels
    on p_commprefs_communication_type_channels.bk_hash = l_commprefs_communication_type_channels.bk_hash
   and p_commprefs_communication_type_channels.l_commprefs_communication_type_channels_id = l_commprefs_communication_type_channels.l_commprefs_communication_type_channels_id
 where l_commprefs_communication_type_channels.l_commprefs_communication_type_channels_id is null
    or (l_commprefs_communication_type_channels.l_commprefs_communication_type_channels_id is not null
        and l_commprefs_communication_type_channels.dv_hash <> #l_commprefs_communication_type_channels_inserts.source_hash)

--calculate hash and lookup to current s_commprefs_communication_type_channels
if object_id('tempdb..#s_commprefs_communication_type_channels_inserts') is not null drop table #s_commprefs_communication_type_channels_inserts
create table #s_commprefs_communication_type_channels_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_commprefs_CommunicationTypeChannels.bk_hash,
       stage_hash_commprefs_CommunicationTypeChannels.[id] communication_type_channels_id,
       stage_hash_commprefs_CommunicationTypeChannels.DisplayNameOverride display_name_override,
       stage_hash_commprefs_CommunicationTypeChannels.CreatedTime created_time,
       stage_hash_commprefs_CommunicationTypeChannels.UpdatedTime updated_time,
       stage_hash_commprefs_CommunicationTypeChannels.DeletedTime deleted_time,
       isnull(cast(stage_hash_commprefs_CommunicationTypeChannels.createdtime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypeChannels.[id] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_commprefs_CommunicationTypeChannels.DisplayNameOverride,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationTypeChannels.CreatedTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationTypeChannels.UpdatedTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationTypeChannels.DeletedTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_commprefs_CommunicationTypeChannels
 where stage_hash_commprefs_CommunicationTypeChannels.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_commprefs_communication_type_channels records
set @insert_date_time = getdate()
insert into s_commprefs_communication_type_channels (
       bk_hash,
       communication_type_channels_id,
       display_name_override,
       created_time,
       updated_time,
       deleted_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_commprefs_communication_type_channels_inserts.bk_hash,
       #s_commprefs_communication_type_channels_inserts.communication_type_channels_id,
       #s_commprefs_communication_type_channels_inserts.display_name_override,
       #s_commprefs_communication_type_channels_inserts.created_time,
       #s_commprefs_communication_type_channels_inserts.updated_time,
       #s_commprefs_communication_type_channels_inserts.deleted_time,
       case when s_commprefs_communication_type_channels.s_commprefs_communication_type_channels_id is null then isnull(#s_commprefs_communication_type_channels_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       28,
       #s_commprefs_communication_type_channels_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_commprefs_communication_type_channels_inserts
  left join p_commprefs_communication_type_channels
    on #s_commprefs_communication_type_channels_inserts.bk_hash = p_commprefs_communication_type_channels.bk_hash
   and p_commprefs_communication_type_channels.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_commprefs_communication_type_channels
    on p_commprefs_communication_type_channels.bk_hash = s_commprefs_communication_type_channels.bk_hash
   and p_commprefs_communication_type_channels.s_commprefs_communication_type_channels_id = s_commprefs_communication_type_channels.s_commprefs_communication_type_channels_id
 where s_commprefs_communication_type_channels.s_commprefs_communication_type_channels_id is null
    or (s_commprefs_communication_type_channels.s_commprefs_communication_type_channels_id is not null
        and s_commprefs_communication_type_channels.dv_hash <> #s_commprefs_communication_type_channels_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_commprefs_communication_type_channels @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_commprefs_communication_type_channels @current_dv_batch_id

end
