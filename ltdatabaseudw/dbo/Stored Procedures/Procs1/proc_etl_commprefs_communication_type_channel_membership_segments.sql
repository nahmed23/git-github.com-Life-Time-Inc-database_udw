CREATE PROC [dbo].[proc_etl_commprefs_communication_type_channel_membership_segments] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_commprefs_CommunicationTypeChannelMembershipSegments

set @insert_date_time = getdate()
insert into dbo.stage_hash_commprefs_CommunicationTypeChannelMembershipSegments (
       bk_hash,
       [Id],
       Show,
       OptInDefault,
       CreatedTime,
       UpdatedTime,
       CommunicationTypeChannelId,
       MembershipSegmentId,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([Id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [Id],
       Show,
       OptInDefault,
       CreatedTime,
       UpdatedTime,
       CommunicationTypeChannelId,
       MembershipSegmentId,
       isnull(cast(stage_commprefs_CommunicationTypeChannelMembershipSegments.UpdatedTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_commprefs_CommunicationTypeChannelMembershipSegments
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_commprefs_communication_type_channel_membership_segments @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_commprefs_communication_type_channel_membership_segments (
       bk_hash,
       communication_type_channel_membership_segments_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.bk_hash,
       stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.[Id] communication_type_channel_membership_segments_id,
       isnull(cast(stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.UpdatedTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       28,
       @insert_date_time,
       @user
  from stage_hash_commprefs_CommunicationTypeChannelMembershipSegments
  left join h_commprefs_communication_type_channel_membership_segments
    on stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.bk_hash = h_commprefs_communication_type_channel_membership_segments.bk_hash
 where h_commprefs_communication_type_channel_membership_segments_id is null
   and stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_commprefs_communication_type_channel_membership_segments
if object_id('tempdb..#l_commprefs_communication_type_channel_membership_segments_inserts') is not null drop table #l_commprefs_communication_type_channel_membership_segments_inserts
create table #l_commprefs_communication_type_channel_membership_segments_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.bk_hash,
       stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.[Id] communication_type_channel_membership_segments_id,
       stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.CommunicationTypeChannelId communication_type_channel_id,
       stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.MembershipSegmentId membership_segment_id,
       isnull(cast(stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.UpdatedTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.[Id] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.CommunicationTypeChannelId as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.MembershipSegmentId as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_commprefs_CommunicationTypeChannelMembershipSegments
 where stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_commprefs_communication_type_channel_membership_segments records
set @insert_date_time = getdate()
insert into l_commprefs_communication_type_channel_membership_segments (
       bk_hash,
       communication_type_channel_membership_segments_id,
       communication_type_channel_id,
       membership_segment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_commprefs_communication_type_channel_membership_segments_inserts.bk_hash,
       #l_commprefs_communication_type_channel_membership_segments_inserts.communication_type_channel_membership_segments_id,
       #l_commprefs_communication_type_channel_membership_segments_inserts.communication_type_channel_id,
       #l_commprefs_communication_type_channel_membership_segments_inserts.membership_segment_id,
       case when l_commprefs_communication_type_channel_membership_segments.l_commprefs_communication_type_channel_membership_segments_id is null then isnull(#l_commprefs_communication_type_channel_membership_segments_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       28,
       #l_commprefs_communication_type_channel_membership_segments_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_commprefs_communication_type_channel_membership_segments_inserts
  left join p_commprefs_communication_type_channel_membership_segments
    on #l_commprefs_communication_type_channel_membership_segments_inserts.bk_hash = p_commprefs_communication_type_channel_membership_segments.bk_hash
   and p_commprefs_communication_type_channel_membership_segments.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_commprefs_communication_type_channel_membership_segments
    on p_commprefs_communication_type_channel_membership_segments.bk_hash = l_commprefs_communication_type_channel_membership_segments.bk_hash
   and p_commprefs_communication_type_channel_membership_segments.l_commprefs_communication_type_channel_membership_segments_id = l_commprefs_communication_type_channel_membership_segments.l_commprefs_communication_type_channel_membership_segments_id
 where l_commprefs_communication_type_channel_membership_segments.l_commprefs_communication_type_channel_membership_segments_id is null
    or (l_commprefs_communication_type_channel_membership_segments.l_commprefs_communication_type_channel_membership_segments_id is not null
        and l_commprefs_communication_type_channel_membership_segments.dv_hash <> #l_commprefs_communication_type_channel_membership_segments_inserts.source_hash)

--calculate hash and lookup to current s_commprefs_communication_type_channel_membership_segments
if object_id('tempdb..#s_commprefs_communication_type_channel_membership_segments_inserts') is not null drop table #s_commprefs_communication_type_channel_membership_segments_inserts
create table #s_commprefs_communication_type_channel_membership_segments_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.bk_hash,
       stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.[Id] communication_type_channel_membership_segments_id,
       stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.Show show,
       stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.OptInDefault opt_in_default,
       stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.CreatedTime created_time,
       stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.UpdatedTime updated_time,
       isnull(cast(stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.UpdatedTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.[Id] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.Show as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.OptInDefault as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.CreatedTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.UpdatedTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_commprefs_CommunicationTypeChannelMembershipSegments
 where stage_hash_commprefs_CommunicationTypeChannelMembershipSegments.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_commprefs_communication_type_channel_membership_segments records
set @insert_date_time = getdate()
insert into s_commprefs_communication_type_channel_membership_segments (
       bk_hash,
       communication_type_channel_membership_segments_id,
       show,
       opt_in_default,
       created_time,
       updated_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_commprefs_communication_type_channel_membership_segments_inserts.bk_hash,
       #s_commprefs_communication_type_channel_membership_segments_inserts.communication_type_channel_membership_segments_id,
       #s_commprefs_communication_type_channel_membership_segments_inserts.show,
       #s_commprefs_communication_type_channel_membership_segments_inserts.opt_in_default,
       #s_commprefs_communication_type_channel_membership_segments_inserts.created_time,
       #s_commprefs_communication_type_channel_membership_segments_inserts.updated_time,
       case when s_commprefs_communication_type_channel_membership_segments.s_commprefs_communication_type_channel_membership_segments_id is null then isnull(#s_commprefs_communication_type_channel_membership_segments_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       28,
       #s_commprefs_communication_type_channel_membership_segments_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_commprefs_communication_type_channel_membership_segments_inserts
  left join p_commprefs_communication_type_channel_membership_segments
    on #s_commprefs_communication_type_channel_membership_segments_inserts.bk_hash = p_commprefs_communication_type_channel_membership_segments.bk_hash
   and p_commprefs_communication_type_channel_membership_segments.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_commprefs_communication_type_channel_membership_segments
    on p_commprefs_communication_type_channel_membership_segments.bk_hash = s_commprefs_communication_type_channel_membership_segments.bk_hash
   and p_commprefs_communication_type_channel_membership_segments.s_commprefs_communication_type_channel_membership_segments_id = s_commprefs_communication_type_channel_membership_segments.s_commprefs_communication_type_channel_membership_segments_id
 where s_commprefs_communication_type_channel_membership_segments.s_commprefs_communication_type_channel_membership_segments_id is null
    or (s_commprefs_communication_type_channel_membership_segments.s_commprefs_communication_type_channel_membership_segments_id is not null
        and s_commprefs_communication_type_channel_membership_segments.dv_hash <> #s_commprefs_communication_type_channel_membership_segments_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_commprefs_communication_type_channel_membership_segments @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_commprefs_communication_type_channel_membership_segments @current_dv_batch_id

end
