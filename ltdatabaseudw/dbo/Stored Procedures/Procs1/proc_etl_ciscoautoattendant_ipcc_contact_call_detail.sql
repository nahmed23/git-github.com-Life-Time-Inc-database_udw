CREATE PROC [dbo].[proc_etl_ciscoautoattendant_ipcc_contact_call_detail] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ciscoautoattendant_IPCCContactCallDetail

set @insert_date_time = getdate()
insert into dbo.stage_hash_ciscoautoattendant_IPCCContactCallDetail (
       bk_hash,
       SessionID,
       SessionSeqNum,
       NodeID,
       ProfileID,
       ContactType,
       ContactDisposition,
       DispositionReason,
       OriginatorType,
       OriginatorID,
       OriginatorDN,
       DestinationType,
       DestinationID,
       DestinationDN,
       StartDateTime,
       EndDateTime,
       GmtOffset,
       CalledNumber,
       OrigCalledNumber,
       ApplicationTaskID,
       ApplicationID,
       ApplicationName,
       ConnectTime,
       CustomVariable1,
       CustomVariable2,
       CustomVariable3,
       CustomVariable4,
       CustomVariable5,
       CustomVariable6,
       CustomVariable7,
       CustomVariable8,
       CustomVariable9,
       CustomVariable10,
       AccountNumber,
       CallerEnteredDigits,
       BadCallTag,
       Transfer,
       Redirect,
       Conference,
       Flowout,
       MetServiceLevel,
       CampaignID,
       OrigProtocolCallRef,
       DestProtocolCallRef,
       CallResult,
       DialingListID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(SessionID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SessionSeqNum as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(NodeID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ProfileID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       SessionID,
       SessionSeqNum,
       NodeID,
       ProfileID,
       ContactType,
       ContactDisposition,
       DispositionReason,
       OriginatorType,
       OriginatorID,
       OriginatorDN,
       DestinationType,
       DestinationID,
       DestinationDN,
       StartDateTime,
       EndDateTime,
       GmtOffset,
       CalledNumber,
       OrigCalledNumber,
       ApplicationTaskID,
       ApplicationID,
       ApplicationName,
       ConnectTime,
       CustomVariable1,
       CustomVariable2,
       CustomVariable3,
       CustomVariable4,
       CustomVariable5,
       CustomVariable6,
       CustomVariable7,
       CustomVariable8,
       CustomVariable9,
       CustomVariable10,
       AccountNumber,
       CallerEnteredDigits,
       BadCallTag,
       Transfer,
       Redirect,
       Conference,
       Flowout,
       MetServiceLevel,
       CampaignID,
       OrigProtocolCallRef,
       DestProtocolCallRef,
       CallResult,
       DialingListID,
       isnull(cast(stage_ciscoautoattendant_IPCCContactCallDetail.startdatetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ciscoautoattendant_IPCCContactCallDetail
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ciscoautoattendant_ipcc_contact_call_detail @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ciscoautoattendant_ipcc_contact_call_detail (
       bk_hash,
       session_id,
       session_seq_num,
       node_id,
       profile_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ciscoautoattendant_IPCCContactCallDetail.bk_hash,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.SessionID session_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.SessionSeqNum session_seq_num,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.NodeID node_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.ProfileID profile_id,
       isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.startdatetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       20,
       @insert_date_time,
       @user
  from stage_hash_ciscoautoattendant_IPCCContactCallDetail
  left join h_ciscoautoattendant_ipcc_contact_call_detail
    on stage_hash_ciscoautoattendant_IPCCContactCallDetail.bk_hash = h_ciscoautoattendant_ipcc_contact_call_detail.bk_hash
 where h_ciscoautoattendant_ipcc_contact_call_detail_id is null
   and stage_hash_ciscoautoattendant_IPCCContactCallDetail.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ciscoautoattendant_ipcc_contact_call_detail
if object_id('tempdb..#l_ciscoautoattendant_ipcc_contact_call_detail_inserts') is not null drop table #l_ciscoautoattendant_ipcc_contact_call_detail_inserts
create table #l_ciscoautoattendant_ipcc_contact_call_detail_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ciscoautoattendant_IPCCContactCallDetail.bk_hash,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.SessionID session_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.SessionSeqNum session_seq_num,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.NodeID node_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.ProfileID profile_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.ApplicationTaskID application_task_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.ApplicationID application_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.startdatetime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.SessionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.SessionSeqNum as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.NodeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.ProfileID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.ApplicationTaskID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.ApplicationID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ciscoautoattendant_IPCCContactCallDetail
 where stage_hash_ciscoautoattendant_IPCCContactCallDetail.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ciscoautoattendant_ipcc_contact_call_detail records
set @insert_date_time = getdate()
insert into l_ciscoautoattendant_ipcc_contact_call_detail (
       bk_hash,
       session_id,
       session_seq_num,
       node_id,
       profile_id,
       application_task_id,
       application_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ciscoautoattendant_ipcc_contact_call_detail_inserts.bk_hash,
       #l_ciscoautoattendant_ipcc_contact_call_detail_inserts.session_id,
       #l_ciscoautoattendant_ipcc_contact_call_detail_inserts.session_seq_num,
       #l_ciscoautoattendant_ipcc_contact_call_detail_inserts.node_id,
       #l_ciscoautoattendant_ipcc_contact_call_detail_inserts.profile_id,
       #l_ciscoautoattendant_ipcc_contact_call_detail_inserts.application_task_id,
       #l_ciscoautoattendant_ipcc_contact_call_detail_inserts.application_id,
       case when l_ciscoautoattendant_ipcc_contact_call_detail.l_ciscoautoattendant_ipcc_contact_call_detail_id is null then isnull(#l_ciscoautoattendant_ipcc_contact_call_detail_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       20,
       #l_ciscoautoattendant_ipcc_contact_call_detail_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ciscoautoattendant_ipcc_contact_call_detail_inserts
  left join p_ciscoautoattendant_ipcc_contact_call_detail
    on #l_ciscoautoattendant_ipcc_contact_call_detail_inserts.bk_hash = p_ciscoautoattendant_ipcc_contact_call_detail.bk_hash
   and p_ciscoautoattendant_ipcc_contact_call_detail.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ciscoautoattendant_ipcc_contact_call_detail
    on p_ciscoautoattendant_ipcc_contact_call_detail.bk_hash = l_ciscoautoattendant_ipcc_contact_call_detail.bk_hash
   and p_ciscoautoattendant_ipcc_contact_call_detail.l_ciscoautoattendant_ipcc_contact_call_detail_id = l_ciscoautoattendant_ipcc_contact_call_detail.l_ciscoautoattendant_ipcc_contact_call_detail_id
 where l_ciscoautoattendant_ipcc_contact_call_detail.l_ciscoautoattendant_ipcc_contact_call_detail_id is null
    or (l_ciscoautoattendant_ipcc_contact_call_detail.l_ciscoautoattendant_ipcc_contact_call_detail_id is not null
        and l_ciscoautoattendant_ipcc_contact_call_detail.dv_hash <> #l_ciscoautoattendant_ipcc_contact_call_detail_inserts.source_hash)

--calculate hash and lookup to current s_ciscoautoattendant_ipcc_contact_call_detail
if object_id('tempdb..#s_ciscoautoattendant_ipcc_contact_call_detail_inserts') is not null drop table #s_ciscoautoattendant_ipcc_contact_call_detail_inserts
create table #s_ciscoautoattendant_ipcc_contact_call_detail_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ciscoautoattendant_IPCCContactCallDetail.bk_hash,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.SessionID session_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.SessionSeqNum session_seq_num,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.NodeID node_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.ProfileID profile_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.ContactType contact_type,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.ContactDisposition contact_disposition,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.DispositionReason disposition_reason,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.OriginatorType originator_type,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.OriginatorID originator_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.OriginatorDN originator_dn,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.DestinationType destination_type,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.DestinationID destination_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.DestinationDN destination_dn,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.StartDateTime start_date_time,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.EndDateTime end_date_time,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.GmtOffset gmt_offset,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CalledNumber called_number,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.OrigCalledNumber orig_called_number,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.ApplicationName application_name,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.ConnectTime connect_time,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable1 custom_variable_1,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable2 custom_variable_2,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable3 custom_variable_3,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable4 custom_variable_4,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable5 custom_variable_5,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable6 custom_variable_6,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable7 custom_variable_7,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable8 custom_variable_8,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable9 custom_variable_9,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable10 custom_variable_10,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.AccountNumber account_number,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CallerEnteredDigits caller_entered_digits,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.BadCallTag bad_call_tag,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.Transfer transfer,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.Redirect redirect,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.Conference conference,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.Flowout flow_out,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.MetServiceLevel met_service_level,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CampaignID campaign_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.OrigProtocolCallRef orig_protocol_call_ref,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.DestProtocolCallRef dest_protocol_call_ref,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.CallResult call_result,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.DialingListID dialing_list_id,
       stage_hash_ciscoautoattendant_IPCCContactCallDetail.startdatetime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.SessionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.SessionSeqNum as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.NodeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.ProfileID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.ContactType as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.ContactDisposition as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.DispositionReason,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.OriginatorType as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.OriginatorID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.OriginatorDN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.DestinationType as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.DestinationID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.DestinationDN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ciscoautoattendant_IPCCContactCallDetail.StartDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ciscoautoattendant_IPCCContactCallDetail.EndDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.GmtOffset as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CalledNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.OrigCalledNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.ApplicationName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.ConnectTime as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable3,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable4,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable5,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable6,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable7,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable8,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable9,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CustomVariable10,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.AccountNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CallerEnteredDigits,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.BadCallTag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.Transfer as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.Redirect as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.Conference as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.Flowout as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.MetServiceLevel as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CampaignID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.OrigProtocolCallRef,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ciscoautoattendant_IPCCContactCallDetail.DestProtocolCallRef,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.CallResult as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ciscoautoattendant_IPCCContactCallDetail.DialingListID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ciscoautoattendant_IPCCContactCallDetail
 where stage_hash_ciscoautoattendant_IPCCContactCallDetail.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ciscoautoattendant_ipcc_contact_call_detail records
set @insert_date_time = getdate()
insert into s_ciscoautoattendant_ipcc_contact_call_detail (
       bk_hash,
       session_id,
       session_seq_num,
       node_id,
       profile_id,
       contact_type,
       contact_disposition,
       disposition_reason,
       originator_type,
       originator_id,
       originator_dn,
       destination_type,
       destination_id,
       destination_dn,
       start_date_time,
       end_date_time,
       gmt_offset,
       called_number,
       orig_called_number,
       application_name,
       connect_time,
       custom_variable_1,
       custom_variable_2,
       custom_variable_3,
       custom_variable_4,
       custom_variable_5,
       custom_variable_6,
       custom_variable_7,
       custom_variable_8,
       custom_variable_9,
       custom_variable_10,
       account_number,
       caller_entered_digits,
       bad_call_tag,
       transfer,
       redirect,
       conference,
       flow_out,
       met_service_level,
       campaign_id,
       orig_protocol_call_ref,
       dest_protocol_call_ref,
       call_result,
       dialing_list_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.bk_hash,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.session_id,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.session_seq_num,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.node_id,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.profile_id,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.contact_type,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.contact_disposition,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.disposition_reason,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.originator_type,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.originator_id,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.originator_dn,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.destination_type,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.destination_id,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.destination_dn,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.start_date_time,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.end_date_time,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.gmt_offset,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.called_number,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.orig_called_number,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.application_name,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.connect_time,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.custom_variable_1,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.custom_variable_2,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.custom_variable_3,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.custom_variable_4,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.custom_variable_5,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.custom_variable_6,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.custom_variable_7,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.custom_variable_8,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.custom_variable_9,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.custom_variable_10,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.account_number,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.caller_entered_digits,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.bad_call_tag,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.transfer,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.redirect,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.conference,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.flow_out,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.met_service_level,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.campaign_id,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.orig_protocol_call_ref,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.dest_protocol_call_ref,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.call_result,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.dialing_list_id,
       case when s_ciscoautoattendant_ipcc_contact_call_detail.s_ciscoautoattendant_ipcc_contact_call_detail_id is null then isnull(#s_ciscoautoattendant_ipcc_contact_call_detail_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       20,
       #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ciscoautoattendant_ipcc_contact_call_detail_inserts
  left join p_ciscoautoattendant_ipcc_contact_call_detail
    on #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.bk_hash = p_ciscoautoattendant_ipcc_contact_call_detail.bk_hash
   and p_ciscoautoattendant_ipcc_contact_call_detail.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ciscoautoattendant_ipcc_contact_call_detail
    on p_ciscoautoattendant_ipcc_contact_call_detail.bk_hash = s_ciscoautoattendant_ipcc_contact_call_detail.bk_hash
   and p_ciscoautoattendant_ipcc_contact_call_detail.s_ciscoautoattendant_ipcc_contact_call_detail_id = s_ciscoautoattendant_ipcc_contact_call_detail.s_ciscoautoattendant_ipcc_contact_call_detail_id
 where s_ciscoautoattendant_ipcc_contact_call_detail.s_ciscoautoattendant_ipcc_contact_call_detail_id is null
    or (s_ciscoautoattendant_ipcc_contact_call_detail.s_ciscoautoattendant_ipcc_contact_call_detail_id is not null
        and s_ciscoautoattendant_ipcc_contact_call_detail.dv_hash <> #s_ciscoautoattendant_ipcc_contact_call_detail_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ciscoautoattendant_ipcc_contact_call_detail @current_dv_batch_id

end
