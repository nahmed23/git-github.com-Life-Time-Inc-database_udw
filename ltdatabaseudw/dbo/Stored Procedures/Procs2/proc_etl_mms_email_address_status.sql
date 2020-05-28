CREATE PROC [dbo].[proc_etl_mms_email_address_status] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_EmailAddressStatus

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_EmailAddressStatus (
       bk_hash,
       EmailAddressStatusID,
       EmailAddress,
       StatusFromDate,
       StatusThruDate,
       InsertedDateTime,
       UpdatedDateTime,
       ValCommunicationPreferenceSourceID,
       ValCommunicationPreferenceStatusID,
       EmailAddressSearch,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(EmailAddressStatusID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       EmailAddressStatusID,
       EmailAddress,
       StatusFromDate,
       StatusThruDate,
       InsertedDateTime,
       UpdatedDateTime,
       ValCommunicationPreferenceSourceID,
       ValCommunicationPreferenceStatusID,
       EmailAddressSearch,
       isnull(cast(stage_mms_EmailAddressStatus.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_EmailAddressStatus
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_email_address_status @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_email_address_status (
       bk_hash,
       email_address_status_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_EmailAddressStatus.bk_hash,
       stage_hash_mms_EmailAddressStatus.EmailAddressStatusID email_address_status_id,
       isnull(cast(stage_hash_mms_EmailAddressStatus.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_EmailAddressStatus
  left join h_mms_email_address_status
    on stage_hash_mms_EmailAddressStatus.bk_hash = h_mms_email_address_status.bk_hash
 where h_mms_email_address_status_id is null
   and stage_hash_mms_EmailAddressStatus.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_email_address_status
if object_id('tempdb..#l_mms_email_address_status_inserts') is not null drop table #l_mms_email_address_status_inserts
create table #l_mms_email_address_status_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_EmailAddressStatus.bk_hash,
       stage_hash_mms_EmailAddressStatus.EmailAddressStatusID email_address_status_id,
       stage_hash_mms_EmailAddressStatus.ValCommunicationPreferenceSourceID val_communication_preference_source_id,
       stage_hash_mms_EmailAddressStatus.ValCommunicationPreferenceStatusID val_communication_preference_status_id,
       isnull(cast(stage_hash_mms_EmailAddressStatus.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_EmailAddressStatus.EmailAddressStatusID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_EmailAddressStatus.ValCommunicationPreferenceSourceID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_EmailAddressStatus.ValCommunicationPreferenceStatusID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_EmailAddressStatus
 where stage_hash_mms_EmailAddressStatus.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_email_address_status records
set @insert_date_time = getdate()
insert into l_mms_email_address_status (
       bk_hash,
       email_address_status_id,
       val_communication_preference_source_id,
       val_communication_preference_status_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_email_address_status_inserts.bk_hash,
       #l_mms_email_address_status_inserts.email_address_status_id,
       #l_mms_email_address_status_inserts.val_communication_preference_source_id,
       #l_mms_email_address_status_inserts.val_communication_preference_status_id,
       case when l_mms_email_address_status.l_mms_email_address_status_id is null then isnull(#l_mms_email_address_status_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_email_address_status_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_email_address_status_inserts
  left join p_mms_email_address_status
    on #l_mms_email_address_status_inserts.bk_hash = p_mms_email_address_status.bk_hash
   and p_mms_email_address_status.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_email_address_status
    on p_mms_email_address_status.bk_hash = l_mms_email_address_status.bk_hash
   and p_mms_email_address_status.l_mms_email_address_status_id = l_mms_email_address_status.l_mms_email_address_status_id
 where l_mms_email_address_status.l_mms_email_address_status_id is null
    or (l_mms_email_address_status.l_mms_email_address_status_id is not null
        and l_mms_email_address_status.dv_hash <> #l_mms_email_address_status_inserts.source_hash)

--calculate hash and lookup to current s_mms_email_address_status
if object_id('tempdb..#s_mms_email_address_status_inserts') is not null drop table #s_mms_email_address_status_inserts
create table #s_mms_email_address_status_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_EmailAddressStatus.bk_hash,
       stage_hash_mms_EmailAddressStatus.EmailAddressStatusID email_address_status_id,
       stage_hash_mms_EmailAddressStatus.EmailAddress email_address,
       stage_hash_mms_EmailAddressStatus.StatusFromDate status_from_date,
       stage_hash_mms_EmailAddressStatus.StatusThruDate status_thru_date,
       stage_hash_mms_EmailAddressStatus.InsertedDateTime inserted_date_time,
       stage_hash_mms_EmailAddressStatus.UpdatedDateTime updated_date_time,
       stage_hash_mms_EmailAddressStatus.EmailAddressSearch email_address_search,
       isnull(cast(stage_hash_mms_EmailAddressStatus.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_EmailAddressStatus.EmailAddressStatusID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_EmailAddressStatus.EmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EmailAddressStatus.StatusFromDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EmailAddressStatus.StatusThruDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EmailAddressStatus.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EmailAddressStatus.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_EmailAddressStatus.EmailAddressSearch,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_EmailAddressStatus
 where stage_hash_mms_EmailAddressStatus.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_email_address_status records
set @insert_date_time = getdate()
insert into s_mms_email_address_status (
       bk_hash,
       email_address_status_id,
       email_address,
       status_from_date,
       status_thru_date,
       inserted_date_time,
       updated_date_time,
       email_address_search,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_email_address_status_inserts.bk_hash,
       #s_mms_email_address_status_inserts.email_address_status_id,
       #s_mms_email_address_status_inserts.email_address,
       #s_mms_email_address_status_inserts.status_from_date,
       #s_mms_email_address_status_inserts.status_thru_date,
       #s_mms_email_address_status_inserts.inserted_date_time,
       #s_mms_email_address_status_inserts.updated_date_time,
       #s_mms_email_address_status_inserts.email_address_search,
       case when s_mms_email_address_status.s_mms_email_address_status_id is null then isnull(#s_mms_email_address_status_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_email_address_status_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_email_address_status_inserts
  left join p_mms_email_address_status
    on #s_mms_email_address_status_inserts.bk_hash = p_mms_email_address_status.bk_hash
   and p_mms_email_address_status.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_email_address_status
    on p_mms_email_address_status.bk_hash = s_mms_email_address_status.bk_hash
   and p_mms_email_address_status.s_mms_email_address_status_id = s_mms_email_address_status.s_mms_email_address_status_id
 where s_mms_email_address_status.s_mms_email_address_status_id is null
    or (s_mms_email_address_status.s_mms_email_address_status_id is not null
        and s_mms_email_address_status.dv_hash <> #s_mms_email_address_status_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_email_address_status @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_email_address_status @current_dv_batch_id

end
