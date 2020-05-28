CREATE PROC [dbo].[proc_etl_mms_package_session] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PackageSession

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PackageSession (
       bk_hash,
       PackageSessionID,
       PackageID,
       CreatedDateTime,
       UTCCreatedDateTime,
       CreatedDateTimeZone,
       ModifiedDateTime,
       UTCModifiedDateTime,
       ModifiedDateTimeZone,
       DeliveredDateTime,
       UTCDeliveredDateTime,
       DeliveredDateTimeZone,
       CreatedEmployeeID,
       ModifiedEmployeeID,
       DeliveredEmployeeID,
       ClubID,
       SessionPrice,
       Comment,
       InsertedDateTime,
       UpdatedDateTime,
       MMSTranID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PackageSessionID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PackageSessionID,
       PackageID,
       CreatedDateTime,
       UTCCreatedDateTime,
       CreatedDateTimeZone,
       ModifiedDateTime,
       UTCModifiedDateTime,
       ModifiedDateTimeZone,
       DeliveredDateTime,
       UTCDeliveredDateTime,
       DeliveredDateTimeZone,
       CreatedEmployeeID,
       ModifiedEmployeeID,
       DeliveredEmployeeID,
       ClubID,
       SessionPrice,
       Comment,
       InsertedDateTime,
       UpdatedDateTime,
       MMSTranID,
       isnull(cast(stage_mms_PackageSession.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_PackageSession
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_package_session @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_package_session (
       bk_hash,
       package_session_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_PackageSession.bk_hash,
       stage_hash_mms_PackageSession.PackageSessionID package_session_id,
       isnull(cast(stage_hash_mms_PackageSession.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PackageSession
  left join h_mms_package_session
    on stage_hash_mms_PackageSession.bk_hash = h_mms_package_session.bk_hash
 where h_mms_package_session_id is null
   and stage_hash_mms_PackageSession.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_package_session
if object_id('tempdb..#l_mms_package_session_inserts') is not null drop table #l_mms_package_session_inserts
create table #l_mms_package_session_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PackageSession.bk_hash,
       stage_hash_mms_PackageSession.PackageSessionID package_session_id,
       stage_hash_mms_PackageSession.PackageID package_id,
       stage_hash_mms_PackageSession.CreatedEmployeeID created_employee_id,
       stage_hash_mms_PackageSession.ModifiedEmployeeID modified_employee_id,
       stage_hash_mms_PackageSession.DeliveredEmployeeID delivered_employee_id,
       stage_hash_mms_PackageSession.ClubID club_id,
       stage_hash_mms_PackageSession.MMSTranID mms_tran_id,
       isnull(cast(stage_hash_mms_PackageSession.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PackageSession.PackageSessionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageSession.PackageID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageSession.CreatedEmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageSession.ModifiedEmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageSession.DeliveredEmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageSession.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageSession.MMSTranID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PackageSession
 where stage_hash_mms_PackageSession.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_package_session records
set @insert_date_time = getdate()
insert into l_mms_package_session (
       bk_hash,
       package_session_id,
       package_id,
       created_employee_id,
       modified_employee_id,
       delivered_employee_id,
       club_id,
       mms_tran_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_package_session_inserts.bk_hash,
       #l_mms_package_session_inserts.package_session_id,
       #l_mms_package_session_inserts.package_id,
       #l_mms_package_session_inserts.created_employee_id,
       #l_mms_package_session_inserts.modified_employee_id,
       #l_mms_package_session_inserts.delivered_employee_id,
       #l_mms_package_session_inserts.club_id,
       #l_mms_package_session_inserts.mms_tran_id,
       case when l_mms_package_session.l_mms_package_session_id is null then isnull(#l_mms_package_session_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_package_session_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_package_session_inserts
  left join p_mms_package_session
    on #l_mms_package_session_inserts.bk_hash = p_mms_package_session.bk_hash
   and p_mms_package_session.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_package_session
    on p_mms_package_session.bk_hash = l_mms_package_session.bk_hash
   and p_mms_package_session.l_mms_package_session_id = l_mms_package_session.l_mms_package_session_id
 where l_mms_package_session.l_mms_package_session_id is null
    or (l_mms_package_session.l_mms_package_session_id is not null
        and l_mms_package_session.dv_hash <> #l_mms_package_session_inserts.source_hash)

--calculate hash and lookup to current s_mms_package_session
if object_id('tempdb..#s_mms_package_session_inserts') is not null drop table #s_mms_package_session_inserts
create table #s_mms_package_session_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PackageSession.bk_hash,
       stage_hash_mms_PackageSession.PackageSessionID package_session_id,
       stage_hash_mms_PackageSession.CreatedDateTime created_date_time,
       stage_hash_mms_PackageSession.UTCCreatedDateTime utc_created_date_time,
       stage_hash_mms_PackageSession.CreatedDateTimeZone created_date_time_zone,
       stage_hash_mms_PackageSession.ModifiedDateTime modified_date_time,
       stage_hash_mms_PackageSession.UTCModifiedDateTime utc_modified_date_time,
       stage_hash_mms_PackageSession.ModifiedDateTimeZone modified_date_time_zone,
       stage_hash_mms_PackageSession.DeliveredDateTime delivered_date_time,
       stage_hash_mms_PackageSession.UTCDeliveredDateTime utc_delivered_date_time,
       stage_hash_mms_PackageSession.DeliveredDateTimeZone delivered_date_time_zone,
       stage_hash_mms_PackageSession.SessionPrice session_price,
       stage_hash_mms_PackageSession.Comment comment,
       stage_hash_mms_PackageSession.InsertedDateTime inserted_date_time,
       stage_hash_mms_PackageSession.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_PackageSession.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PackageSession.PackageSessionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageSession.CreatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageSession.UTCCreatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PackageSession.CreatedDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageSession.ModifiedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageSession.UTCModifiedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PackageSession.ModifiedDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageSession.DeliveredDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageSession.UTCDeliveredDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PackageSession.DeliveredDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PackageSession.SessionPrice as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PackageSession.Comment,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageSession.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PackageSession.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PackageSession
 where stage_hash_mms_PackageSession.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_package_session records
set @insert_date_time = getdate()
insert into s_mms_package_session (
       bk_hash,
       package_session_id,
       created_date_time,
       utc_created_date_time,
       created_date_time_zone,
       modified_date_time,
       utc_modified_date_time,
       modified_date_time_zone,
       delivered_date_time,
       utc_delivered_date_time,
       delivered_date_time_zone,
       session_price,
       comment,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_package_session_inserts.bk_hash,
       #s_mms_package_session_inserts.package_session_id,
       #s_mms_package_session_inserts.created_date_time,
       #s_mms_package_session_inserts.utc_created_date_time,
       #s_mms_package_session_inserts.created_date_time_zone,
       #s_mms_package_session_inserts.modified_date_time,
       #s_mms_package_session_inserts.utc_modified_date_time,
       #s_mms_package_session_inserts.modified_date_time_zone,
       #s_mms_package_session_inserts.delivered_date_time,
       #s_mms_package_session_inserts.utc_delivered_date_time,
       #s_mms_package_session_inserts.delivered_date_time_zone,
       #s_mms_package_session_inserts.session_price,
       #s_mms_package_session_inserts.comment,
       #s_mms_package_session_inserts.inserted_date_time,
       #s_mms_package_session_inserts.updated_date_time,
       case when s_mms_package_session.s_mms_package_session_id is null then isnull(#s_mms_package_session_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_package_session_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_package_session_inserts
  left join p_mms_package_session
    on #s_mms_package_session_inserts.bk_hash = p_mms_package_session.bk_hash
   and p_mms_package_session.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_package_session
    on p_mms_package_session.bk_hash = s_mms_package_session.bk_hash
   and p_mms_package_session.s_mms_package_session_id = s_mms_package_session.s_mms_package_session_id
 where s_mms_package_session.s_mms_package_session_id is null
    or (s_mms_package_session.s_mms_package_session_id is not null
        and s_mms_package_session.dv_hash <> #s_mms_package_session_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_package_session @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_package_session @current_dv_batch_id

end
