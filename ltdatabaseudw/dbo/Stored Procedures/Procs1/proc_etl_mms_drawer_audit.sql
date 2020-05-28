CREATE PROC [dbo].[proc_etl_mms_drawer_audit] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_DrawerAudit

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_DrawerAudit (
       bk_hash,
       DrawerAuditID,
       EmployeeOneID,
       DrawerActivityID,
       Amount,
       AuditDateTime,
       ValDrawerAuditTypeID,
       UTCAuditDateTime,
       AuditDateTimeZone,
       InsertedDateTime,
       UpdatedDateTime,
       ValPaymentTypeID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(DrawerAuditID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       DrawerAuditID,
       EmployeeOneID,
       DrawerActivityID,
       Amount,
       AuditDateTime,
       ValDrawerAuditTypeID,
       UTCAuditDateTime,
       AuditDateTimeZone,
       InsertedDateTime,
       UpdatedDateTime,
       ValPaymentTypeID,
       isnull(cast(stage_mms_DrawerAudit.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_DrawerAudit
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_drawer_audit @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_drawer_audit (
       bk_hash,
       drawer_audit_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_DrawerAudit.bk_hash,
       stage_hash_mms_DrawerAudit.DrawerAuditID drawer_audit_id,
       isnull(cast(stage_hash_mms_DrawerAudit.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_DrawerAudit
  left join h_mms_drawer_audit
    on stage_hash_mms_DrawerAudit.bk_hash = h_mms_drawer_audit.bk_hash
 where h_mms_drawer_audit_id is null
   and stage_hash_mms_DrawerAudit.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_drawer_audit
if object_id('tempdb..#l_mms_drawer_audit_inserts') is not null drop table #l_mms_drawer_audit_inserts
create table #l_mms_drawer_audit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_DrawerAudit.bk_hash,
       stage_hash_mms_DrawerAudit.DrawerAuditID drawer_audit_id,
       stage_hash_mms_DrawerAudit.EmployeeOneID employee_one_id,
       stage_hash_mms_DrawerAudit.DrawerActivityID drawer_activity_id,
       stage_hash_mms_DrawerAudit.ValDrawerAuditTypeID val_drawer_audit_type_id,
       stage_hash_mms_DrawerAudit.ValPaymentTypeID val_payment_type_id,
       stage_hash_mms_DrawerAudit.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerAudit.DrawerAuditID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerAudit.EmployeeOneID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerAudit.DrawerActivityID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerAudit.ValDrawerAuditTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerAudit.ValPaymentTypeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_DrawerAudit
 where stage_hash_mms_DrawerAudit.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_drawer_audit records
set @insert_date_time = getdate()
insert into l_mms_drawer_audit (
       bk_hash,
       drawer_audit_id,
       employee_one_id,
       drawer_activity_id,
       val_drawer_audit_type_id,
       val_payment_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_drawer_audit_inserts.bk_hash,
       #l_mms_drawer_audit_inserts.drawer_audit_id,
       #l_mms_drawer_audit_inserts.employee_one_id,
       #l_mms_drawer_audit_inserts.drawer_activity_id,
       #l_mms_drawer_audit_inserts.val_drawer_audit_type_id,
       #l_mms_drawer_audit_inserts.val_payment_type_id,
       case when l_mms_drawer_audit.l_mms_drawer_audit_id is null then isnull(#l_mms_drawer_audit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_drawer_audit_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_drawer_audit_inserts
  left join p_mms_drawer_audit
    on #l_mms_drawer_audit_inserts.bk_hash = p_mms_drawer_audit.bk_hash
   and p_mms_drawer_audit.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_drawer_audit
    on p_mms_drawer_audit.bk_hash = l_mms_drawer_audit.bk_hash
   and p_mms_drawer_audit.l_mms_drawer_audit_id = l_mms_drawer_audit.l_mms_drawer_audit_id
 where l_mms_drawer_audit.l_mms_drawer_audit_id is null
    or (l_mms_drawer_audit.l_mms_drawer_audit_id is not null
        and l_mms_drawer_audit.dv_hash <> #l_mms_drawer_audit_inserts.source_hash)

--calculate hash and lookup to current s_mms_drawer_audit
if object_id('tempdb..#s_mms_drawer_audit_inserts') is not null drop table #s_mms_drawer_audit_inserts
create table #s_mms_drawer_audit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_DrawerAudit.bk_hash,
       stage_hash_mms_DrawerAudit.DrawerAuditID drawer_audit_id,
       stage_hash_mms_DrawerAudit.Amount amount,
       stage_hash_mms_DrawerAudit.AuditDateTime audit_date_time,
       stage_hash_mms_DrawerAudit.UTCAuditDateTime utc_audit_date_time,
       stage_hash_mms_DrawerAudit.AuditDateTimeZone audit_date_time_zone,
       stage_hash_mms_DrawerAudit.InsertedDateTime inserted_date_time,
       stage_hash_mms_DrawerAudit.UpdatedDateTime updated_date_time,
       stage_hash_mms_DrawerAudit.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerAudit.DrawerAuditID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerAudit.Amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerAudit.AuditDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerAudit.UTCAuditDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_DrawerAudit.AuditDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerAudit.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerAudit.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_DrawerAudit
 where stage_hash_mms_DrawerAudit.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_drawer_audit records
set @insert_date_time = getdate()
insert into s_mms_drawer_audit (
       bk_hash,
       drawer_audit_id,
       amount,
       audit_date_time,
       utc_audit_date_time,
       audit_date_time_zone,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_drawer_audit_inserts.bk_hash,
       #s_mms_drawer_audit_inserts.drawer_audit_id,
       #s_mms_drawer_audit_inserts.amount,
       #s_mms_drawer_audit_inserts.audit_date_time,
       #s_mms_drawer_audit_inserts.utc_audit_date_time,
       #s_mms_drawer_audit_inserts.audit_date_time_zone,
       #s_mms_drawer_audit_inserts.inserted_date_time,
       #s_mms_drawer_audit_inserts.updated_date_time,
       case when s_mms_drawer_audit.s_mms_drawer_audit_id is null then isnull(#s_mms_drawer_audit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_drawer_audit_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_drawer_audit_inserts
  left join p_mms_drawer_audit
    on #s_mms_drawer_audit_inserts.bk_hash = p_mms_drawer_audit.bk_hash
   and p_mms_drawer_audit.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_drawer_audit
    on p_mms_drawer_audit.bk_hash = s_mms_drawer_audit.bk_hash
   and p_mms_drawer_audit.s_mms_drawer_audit_id = s_mms_drawer_audit.s_mms_drawer_audit_id
 where s_mms_drawer_audit.s_mms_drawer_audit_id is null
    or (s_mms_drawer_audit.s_mms_drawer_audit_id is not null
        and s_mms_drawer_audit.dv_hash <> #s_mms_drawer_audit_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_drawer_audit @current_dv_batch_id

end
