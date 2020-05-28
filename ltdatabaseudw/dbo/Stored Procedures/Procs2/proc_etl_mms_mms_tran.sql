CREATE PROC [dbo].[proc_etl_mms_mms_tran] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_MMSTran

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MMSTran (
       bk_hash,
       MMSTranID,
       ClubID,
       MembershipID,
       MemberID,
       DrawerActivityID,
       TranVoidedID,
       ReasonCodeID,
       ValTranTypeID,
       DomainName,
       ReceiptNumber,
       ReceiptComment,
       PostDateTime,
       EmployeeID,
       TranDate,
       POSAmount,
       TranAmount,
       OriginalDrawerActivityID,
       ChangeRendered,
       UTCPostDateTime,
       PostDateTimeZone,
       InsertedDateTime,
       UpdatedDateTime,
       OriginalMMSTranID,
       TranEditedFlag,
       TranEditedEmployeeID,
       TranEditedDateTime,
       UTCTranEditedDateTime,
       TranEditedDateTimeZone,
       ReverseTranFlag,
       ComputerName,
       IPAddress,
       ValCurrencyCodeID,
       CorporatePartnerID,
       ConvertedAmount,
       ConvertedValCurrencyCodeID,
       ReimbursementProgramID,
       RefundedAsProductFlag,
       TransactionSource,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MMSTranID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MMSTranID,
       ClubID,
       MembershipID,
       MemberID,
       DrawerActivityID,
       TranVoidedID,
       ReasonCodeID,
       ValTranTypeID,
       DomainName,
       ReceiptNumber,
       ReceiptComment,
       PostDateTime,
       EmployeeID,
       TranDate,
       POSAmount,
       TranAmount,
       OriginalDrawerActivityID,
       ChangeRendered,
       UTCPostDateTime,
       PostDateTimeZone,
       InsertedDateTime,
       UpdatedDateTime,
       OriginalMMSTranID,
       TranEditedFlag,
       TranEditedEmployeeID,
       TranEditedDateTime,
       UTCTranEditedDateTime,
       TranEditedDateTimeZone,
       ReverseTranFlag,
       ComputerName,
       IPAddress,
       ValCurrencyCodeID,
       CorporatePartnerID,
       ConvertedAmount,
       ConvertedValCurrencyCodeID,
       ReimbursementProgramID,
       RefundedAsProductFlag,
       TransactionSource,
       isnull(cast(stage_mms_MMSTran.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_MMSTran
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_mms_tran @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_mms_tran (
       bk_hash,
       mms_tran_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_MMSTran.bk_hash,
       stage_hash_mms_MMSTran.MMSTranID mms_tran_id,
       isnull(cast(stage_hash_mms_MMSTran.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MMSTran
  left join h_mms_mms_tran
    on stage_hash_mms_MMSTran.bk_hash = h_mms_mms_tran.bk_hash
 where h_mms_mms_tran_id is null
   and stage_hash_mms_MMSTran.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_mms_tran
if object_id('tempdb..#l_mms_mms_tran_inserts') is not null drop table #l_mms_mms_tran_inserts
create table #l_mms_mms_tran_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MMSTran.bk_hash,
       stage_hash_mms_MMSTran.MMSTranID mms_tran_id,
       stage_hash_mms_MMSTran.ClubID club_id,
       stage_hash_mms_MMSTran.MembershipID membership_id,
       stage_hash_mms_MMSTran.MemberID member_id,
       stage_hash_mms_MMSTran.DrawerActivityID drawer_activity_id,
       stage_hash_mms_MMSTran.TranVoidedID tran_voided_id,
       stage_hash_mms_MMSTran.ReasonCodeID reason_code_id,
       stage_hash_mms_MMSTran.ValTranTypeID val_tran_type_id,
       stage_hash_mms_MMSTran.EmployeeID employee_id,
       stage_hash_mms_MMSTran.OriginalDrawerActivityID original_drawer_activity_id,
       stage_hash_mms_MMSTran.OriginalMMSTranID original_mms_tran_id,
       stage_hash_mms_MMSTran.TranEditedEmployeeID tran_edited_employee_id,
       stage_hash_mms_MMSTran.ValCurrencyCodeID val_currency_code_id,
       stage_hash_mms_MMSTran.CorporatePartnerID corporate_partner_id,
       stage_hash_mms_MMSTran.ConvertedValCurrencyCodeID converted_val_currency_code_id,
       stage_hash_mms_MMSTran.ReimbursementProgramID reimbursement_program_id,
       isnull(cast(stage_hash_mms_MMSTran.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.MMSTranID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.MemberID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.DrawerActivityID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.TranVoidedID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.ReasonCodeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.ValTranTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.EmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.OriginalDrawerActivityID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.OriginalMMSTranID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.TranEditedEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.ValCurrencyCodeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.CorporatePartnerID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.ConvertedValCurrencyCodeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.ReimbursementProgramID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MMSTran
 where stage_hash_mms_MMSTran.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_mms_tran records
set @insert_date_time = getdate()
insert into l_mms_mms_tran (
       bk_hash,
       mms_tran_id,
       club_id,
       membership_id,
       member_id,
       drawer_activity_id,
       tran_voided_id,
       reason_code_id,
       val_tran_type_id,
       employee_id,
       original_drawer_activity_id,
       original_mms_tran_id,
       tran_edited_employee_id,
       val_currency_code_id,
       corporate_partner_id,
       converted_val_currency_code_id,
       reimbursement_program_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_mms_tran_inserts.bk_hash,
       #l_mms_mms_tran_inserts.mms_tran_id,
       #l_mms_mms_tran_inserts.club_id,
       #l_mms_mms_tran_inserts.membership_id,
       #l_mms_mms_tran_inserts.member_id,
       #l_mms_mms_tran_inserts.drawer_activity_id,
       #l_mms_mms_tran_inserts.tran_voided_id,
       #l_mms_mms_tran_inserts.reason_code_id,
       #l_mms_mms_tran_inserts.val_tran_type_id,
       #l_mms_mms_tran_inserts.employee_id,
       #l_mms_mms_tran_inserts.original_drawer_activity_id,
       #l_mms_mms_tran_inserts.original_mms_tran_id,
       #l_mms_mms_tran_inserts.tran_edited_employee_id,
       #l_mms_mms_tran_inserts.val_currency_code_id,
       #l_mms_mms_tran_inserts.corporate_partner_id,
       #l_mms_mms_tran_inserts.converted_val_currency_code_id,
       #l_mms_mms_tran_inserts.reimbursement_program_id,
       case when l_mms_mms_tran.l_mms_mms_tran_id is null then isnull(#l_mms_mms_tran_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_mms_tran_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_mms_tran_inserts
  left join p_mms_mms_tran
    on #l_mms_mms_tran_inserts.bk_hash = p_mms_mms_tran.bk_hash
   and p_mms_mms_tran.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_mms_tran
    on p_mms_mms_tran.bk_hash = l_mms_mms_tran.bk_hash
   and p_mms_mms_tran.l_mms_mms_tran_id = l_mms_mms_tran.l_mms_mms_tran_id
 where l_mms_mms_tran.l_mms_mms_tran_id is null
    or (l_mms_mms_tran.l_mms_mms_tran_id is not null
        and l_mms_mms_tran.dv_hash <> #l_mms_mms_tran_inserts.source_hash)

--calculate hash and lookup to current s_mms_mms_tran
if object_id('tempdb..#s_mms_mms_tran_inserts') is not null drop table #s_mms_mms_tran_inserts
create table #s_mms_mms_tran_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MMSTran.bk_hash,
       stage_hash_mms_MMSTran.MMSTranID mms_tran_id,
       stage_hash_mms_MMSTran.DomainName domain_name,
       stage_hash_mms_MMSTran.ReceiptNumber receipt_number,
       stage_hash_mms_MMSTran.ReceiptComment receipt_comment,
       stage_hash_mms_MMSTran.PostDateTime post_date_time,
       stage_hash_mms_MMSTran.TranDate tran_date,
       stage_hash_mms_MMSTran.POSAmount pos_amount,
       stage_hash_mms_MMSTran.TranAmount tran_amount,
       stage_hash_mms_MMSTran.ChangeRendered change_rendered,
       stage_hash_mms_MMSTran.UTCPostDateTime utc_post_date_time,
       stage_hash_mms_MMSTran.PostDateTimeZone post_date_time_zone,
       stage_hash_mms_MMSTran.InsertedDateTime inserted_date_time,
       stage_hash_mms_MMSTran.UpdatedDateTime updated_date_time,
       stage_hash_mms_MMSTran.TranEditedFlag tran_edited_flag,
       stage_hash_mms_MMSTran.TranEditedDateTime tran_edited_date_time,
       stage_hash_mms_MMSTran.UTCTranEditedDateTime utc_tran_edited_date_time,
       stage_hash_mms_MMSTran.TranEditedDateTimeZone tran_edited_date_time_zone,
       stage_hash_mms_MMSTran.ReverseTranFlag reverse_tran_flag,
       stage_hash_mms_MMSTran.ComputerName computer_name,
       stage_hash_mms_MMSTran.IPAddress ip_address,
       stage_hash_mms_MMSTran.ConvertedAmount converted_amount,
       stage_hash_mms_MMSTran.RefundedAsProductFlag refunded_as_product_flag,
       stage_hash_mms_MMSTran.TransactionSource transaction_source,
       isnull(cast(stage_hash_mms_MMSTran.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.MMSTranID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MMSTran.DomainName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MMSTran.ReceiptNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MMSTran.ReceiptComment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MMSTran.PostDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MMSTran.TranDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.POSAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.TranAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.ChangeRendered as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MMSTran.UTCPostDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MMSTran.PostDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MMSTran.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MMSTran.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.TranEditedFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MMSTran.TranEditedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MMSTran.UTCTranEditedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MMSTran.TranEditedDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.ReverseTranFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MMSTran.ComputerName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MMSTran.IPAddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.ConvertedAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MMSTran.RefundedAsProductFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MMSTran.TransactionSource,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MMSTran
 where stage_hash_mms_MMSTran.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_mms_tran records
set @insert_date_time = getdate()
insert into s_mms_mms_tran (
       bk_hash,
       mms_tran_id,
       domain_name,
       receipt_number,
       receipt_comment,
       post_date_time,
       tran_date,
       pos_amount,
       tran_amount,
       change_rendered,
       utc_post_date_time,
       post_date_time_zone,
       inserted_date_time,
       updated_date_time,
       tran_edited_flag,
       tran_edited_date_time,
       utc_tran_edited_date_time,
       tran_edited_date_time_zone,
       reverse_tran_flag,
       computer_name,
       ip_address,
       converted_amount,
       refunded_as_product_flag,
       transaction_source,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_mms_tran_inserts.bk_hash,
       #s_mms_mms_tran_inserts.mms_tran_id,
       #s_mms_mms_tran_inserts.domain_name,
       #s_mms_mms_tran_inserts.receipt_number,
       #s_mms_mms_tran_inserts.receipt_comment,
       #s_mms_mms_tran_inserts.post_date_time,
       #s_mms_mms_tran_inserts.tran_date,
       #s_mms_mms_tran_inserts.pos_amount,
       #s_mms_mms_tran_inserts.tran_amount,
       #s_mms_mms_tran_inserts.change_rendered,
       #s_mms_mms_tran_inserts.utc_post_date_time,
       #s_mms_mms_tran_inserts.post_date_time_zone,
       #s_mms_mms_tran_inserts.inserted_date_time,
       #s_mms_mms_tran_inserts.updated_date_time,
       #s_mms_mms_tran_inserts.tran_edited_flag,
       #s_mms_mms_tran_inserts.tran_edited_date_time,
       #s_mms_mms_tran_inserts.utc_tran_edited_date_time,
       #s_mms_mms_tran_inserts.tran_edited_date_time_zone,
       #s_mms_mms_tran_inserts.reverse_tran_flag,
       #s_mms_mms_tran_inserts.computer_name,
       #s_mms_mms_tran_inserts.ip_address,
       #s_mms_mms_tran_inserts.converted_amount,
       #s_mms_mms_tran_inserts.refunded_as_product_flag,
       #s_mms_mms_tran_inserts.transaction_source,
       case when s_mms_mms_tran.s_mms_mms_tran_id is null then isnull(#s_mms_mms_tran_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_mms_tran_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_mms_tran_inserts
  left join p_mms_mms_tran
    on #s_mms_mms_tran_inserts.bk_hash = p_mms_mms_tran.bk_hash
   and p_mms_mms_tran.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_mms_tran
    on p_mms_mms_tran.bk_hash = s_mms_mms_tran.bk_hash
   and p_mms_mms_tran.s_mms_mms_tran_id = s_mms_mms_tran.s_mms_mms_tran_id
 where s_mms_mms_tran.s_mms_mms_tran_id is null
    or (s_mms_mms_tran.s_mms_mms_tran_id is not null
        and s_mms_mms_tran.dv_hash <> #s_mms_mms_tran_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_mms_tran @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_mms_tran @current_dv_batch_id

end
