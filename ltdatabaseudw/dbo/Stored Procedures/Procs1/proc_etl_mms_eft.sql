CREATE PROC [dbo].[proc_etl_mms_eft] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_EFT

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_EFT (
       bk_hash,
       EFTID,
       MembershipID,
       ValEFTStatusID,
       EFTReturnCodeID,
       AccountNumber,
       AccountOwner,
       RoutingNumber,
       ExpirationDate,
       EFTDate,
       PaymentID,
       ReturnCode,
       ValEFTTypeID,
       EFTAmount,
       ValPaymentTypeID,
       MemberID,
       Job_Task_ID,
       MaskedAccountNumber,
       MaskedAccountNumber64,
       InsertedDateTime,
       UpdatedDateTime,
       DuesAmountUsedForProducts,
       EFTAmountProducts,
       OrderNumber,
       Token,
       ValEFTAccountTypeID,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(EFTID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       EFTID,
       MembershipID,
       ValEFTStatusID,
       EFTReturnCodeID,
       AccountNumber,
       AccountOwner,
       RoutingNumber,
       ExpirationDate,
       EFTDate,
       PaymentID,
       ReturnCode,
       ValEFTTypeID,
       EFTAmount,
       ValPaymentTypeID,
       MemberID,
       Job_Task_ID,
       MaskedAccountNumber,
       MaskedAccountNumber64,
       InsertedDateTime,
       UpdatedDateTime,
       DuesAmountUsedForProducts,
       EFTAmountProducts,
       OrderNumber,
       Token,
       ValEFTAccountTypeID,
       isnull(cast(stage_mms_EFT.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_EFT
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_eft @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_eft (
       bk_hash,
       eft_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_EFT.bk_hash,
       stage_hash_mms_EFT.EFTID eft_id,
       isnull(cast(stage_hash_mms_EFT.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_EFT
  left join h_mms_eft
    on stage_hash_mms_EFT.bk_hash = h_mms_eft.bk_hash
 where h_mms_eft_id is null
   and stage_hash_mms_EFT.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_eft
if object_id('tempdb..#l_mms_eft_inserts') is not null drop table #l_mms_eft_inserts
create table #l_mms_eft_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_EFT.bk_hash,
       stage_hash_mms_EFT.EFTID eft_id,
       stage_hash_mms_EFT.MembershipID membership_id,
       stage_hash_mms_EFT.ValEFTStatusID val_eft_status_id,
       stage_hash_mms_EFT.EFTReturnCodeID eft_return_code_id,
       stage_hash_mms_EFT.PaymentID payment_id,
       stage_hash_mms_EFT.ValEFTTypeID val_eft_type_id,
       stage_hash_mms_EFT.ValPaymentTypeID val_payment_type_id,
       stage_hash_mms_EFT.MemberID member_id,
       stage_hash_mms_EFT.Job_Task_ID job_task_id,
       isnull(cast(stage_hash_mms_EFT.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.EFTID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.ValEFTStatusID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.EFTReturnCodeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.PaymentID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.ValEFTTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.ValPaymentTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.MemberID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.Job_Task_ID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_EFT
 where stage_hash_mms_EFT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_eft records
set @insert_date_time = getdate()
insert into l_mms_eft (
       bk_hash,
       eft_id,
       membership_id,
       val_eft_status_id,
       eft_return_code_id,
       payment_id,
       val_eft_type_id,
       val_payment_type_id,
       member_id,
       job_task_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_eft_inserts.bk_hash,
       #l_mms_eft_inserts.eft_id,
       #l_mms_eft_inserts.membership_id,
       #l_mms_eft_inserts.val_eft_status_id,
       #l_mms_eft_inserts.eft_return_code_id,
       #l_mms_eft_inserts.payment_id,
       #l_mms_eft_inserts.val_eft_type_id,
       #l_mms_eft_inserts.val_payment_type_id,
       #l_mms_eft_inserts.member_id,
       #l_mms_eft_inserts.job_task_id,
       case when l_mms_eft.l_mms_eft_id is null then isnull(#l_mms_eft_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_eft_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_eft_inserts
  left join p_mms_eft
    on #l_mms_eft_inserts.bk_hash = p_mms_eft.bk_hash
   and p_mms_eft.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_eft
    on p_mms_eft.bk_hash = l_mms_eft.bk_hash
   and p_mms_eft.l_mms_eft_id = l_mms_eft.l_mms_eft_id
 where l_mms_eft.l_mms_eft_id is null
    or (l_mms_eft.l_mms_eft_id is not null
        and l_mms_eft.dv_hash <> #l_mms_eft_inserts.source_hash)

--calculate hash and lookup to current l_mms_eft_1
if object_id('tempdb..#l_mms_eft_1_inserts') is not null drop table #l_mms_eft_1_inserts
create table #l_mms_eft_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_EFT.bk_hash,
       stage_hash_mms_EFT.EFTID eft_id,
       stage_hash_mms_EFT.ValEFTAccountTypeID val_eft_account_type_id,
       isnull(cast(stage_hash_mms_EFT.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.EFTID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.ValEFTAccountTypeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_EFT
 where stage_hash_mms_EFT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_eft_1 records
set @insert_date_time = getdate()
insert into l_mms_eft_1 (
       bk_hash,
       eft_id,
       val_eft_account_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_eft_1_inserts.bk_hash,
       #l_mms_eft_1_inserts.eft_id,
       #l_mms_eft_1_inserts.val_eft_account_type_id,
       case when l_mms_eft_1.l_mms_eft_1_id is null then isnull(#l_mms_eft_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_eft_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_eft_1_inserts
  left join p_mms_eft
    on #l_mms_eft_1_inserts.bk_hash = p_mms_eft.bk_hash
   and p_mms_eft.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_eft_1
    on p_mms_eft.bk_hash = l_mms_eft_1.bk_hash
   and p_mms_eft.l_mms_eft_1_id = l_mms_eft_1.l_mms_eft_1_id
 where l_mms_eft_1.l_mms_eft_1_id is null
    or (l_mms_eft_1.l_mms_eft_1_id is not null
        and l_mms_eft_1.dv_hash <> #l_mms_eft_1_inserts.source_hash)

--calculate hash and lookup to current s_mms_eft
if object_id('tempdb..#s_mms_eft_inserts') is not null drop table #s_mms_eft_inserts
create table #s_mms_eft_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_EFT.bk_hash,
       stage_hash_mms_EFT.EFTID eft_id,
       stage_hash_mms_EFT.AccountNumber account_number,
       stage_hash_mms_EFT.AccountOwner account_owner,
       stage_hash_mms_EFT.RoutingNumber routing_number,
       stage_hash_mms_EFT.ExpirationDate expiration_date,
       stage_hash_mms_EFT.EFTDate eft_date,
       stage_hash_mms_EFT.ReturnCode return_code,
       stage_hash_mms_EFT.EFTAmount eft_amount,
       stage_hash_mms_EFT.MaskedAccountNumber masked_account_number,
       stage_hash_mms_EFT.MaskedAccountNumber64 masked_account_number64,
       stage_hash_mms_EFT.InsertedDateTime inserted_date_time,
       stage_hash_mms_EFT.UpdatedDateTime updated_date_time,
       stage_hash_mms_EFT.DuesAmountUsedForProducts dues_amount_used_for_products,
       stage_hash_mms_EFT.EFTAmountProducts eft_amount_products,
       stage_hash_mms_EFT.OrderNumber order_number,
       isnull(cast(stage_hash_mms_EFT.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.EFTID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFT.AccountNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFT.AccountOwner,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFT.RoutingNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EFT.ExpirationDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EFT.EFTDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFT.ReturnCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.EFTAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFT.MaskedAccountNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFT.MaskedAccountNumber64,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EFT.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EFT.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.DuesAmountUsedForProducts as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.EFTAmountProducts as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFT.OrderNumber,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_EFT
 where stage_hash_mms_EFT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_eft records
set @insert_date_time = getdate()
insert into s_mms_eft (
       bk_hash,
       eft_id,
       account_number,
       account_owner,
       routing_number,
       expiration_date,
       eft_date,
       return_code,
       eft_amount,
       masked_account_number,
       masked_account_number64,
       inserted_date_time,
       updated_date_time,
       dues_amount_used_for_products,
       eft_amount_products,
       order_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_eft_inserts.bk_hash,
       #s_mms_eft_inserts.eft_id,
       #s_mms_eft_inserts.account_number,
       #s_mms_eft_inserts.account_owner,
       #s_mms_eft_inserts.routing_number,
       #s_mms_eft_inserts.expiration_date,
       #s_mms_eft_inserts.eft_date,
       #s_mms_eft_inserts.return_code,
       #s_mms_eft_inserts.eft_amount,
       #s_mms_eft_inserts.masked_account_number,
       #s_mms_eft_inserts.masked_account_number64,
       #s_mms_eft_inserts.inserted_date_time,
       #s_mms_eft_inserts.updated_date_time,
       #s_mms_eft_inserts.dues_amount_used_for_products,
       #s_mms_eft_inserts.eft_amount_products,
       #s_mms_eft_inserts.order_number,
       case when s_mms_eft.s_mms_eft_id is null then isnull(#s_mms_eft_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_eft_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_eft_inserts
  left join p_mms_eft
    on #s_mms_eft_inserts.bk_hash = p_mms_eft.bk_hash
   and p_mms_eft.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_eft
    on p_mms_eft.bk_hash = s_mms_eft.bk_hash
   and p_mms_eft.s_mms_eft_id = s_mms_eft.s_mms_eft_id
 where s_mms_eft.s_mms_eft_id is null
    or (s_mms_eft.s_mms_eft_id is not null
        and s_mms_eft.dv_hash <> #s_mms_eft_inserts.source_hash)

--calculate hash and lookup to current s_mms_eft_1
if object_id('tempdb..#s_mms_eft_1_inserts') is not null drop table #s_mms_eft_1_inserts
create table #s_mms_eft_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_EFT.bk_hash,
       stage_hash_mms_EFT.EFTID eft_id,
       stage_hash_mms_EFT.Token token,
       isnull(cast(stage_hash_mms_EFT.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_EFT.EFTID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFT.Token,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_EFT
 where stage_hash_mms_EFT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_eft_1 records
set @insert_date_time = getdate()
insert into s_mms_eft_1 (
       bk_hash,
       eft_id,
       token,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_eft_1_inserts.bk_hash,
       #s_mms_eft_1_inserts.eft_id,
       #s_mms_eft_1_inserts.token,
       case when s_mms_eft_1.s_mms_eft_1_id is null then isnull(#s_mms_eft_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_eft_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_eft_1_inserts
  left join p_mms_eft
    on #s_mms_eft_1_inserts.bk_hash = p_mms_eft.bk_hash
   and p_mms_eft.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_eft_1
    on p_mms_eft.bk_hash = s_mms_eft_1.bk_hash
   and p_mms_eft.s_mms_eft_1_id = s_mms_eft_1.s_mms_eft_1_id
 where s_mms_eft_1.s_mms_eft_1_id is null
    or (s_mms_eft_1.s_mms_eft_1_id is not null
        and s_mms_eft_1.dv_hash <> #s_mms_eft_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_eft @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_eft @current_dv_batch_id

end
