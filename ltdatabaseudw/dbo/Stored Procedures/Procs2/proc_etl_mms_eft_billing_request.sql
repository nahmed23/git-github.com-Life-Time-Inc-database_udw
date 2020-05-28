﻿CREATE PROC [dbo].[proc_etl_mms_eft_billing_request] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_EFTBillingRequest

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_EFTBillingRequest (
       bk_hash,
       EFTBillingRequestID,
       FileName,
       ClubID,
       PersonID,
       ProductID,
       ProductPrice,
       Quantity,
       TotalAmount,
       PaymentRequestReference,
       CommissionEmployee,
       TransactionSource,
       ExternalItemID,
       ExternalPackageID,
       OriginalExternalItemID,
       SubscriptionID,
       mmsTranID,
       PackageID,
       ResponseCode,
       Message,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(EFTBillingRequestID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       EFTBillingRequestID,
       FileName,
       ClubID,
       PersonID,
       ProductID,
       ProductPrice,
       Quantity,
       TotalAmount,
       PaymentRequestReference,
       CommissionEmployee,
       TransactionSource,
       ExternalItemID,
       ExternalPackageID,
       OriginalExternalItemID,
       SubscriptionID,
       mmsTranID,
       PackageID,
       ResponseCode,
       Message,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_EFTBillingRequest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_EFTBillingRequest
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_eft_billing_request @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_eft_billing_request (
       bk_hash,
       eft_billing_request_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_EFTBillingRequest.bk_hash,
       stage_hash_mms_EFTBillingRequest.EFTBillingRequestID eft_billing_request_id,
       isnull(cast(stage_hash_mms_EFTBillingRequest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_EFTBillingRequest
  left join h_mms_eft_billing_request
    on stage_hash_mms_EFTBillingRequest.bk_hash = h_mms_eft_billing_request.bk_hash
 where h_mms_eft_billing_request_id is null
   and stage_hash_mms_EFTBillingRequest.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_eft_billing_request
if object_id('tempdb..#l_mms_eft_billing_request_inserts') is not null drop table #l_mms_eft_billing_request_inserts
create table #l_mms_eft_billing_request_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_EFTBillingRequest.bk_hash,
       stage_hash_mms_EFTBillingRequest.EFTBillingRequestID eft_billing_request_id,
       stage_hash_mms_EFTBillingRequest.ClubID club_id,
       stage_hash_mms_EFTBillingRequest.PersonID person_id,
       stage_hash_mms_EFTBillingRequest.ProductID product_id,
       stage_hash_mms_EFTBillingRequest.ExternalItemID external_item_id,
       stage_hash_mms_EFTBillingRequest.ExternalPackageID external_package_id,
       stage_hash_mms_EFTBillingRequest.OriginalExternalItemID original_external_item_id,
       stage_hash_mms_EFTBillingRequest.SubscriptionID subscription_id,
       stage_hash_mms_EFTBillingRequest.mmsTranID mms_tran_id,
       stage_hash_mms_EFTBillingRequest.PackageID package_id,
       isnull(cast(stage_hash_mms_EFTBillingRequest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_EFTBillingRequest.EFTBillingRequestID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.ClubID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.PersonID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.ProductID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.ExternalItemID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.ExternalPackageID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.OriginalExternalItemID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.SubscriptionID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFTBillingRequest.mmsTranID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EFTBillingRequest.PackageID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_EFTBillingRequest
 where stage_hash_mms_EFTBillingRequest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_eft_billing_request records
set @insert_date_time = getdate()
insert into l_mms_eft_billing_request (
       bk_hash,
       eft_billing_request_id,
       club_id,
       person_id,
       product_id,
       external_item_id,
       external_package_id,
       original_external_item_id,
       subscription_id,
       mms_tran_id,
       package_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_eft_billing_request_inserts.bk_hash,
       #l_mms_eft_billing_request_inserts.eft_billing_request_id,
       #l_mms_eft_billing_request_inserts.club_id,
       #l_mms_eft_billing_request_inserts.person_id,
       #l_mms_eft_billing_request_inserts.product_id,
       #l_mms_eft_billing_request_inserts.external_item_id,
       #l_mms_eft_billing_request_inserts.external_package_id,
       #l_mms_eft_billing_request_inserts.original_external_item_id,
       #l_mms_eft_billing_request_inserts.subscription_id,
       #l_mms_eft_billing_request_inserts.mms_tran_id,
       #l_mms_eft_billing_request_inserts.package_id,
       case when l_mms_eft_billing_request.l_mms_eft_billing_request_id is null then isnull(#l_mms_eft_billing_request_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_eft_billing_request_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_eft_billing_request_inserts
  left join p_mms_eft_billing_request
    on #l_mms_eft_billing_request_inserts.bk_hash = p_mms_eft_billing_request.bk_hash
   and p_mms_eft_billing_request.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_eft_billing_request
    on p_mms_eft_billing_request.bk_hash = l_mms_eft_billing_request.bk_hash
   and p_mms_eft_billing_request.l_mms_eft_billing_request_id = l_mms_eft_billing_request.l_mms_eft_billing_request_id
 where l_mms_eft_billing_request.l_mms_eft_billing_request_id is null
    or (l_mms_eft_billing_request.l_mms_eft_billing_request_id is not null
        and l_mms_eft_billing_request.dv_hash <> #l_mms_eft_billing_request_inserts.source_hash)

--calculate hash and lookup to current s_mms_eft_billing_request
if object_id('tempdb..#s_mms_eft_billing_request_inserts') is not null drop table #s_mms_eft_billing_request_inserts
create table #s_mms_eft_billing_request_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_EFTBillingRequest.bk_hash,
       stage_hash_mms_EFTBillingRequest.EFTBillingRequestID eft_billing_request_id,
       stage_hash_mms_EFTBillingRequest.FileName file_name,
       stage_hash_mms_EFTBillingRequest.ProductPrice product_price,
       stage_hash_mms_EFTBillingRequest.Quantity quantity,
       stage_hash_mms_EFTBillingRequest.TotalAmount total_amount,
       stage_hash_mms_EFTBillingRequest.PaymentRequestReference payment_request_reference,
       stage_hash_mms_EFTBillingRequest.CommissionEmployee commission_employee,
       stage_hash_mms_EFTBillingRequest.TransactionSource transaction_source,
       stage_hash_mms_EFTBillingRequest.ResponseCode response_code,
       stage_hash_mms_EFTBillingRequest.Message message,
       stage_hash_mms_EFTBillingRequest.InsertedDateTime inserted_date_time,
       stage_hash_mms_EFTBillingRequest.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_EFTBillingRequest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_EFTBillingRequest.EFTBillingRequestID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.FileName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.ProductPrice,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.Quantity,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.TotalAmount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.PaymentRequestReference,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.CommissionEmployee,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.TransactionSource,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.ResponseCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_EFTBillingRequest.Message,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EFTBillingRequest.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EFTBillingRequest.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_EFTBillingRequest
 where stage_hash_mms_EFTBillingRequest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_eft_billing_request records
set @insert_date_time = getdate()
insert into s_mms_eft_billing_request (
       bk_hash,
       eft_billing_request_id,
       file_name,
       product_price,
       quantity,
       total_amount,
       payment_request_reference,
       commission_employee,
       transaction_source,
       response_code,
       message,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_eft_billing_request_inserts.bk_hash,
       #s_mms_eft_billing_request_inserts.eft_billing_request_id,
       #s_mms_eft_billing_request_inserts.file_name,
       #s_mms_eft_billing_request_inserts.product_price,
       #s_mms_eft_billing_request_inserts.quantity,
       #s_mms_eft_billing_request_inserts.total_amount,
       #s_mms_eft_billing_request_inserts.payment_request_reference,
       #s_mms_eft_billing_request_inserts.commission_employee,
       #s_mms_eft_billing_request_inserts.transaction_source,
       #s_mms_eft_billing_request_inserts.response_code,
       #s_mms_eft_billing_request_inserts.message,
       #s_mms_eft_billing_request_inserts.inserted_date_time,
       #s_mms_eft_billing_request_inserts.updated_date_time,
       case when s_mms_eft_billing_request.s_mms_eft_billing_request_id is null then isnull(#s_mms_eft_billing_request_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_eft_billing_request_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_eft_billing_request_inserts
  left join p_mms_eft_billing_request
    on #s_mms_eft_billing_request_inserts.bk_hash = p_mms_eft_billing_request.bk_hash
   and p_mms_eft_billing_request.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_eft_billing_request
    on p_mms_eft_billing_request.bk_hash = s_mms_eft_billing_request.bk_hash
   and p_mms_eft_billing_request.s_mms_eft_billing_request_id = s_mms_eft_billing_request.s_mms_eft_billing_request_id
 where s_mms_eft_billing_request.s_mms_eft_billing_request_id is null
    or (s_mms_eft_billing_request.s_mms_eft_billing_request_id is not null
        and s_mms_eft_billing_request.dv_hash <> #s_mms_eft_billing_request_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_eft_billing_request @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_eft_billing_request @current_dv_batch_id

end
