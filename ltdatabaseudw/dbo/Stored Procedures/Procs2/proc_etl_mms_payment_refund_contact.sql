CREATE PROC [dbo].[proc_etl_mms_payment_refund_contact] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PaymentRefundContact

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PaymentRefundContact (
       bk_hash,
       PaymentRefundContactID,
       FirstName,
       LastName,
       MiddleInit,
       PhoneAreaCode,
       PhoneNumber,
       AddressLine1,
       AddressLine2,
       City,
       Zip,
       ValCountryID,
       ValStateID,
       PaymentRefundID,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PaymentRefundContactID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PaymentRefundContactID,
       FirstName,
       LastName,
       MiddleInit,
       PhoneAreaCode,
       PhoneNumber,
       AddressLine1,
       AddressLine2,
       City,
       Zip,
       ValCountryID,
       ValStateID,
       PaymentRefundID,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_PaymentRefundContact.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_PaymentRefundContact
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_payment_refund_contact @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_payment_refund_contact (
       bk_hash,
       payment_refund_contact_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_PaymentRefundContact.bk_hash,
       stage_hash_mms_PaymentRefundContact.PaymentRefundContactID payment_refund_contact_id,
       isnull(cast(stage_hash_mms_PaymentRefundContact.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PaymentRefundContact
  left join h_mms_payment_refund_contact
    on stage_hash_mms_PaymentRefundContact.bk_hash = h_mms_payment_refund_contact.bk_hash
 where h_mms_payment_refund_contact_id is null
   and stage_hash_mms_PaymentRefundContact.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_payment_refund_contact
if object_id('tempdb..#l_mms_payment_refund_contact_inserts') is not null drop table #l_mms_payment_refund_contact_inserts
create table #l_mms_payment_refund_contact_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PaymentRefundContact.bk_hash,
       stage_hash_mms_PaymentRefundContact.PaymentRefundContactID payment_refund_contact_id,
       stage_hash_mms_PaymentRefundContact.ValCountryID val_country_id,
       stage_hash_mms_PaymentRefundContact.ValStateID val_state_id,
       stage_hash_mms_PaymentRefundContact.PaymentRefundID payment_refund_id,
       stage_hash_mms_PaymentRefundContact.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentRefundContact.PaymentRefundContactID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentRefundContact.ValCountryID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentRefundContact.ValStateID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentRefundContact.PaymentRefundID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PaymentRefundContact
 where stage_hash_mms_PaymentRefundContact.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_payment_refund_contact records
set @insert_date_time = getdate()
insert into l_mms_payment_refund_contact (
       bk_hash,
       payment_refund_contact_id,
       val_country_id,
       val_state_id,
       payment_refund_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_payment_refund_contact_inserts.bk_hash,
       #l_mms_payment_refund_contact_inserts.payment_refund_contact_id,
       #l_mms_payment_refund_contact_inserts.val_country_id,
       #l_mms_payment_refund_contact_inserts.val_state_id,
       #l_mms_payment_refund_contact_inserts.payment_refund_id,
       case when l_mms_payment_refund_contact.l_mms_payment_refund_contact_id is null then isnull(#l_mms_payment_refund_contact_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_payment_refund_contact_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_payment_refund_contact_inserts
  left join p_mms_payment_refund_contact
    on #l_mms_payment_refund_contact_inserts.bk_hash = p_mms_payment_refund_contact.bk_hash
   and p_mms_payment_refund_contact.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_payment_refund_contact
    on p_mms_payment_refund_contact.bk_hash = l_mms_payment_refund_contact.bk_hash
   and p_mms_payment_refund_contact.l_mms_payment_refund_contact_id = l_mms_payment_refund_contact.l_mms_payment_refund_contact_id
 where l_mms_payment_refund_contact.l_mms_payment_refund_contact_id is null
    or (l_mms_payment_refund_contact.l_mms_payment_refund_contact_id is not null
        and l_mms_payment_refund_contact.dv_hash <> #l_mms_payment_refund_contact_inserts.source_hash)

--calculate hash and lookup to current s_mms_payment_refund_contact
if object_id('tempdb..#s_mms_payment_refund_contact_inserts') is not null drop table #s_mms_payment_refund_contact_inserts
create table #s_mms_payment_refund_contact_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PaymentRefundContact.bk_hash,
       stage_hash_mms_PaymentRefundContact.PaymentRefundContactID payment_refund_contact_id,
       stage_hash_mms_PaymentRefundContact.FirstName first_name,
       stage_hash_mms_PaymentRefundContact.LastName last_name,
       stage_hash_mms_PaymentRefundContact.MiddleInit middle_init,
       stage_hash_mms_PaymentRefundContact.PhoneAreaCode phone_area_code,
       stage_hash_mms_PaymentRefundContact.PhoneNumber phone_number,
       stage_hash_mms_PaymentRefundContact.AddressLine1 address_line1,
       stage_hash_mms_PaymentRefundContact.AddressLine2 address_line2,
       stage_hash_mms_PaymentRefundContact.City city,
       stage_hash_mms_PaymentRefundContact.Zip zip,
       stage_hash_mms_PaymentRefundContact.InsertedDateTime inserted_date_time,
       stage_hash_mms_PaymentRefundContact.UpdatedDateTime updated_date_time,
       stage_hash_mms_PaymentRefundContact.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PaymentRefundContact.PaymentRefundContactID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefundContact.FirstName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefundContact.LastName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefundContact.MiddleInit,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefundContact.PhoneAreaCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefundContact.PhoneNumber,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefundContact.AddressLine1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefundContact.AddressLine2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefundContact.City,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PaymentRefundContact.Zip,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PaymentRefundContact.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PaymentRefundContact.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PaymentRefundContact
 where stage_hash_mms_PaymentRefundContact.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_payment_refund_contact records
set @insert_date_time = getdate()
insert into s_mms_payment_refund_contact (
       bk_hash,
       payment_refund_contact_id,
       first_name,
       last_name,
       middle_init,
       phone_area_code,
       phone_number,
       address_line1,
       address_line2,
       city,
       zip,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_payment_refund_contact_inserts.bk_hash,
       #s_mms_payment_refund_contact_inserts.payment_refund_contact_id,
       #s_mms_payment_refund_contact_inserts.first_name,
       #s_mms_payment_refund_contact_inserts.last_name,
       #s_mms_payment_refund_contact_inserts.middle_init,
       #s_mms_payment_refund_contact_inserts.phone_area_code,
       #s_mms_payment_refund_contact_inserts.phone_number,
       #s_mms_payment_refund_contact_inserts.address_line1,
       #s_mms_payment_refund_contact_inserts.address_line2,
       #s_mms_payment_refund_contact_inserts.city,
       #s_mms_payment_refund_contact_inserts.zip,
       #s_mms_payment_refund_contact_inserts.inserted_date_time,
       #s_mms_payment_refund_contact_inserts.updated_date_time,
       case when s_mms_payment_refund_contact.s_mms_payment_refund_contact_id is null then isnull(#s_mms_payment_refund_contact_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_payment_refund_contact_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_payment_refund_contact_inserts
  left join p_mms_payment_refund_contact
    on #s_mms_payment_refund_contact_inserts.bk_hash = p_mms_payment_refund_contact.bk_hash
   and p_mms_payment_refund_contact.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_payment_refund_contact
    on p_mms_payment_refund_contact.bk_hash = s_mms_payment_refund_contact.bk_hash
   and p_mms_payment_refund_contact.s_mms_payment_refund_contact_id = s_mms_payment_refund_contact.s_mms_payment_refund_contact_id
 where s_mms_payment_refund_contact.s_mms_payment_refund_contact_id is null
    or (s_mms_payment_refund_contact.s_mms_payment_refund_contact_id is not null
        and s_mms_payment_refund_contact.dv_hash <> #s_mms_payment_refund_contact_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_payment_refund_contact @current_dv_batch_id

end
