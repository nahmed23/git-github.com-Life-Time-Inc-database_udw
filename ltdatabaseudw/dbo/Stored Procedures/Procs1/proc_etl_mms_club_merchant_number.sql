CREATE PROC [dbo].[proc_etl_mms_club_merchant_number] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ClubMerchantNumber

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ClubMerchantNumber (
       bk_hash,
       ClubMerchantNumberID,
       ClubID,
       MerchantNumber,
       Description,
       ValBusinessAreaID,
       InsertedDateTime,
       UpdatedDateTime,
       MerchantLocationNumber,
       AutoReconcileFlag,
       ValCurrencyCodeID,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubMerchantNumberID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ClubMerchantNumberID,
       ClubID,
       MerchantNumber,
       Description,
       ValBusinessAreaID,
       InsertedDateTime,
       UpdatedDateTime,
       MerchantLocationNumber,
       AutoReconcileFlag,
       ValCurrencyCodeID,
       isnull(cast(stage_mms_ClubMerchantNumber.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_ClubMerchantNumber
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_club_merchant_number @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_club_merchant_number (
       bk_hash,
       club_merchant_number_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_ClubMerchantNumber.bk_hash,
       stage_hash_mms_ClubMerchantNumber.ClubMerchantNumberID club_merchant_number_id,
       isnull(cast(stage_hash_mms_ClubMerchantNumber.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ClubMerchantNumber
  left join h_mms_club_merchant_number
    on stage_hash_mms_ClubMerchantNumber.bk_hash = h_mms_club_merchant_number.bk_hash
 where h_mms_club_merchant_number_id is null
   and stage_hash_mms_ClubMerchantNumber.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_club_merchant_number
if object_id('tempdb..#l_mms_club_merchant_number_inserts') is not null drop table #l_mms_club_merchant_number_inserts
create table #l_mms_club_merchant_number_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ClubMerchantNumber.bk_hash,
       stage_hash_mms_ClubMerchantNumber.ClubMerchantNumberID club_merchant_number_id,
       stage_hash_mms_ClubMerchantNumber.ClubID club_id,
       stage_hash_mms_ClubMerchantNumber.MerchantNumber merchant_number,
       stage_hash_mms_ClubMerchantNumber.ValBusinessAreaID val_business_area_id,
       stage_hash_mms_ClubMerchantNumber.ValCurrencyCodeID val_currency_code_id,
       isnull(cast(stage_hash_mms_ClubMerchantNumber.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ClubMerchantNumber.ClubMerchantNumberID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubMerchantNumber.ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubMerchantNumber.MerchantNumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubMerchantNumber.ValBusinessAreaID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubMerchantNumber.ValCurrencyCodeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ClubMerchantNumber
 where stage_hash_mms_ClubMerchantNumber.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_club_merchant_number records
set @insert_date_time = getdate()
insert into l_mms_club_merchant_number (
       bk_hash,
       club_merchant_number_id,
       club_id,
       merchant_number,
       val_business_area_id,
       val_currency_code_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_club_merchant_number_inserts.bk_hash,
       #l_mms_club_merchant_number_inserts.club_merchant_number_id,
       #l_mms_club_merchant_number_inserts.club_id,
       #l_mms_club_merchant_number_inserts.merchant_number,
       #l_mms_club_merchant_number_inserts.val_business_area_id,
       #l_mms_club_merchant_number_inserts.val_currency_code_id,
       case when l_mms_club_merchant_number.l_mms_club_merchant_number_id is null then isnull(#l_mms_club_merchant_number_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_club_merchant_number_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_club_merchant_number_inserts
  left join p_mms_club_merchant_number
    on #l_mms_club_merchant_number_inserts.bk_hash = p_mms_club_merchant_number.bk_hash
   and p_mms_club_merchant_number.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_club_merchant_number
    on p_mms_club_merchant_number.bk_hash = l_mms_club_merchant_number.bk_hash
   and p_mms_club_merchant_number.l_mms_club_merchant_number_id = l_mms_club_merchant_number.l_mms_club_merchant_number_id
 where l_mms_club_merchant_number.l_mms_club_merchant_number_id is null
    or (l_mms_club_merchant_number.l_mms_club_merchant_number_id is not null
        and l_mms_club_merchant_number.dv_hash <> #l_mms_club_merchant_number_inserts.source_hash)

--calculate hash and lookup to current s_mms_club_merchant_number
if object_id('tempdb..#s_mms_club_merchant_number_inserts') is not null drop table #s_mms_club_merchant_number_inserts
create table #s_mms_club_merchant_number_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ClubMerchantNumber.bk_hash,
       stage_hash_mms_ClubMerchantNumber.ClubMerchantNumberID club_merchant_number_id,
       stage_hash_mms_ClubMerchantNumber.Description description,
       stage_hash_mms_ClubMerchantNumber.InsertedDateTime inserted_date_time,
       stage_hash_mms_ClubMerchantNumber.UpdatedDateTime updated_date_time,
       stage_hash_mms_ClubMerchantNumber.MerchantLocationNumber merchant_location_number,
       stage_hash_mms_ClubMerchantNumber.AutoReconcileFlag auto_reconcile_flag,
       isnull(cast(stage_hash_mms_ClubMerchantNumber.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ClubMerchantNumber.ClubMerchantNumberID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_ClubMerchantNumber.Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ClubMerchantNumber.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ClubMerchantNumber.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_ClubMerchantNumber.MerchantLocationNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubMerchantNumber.AutoReconcileFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ClubMerchantNumber
 where stage_hash_mms_ClubMerchantNumber.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_club_merchant_number records
set @insert_date_time = getdate()
insert into s_mms_club_merchant_number (
       bk_hash,
       club_merchant_number_id,
       description,
       inserted_date_time,
       updated_date_time,
       merchant_location_number,
       auto_reconcile_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_club_merchant_number_inserts.bk_hash,
       #s_mms_club_merchant_number_inserts.club_merchant_number_id,
       #s_mms_club_merchant_number_inserts.description,
       #s_mms_club_merchant_number_inserts.inserted_date_time,
       #s_mms_club_merchant_number_inserts.updated_date_time,
       #s_mms_club_merchant_number_inserts.merchant_location_number,
       #s_mms_club_merchant_number_inserts.auto_reconcile_flag,
       case when s_mms_club_merchant_number.s_mms_club_merchant_number_id is null then isnull(#s_mms_club_merchant_number_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_club_merchant_number_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_club_merchant_number_inserts
  left join p_mms_club_merchant_number
    on #s_mms_club_merchant_number_inserts.bk_hash = p_mms_club_merchant_number.bk_hash
   and p_mms_club_merchant_number.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_club_merchant_number
    on p_mms_club_merchant_number.bk_hash = s_mms_club_merchant_number.bk_hash
   and p_mms_club_merchant_number.s_mms_club_merchant_number_id = s_mms_club_merchant_number.s_mms_club_merchant_number_id
 where s_mms_club_merchant_number.s_mms_club_merchant_number_id is null
    or (s_mms_club_merchant_number.s_mms_club_merchant_number_id is not null
        and s_mms_club_merchant_number.dv_hash <> #s_mms_club_merchant_number_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_club_merchant_number @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_club_merchant_number @current_dv_batch_id

end
