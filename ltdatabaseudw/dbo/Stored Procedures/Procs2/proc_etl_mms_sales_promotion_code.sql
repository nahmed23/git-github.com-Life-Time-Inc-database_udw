CREATE PROC [dbo].[proc_etl_mms_sales_promotion_code] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_SalesPromotionCode

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_SalesPromotionCode (
       bk_hash,
       SalesPromotionCodeID,
       SalesPromotionID,
       MemberID,
       PromotionCode,
       ExpirationDate,
       UsageLimit,
       NotifyEmailAddress,
       NumberOfCodeRecipients,
       InsertedDateTime,
       UpdatedDateTime,
       DisplayUIFlag,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(SalesPromotionCodeID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       SalesPromotionCodeID,
       SalesPromotionID,
       MemberID,
       PromotionCode,
       ExpirationDate,
       UsageLimit,
       NotifyEmailAddress,
       NumberOfCodeRecipients,
       InsertedDateTime,
       UpdatedDateTime,
       DisplayUIFlag,
       isnull(cast(stage_mms_SalesPromotionCode.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_SalesPromotionCode
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_sales_promotion_code @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_sales_promotion_code (
       bk_hash,
       sales_promotion_code_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_SalesPromotionCode.bk_hash,
       stage_hash_mms_SalesPromotionCode.SalesPromotionCodeID sales_promotion_code_id,
       isnull(cast(stage_hash_mms_SalesPromotionCode.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_SalesPromotionCode
  left join h_mms_sales_promotion_code
    on stage_hash_mms_SalesPromotionCode.bk_hash = h_mms_sales_promotion_code.bk_hash
 where h_mms_sales_promotion_code_id is null
   and stage_hash_mms_SalesPromotionCode.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_sales_promotion_code
if object_id('tempdb..#l_mms_sales_promotion_code_inserts') is not null drop table #l_mms_sales_promotion_code_inserts
create table #l_mms_sales_promotion_code_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_SalesPromotionCode.bk_hash,
       stage_hash_mms_SalesPromotionCode.SalesPromotionCodeID sales_promotion_code_id,
       stage_hash_mms_SalesPromotionCode.SalesPromotionID sales_promotion_id,
       stage_hash_mms_SalesPromotionCode.MemberID member_id,
       isnull(cast(stage_hash_mms_SalesPromotionCode.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotionCode.SalesPromotionCodeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotionCode.SalesPromotionID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotionCode.MemberID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_SalesPromotionCode
 where stage_hash_mms_SalesPromotionCode.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_sales_promotion_code records
set @insert_date_time = getdate()
insert into l_mms_sales_promotion_code (
       bk_hash,
       sales_promotion_code_id,
       sales_promotion_id,
       member_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_sales_promotion_code_inserts.bk_hash,
       #l_mms_sales_promotion_code_inserts.sales_promotion_code_id,
       #l_mms_sales_promotion_code_inserts.sales_promotion_id,
       #l_mms_sales_promotion_code_inserts.member_id,
       case when l_mms_sales_promotion_code.l_mms_sales_promotion_code_id is null then isnull(#l_mms_sales_promotion_code_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_sales_promotion_code_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_sales_promotion_code_inserts
  left join p_mms_sales_promotion_code
    on #l_mms_sales_promotion_code_inserts.bk_hash = p_mms_sales_promotion_code.bk_hash
   and p_mms_sales_promotion_code.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_sales_promotion_code
    on p_mms_sales_promotion_code.bk_hash = l_mms_sales_promotion_code.bk_hash
   and p_mms_sales_promotion_code.l_mms_sales_promotion_code_id = l_mms_sales_promotion_code.l_mms_sales_promotion_code_id
 where l_mms_sales_promotion_code.l_mms_sales_promotion_code_id is null
    or (l_mms_sales_promotion_code.l_mms_sales_promotion_code_id is not null
        and l_mms_sales_promotion_code.dv_hash <> #l_mms_sales_promotion_code_inserts.source_hash)

--calculate hash and lookup to current s_mms_sales_promotion_code
if object_id('tempdb..#s_mms_sales_promotion_code_inserts') is not null drop table #s_mms_sales_promotion_code_inserts
create table #s_mms_sales_promotion_code_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_SalesPromotionCode.bk_hash,
       stage_hash_mms_SalesPromotionCode.SalesPromotionCodeID sales_promotion_code_id,
       stage_hash_mms_SalesPromotionCode.PromotionCode promotion_code,
       stage_hash_mms_SalesPromotionCode.ExpirationDate expiration_date,
       stage_hash_mms_SalesPromotionCode.UsageLimit usage_limit,
       stage_hash_mms_SalesPromotionCode.NotifyEmailAddress notify_email_address,
       stage_hash_mms_SalesPromotionCode.NumberOfCodeRecipients number_of_code_recipients,
       stage_hash_mms_SalesPromotionCode.InsertedDateTime inserted_date_time,
       stage_hash_mms_SalesPromotionCode.UpdatedDateTime updated_date_time,
       stage_hash_mms_SalesPromotionCode.DisplayUIFlag display_ui_flag,
       isnull(cast(stage_hash_mms_SalesPromotionCode.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotionCode.SalesPromotionCodeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_SalesPromotionCode.PromotionCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SalesPromotionCode.ExpirationDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotionCode.UsageLimit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_SalesPromotionCode.NotifyEmailAddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotionCode.NumberOfCodeRecipients as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SalesPromotionCode.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SalesPromotionCode.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotionCode.DisplayUIFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_SalesPromotionCode
 where stage_hash_mms_SalesPromotionCode.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_sales_promotion_code records
set @insert_date_time = getdate()
insert into s_mms_sales_promotion_code (
       bk_hash,
       sales_promotion_code_id,
       promotion_code,
       expiration_date,
       usage_limit,
       notify_email_address,
       number_of_code_recipients,
       inserted_date_time,
       updated_date_time,
       display_ui_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_sales_promotion_code_inserts.bk_hash,
       #s_mms_sales_promotion_code_inserts.sales_promotion_code_id,
       #s_mms_sales_promotion_code_inserts.promotion_code,
       #s_mms_sales_promotion_code_inserts.expiration_date,
       #s_mms_sales_promotion_code_inserts.usage_limit,
       #s_mms_sales_promotion_code_inserts.notify_email_address,
       #s_mms_sales_promotion_code_inserts.number_of_code_recipients,
       #s_mms_sales_promotion_code_inserts.inserted_date_time,
       #s_mms_sales_promotion_code_inserts.updated_date_time,
       #s_mms_sales_promotion_code_inserts.display_ui_flag,
       case when s_mms_sales_promotion_code.s_mms_sales_promotion_code_id is null then isnull(#s_mms_sales_promotion_code_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_sales_promotion_code_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_sales_promotion_code_inserts
  left join p_mms_sales_promotion_code
    on #s_mms_sales_promotion_code_inserts.bk_hash = p_mms_sales_promotion_code.bk_hash
   and p_mms_sales_promotion_code.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_sales_promotion_code
    on p_mms_sales_promotion_code.bk_hash = s_mms_sales_promotion_code.bk_hash
   and p_mms_sales_promotion_code.s_mms_sales_promotion_code_id = s_mms_sales_promotion_code.s_mms_sales_promotion_code_id
 where s_mms_sales_promotion_code.s_mms_sales_promotion_code_id is null
    or (s_mms_sales_promotion_code.s_mms_sales_promotion_code_id is not null
        and s_mms_sales_promotion_code.dv_hash <> #s_mms_sales_promotion_code_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_sales_promotion_code @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_sales_promotion_code @current_dv_batch_id

end
