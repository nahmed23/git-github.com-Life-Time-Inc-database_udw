CREATE PROC [dbo].[proc_etl_lt_bucks_product_options] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_lt_bucks_ProductOptions

set @insert_date_time = getdate()
insert into dbo.stage_hash_lt_bucks_ProductOptions (
       bk_hash,
       poption_id,
       poption_product,
       poption_title,
       poption_price,
       poption_active,
       poption_timestamp,
       poption_desc,
       poption_mms_id,
       poption_mms_multiplier,
       poption_conf_email_addr,
       poption_expiration_days,
       poption_has_quantities,
       poption_was_price,
       poption_continuousSchedule,
       LastModifiedTimestamp,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(poption_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       poption_id,
       poption_product,
       poption_title,
       poption_price,
       poption_active,
       poption_timestamp,
       poption_desc,
       poption_mms_id,
       poption_mms_multiplier,
       poption_conf_email_addr,
       poption_expiration_days,
       poption_has_quantities,
       poption_was_price,
       poption_continuousSchedule,
       LastModifiedTimestamp,
       isnull(cast(stage_lt_bucks_ProductOptions.poption_timestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_lt_bucks_ProductOptions
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_lt_bucks_product_options @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_lt_bucks_product_options (
       bk_hash,
       poption_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_lt_bucks_ProductOptions.bk_hash,
       stage_hash_lt_bucks_ProductOptions.poption_id poption_id,
       isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_timestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       5,
       @insert_date_time,
       @user
  from stage_hash_lt_bucks_ProductOptions
  left join h_lt_bucks_product_options
    on stage_hash_lt_bucks_ProductOptions.bk_hash = h_lt_bucks_product_options.bk_hash
 where h_lt_bucks_product_options_id is null
   and stage_hash_lt_bucks_ProductOptions.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_lt_bucks_product_options
if object_id('tempdb..#l_lt_bucks_product_options_inserts') is not null drop table #l_lt_bucks_product_options_inserts
create table #l_lt_bucks_product_options_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_ProductOptions.bk_hash,
       stage_hash_lt_bucks_ProductOptions.poption_id poption_id,
       stage_hash_lt_bucks_ProductOptions.poption_product poption_product,
       stage_hash_lt_bucks_ProductOptions.poption_mms_id poption_mms_id,
       isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_timestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_product as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_mms_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_ProductOptions
 where stage_hash_lt_bucks_ProductOptions.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_lt_bucks_product_options records
set @insert_date_time = getdate()
insert into l_lt_bucks_product_options (
       bk_hash,
       poption_id,
       poption_product,
       poption_mms_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_lt_bucks_product_options_inserts.bk_hash,
       #l_lt_bucks_product_options_inserts.poption_id,
       #l_lt_bucks_product_options_inserts.poption_product,
       #l_lt_bucks_product_options_inserts.poption_mms_id,
       case when l_lt_bucks_product_options.l_lt_bucks_product_options_id is null then isnull(#l_lt_bucks_product_options_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #l_lt_bucks_product_options_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_lt_bucks_product_options_inserts
  left join p_lt_bucks_product_options
    on #l_lt_bucks_product_options_inserts.bk_hash = p_lt_bucks_product_options.bk_hash
   and p_lt_bucks_product_options.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_lt_bucks_product_options
    on p_lt_bucks_product_options.bk_hash = l_lt_bucks_product_options.bk_hash
   and p_lt_bucks_product_options.l_lt_bucks_product_options_id = l_lt_bucks_product_options.l_lt_bucks_product_options_id
 where l_lt_bucks_product_options.l_lt_bucks_product_options_id is null
    or (l_lt_bucks_product_options.l_lt_bucks_product_options_id is not null
        and l_lt_bucks_product_options.dv_hash <> #l_lt_bucks_product_options_inserts.source_hash)

--calculate hash and lookup to current s_lt_bucks_product_options
if object_id('tempdb..#s_lt_bucks_product_options_inserts') is not null drop table #s_lt_bucks_product_options_inserts
create table #s_lt_bucks_product_options_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_ProductOptions.bk_hash,
       stage_hash_lt_bucks_ProductOptions.poption_id poption_id,
       stage_hash_lt_bucks_ProductOptions.poption_title poption_title,
       stage_hash_lt_bucks_ProductOptions.poption_price poption_price,
       stage_hash_lt_bucks_ProductOptions.poption_active poption_active,
       stage_hash_lt_bucks_ProductOptions.poption_timestamp poption_timestamp,
       stage_hash_lt_bucks_ProductOptions.poption_desc poption_desc,
       stage_hash_lt_bucks_ProductOptions.poption_mms_multiplier poption_mms_multiplier,
       stage_hash_lt_bucks_ProductOptions.poption_conf_email_addr poption_conf_email_addr,
       stage_hash_lt_bucks_ProductOptions.poption_expiration_days poption_expiration_days,
       stage_hash_lt_bucks_ProductOptions.poption_has_quantities poption_has_quantities,
       stage_hash_lt_bucks_ProductOptions.poption_was_price poption_was_price,
       stage_hash_lt_bucks_ProductOptions.poption_continuousSchedule poption_continuous_schedule,
       stage_hash_lt_bucks_ProductOptions.LastModifiedTimestamp last_modified_timestamp,
       isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_timestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_ProductOptions.poption_title,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_price as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_active as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_ProductOptions.poption_timestamp,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_ProductOptions.poption_desc,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_mms_multiplier as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_ProductOptions.poption_conf_email_addr,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_expiration_days as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_has_quantities as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_was_price as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ProductOptions.poption_continuousSchedule as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_ProductOptions.LastModifiedTimestamp,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_ProductOptions
 where stage_hash_lt_bucks_ProductOptions.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_lt_bucks_product_options records
set @insert_date_time = getdate()
insert into s_lt_bucks_product_options (
       bk_hash,
       poption_id,
       poption_title,
       poption_price,
       poption_active,
       poption_timestamp,
       poption_desc,
       poption_mms_multiplier,
       poption_conf_email_addr,
       poption_expiration_days,
       poption_has_quantities,
       poption_was_price,
       poption_continuous_schedule,
       last_modified_timestamp,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_lt_bucks_product_options_inserts.bk_hash,
       #s_lt_bucks_product_options_inserts.poption_id,
       #s_lt_bucks_product_options_inserts.poption_title,
       #s_lt_bucks_product_options_inserts.poption_price,
       #s_lt_bucks_product_options_inserts.poption_active,
       #s_lt_bucks_product_options_inserts.poption_timestamp,
       #s_lt_bucks_product_options_inserts.poption_desc,
       #s_lt_bucks_product_options_inserts.poption_mms_multiplier,
       #s_lt_bucks_product_options_inserts.poption_conf_email_addr,
       #s_lt_bucks_product_options_inserts.poption_expiration_days,
       #s_lt_bucks_product_options_inserts.poption_has_quantities,
       #s_lt_bucks_product_options_inserts.poption_was_price,
       #s_lt_bucks_product_options_inserts.poption_continuous_schedule,
       #s_lt_bucks_product_options_inserts.last_modified_timestamp,
       case when s_lt_bucks_product_options.s_lt_bucks_product_options_id is null then isnull(#s_lt_bucks_product_options_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #s_lt_bucks_product_options_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_lt_bucks_product_options_inserts
  left join p_lt_bucks_product_options
    on #s_lt_bucks_product_options_inserts.bk_hash = p_lt_bucks_product_options.bk_hash
   and p_lt_bucks_product_options.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_lt_bucks_product_options
    on p_lt_bucks_product_options.bk_hash = s_lt_bucks_product_options.bk_hash
   and p_lt_bucks_product_options.s_lt_bucks_product_options_id = s_lt_bucks_product_options.s_lt_bucks_product_options_id
 where s_lt_bucks_product_options.s_lt_bucks_product_options_id is null
    or (s_lt_bucks_product_options.s_lt_bucks_product_options_id is not null
        and s_lt_bucks_product_options.dv_hash <> #s_lt_bucks_product_options_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_lt_bucks_product_options @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_lt_bucks_product_options @current_dv_batch_id

end
