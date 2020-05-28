CREATE PROC [dbo].[proc_etl_spabiz_discount] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_DISCOUNT

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_DISCOUNT (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       AMOUNT,
       PAYRETAILCOMISH,
       PAYSERVICECOMISH,
       PAYCOMISH,
       ISPROMO,
       USEDATERANGE,
       FROMDATE,
       TODATE,
       APPLYTO,
       DISCOUNTFILTER,
       APPLYWHEN,
       DEPTCAT,
       SEARCHCAT,
       Percent_Discount,
       Percent_Dollar,
       STORE_NUMBER,
       GLACCOUNT,
       DESCRIPTION,
       DESCRIPTITON,
       SECURITYLEVEL,
       ONETIME,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       AMOUNT,
       PAYRETAILCOMISH,
       PAYSERVICECOMISH,
       PAYCOMISH,
       ISPROMO,
       USEDATERANGE,
       FROMDATE,
       TODATE,
       APPLYTO,
       DISCOUNTFILTER,
       APPLYWHEN,
       DEPTCAT,
       SEARCHCAT,
       Percent_Discount,
       Percent_Dollar,
       STORE_NUMBER,
       GLACCOUNT,
       DESCRIPTION,
       DESCRIPTITON,
       SECURITYLEVEL,
       ONETIME,
       isnull(cast(stage_spabiz_DISCOUNT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_DISCOUNT
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_discount @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_discount (
       bk_hash,
       discount_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_DISCOUNT.bk_hash,
       stage_hash_spabiz_DISCOUNT.ID discount_id,
       stage_hash_spabiz_DISCOUNT.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_DISCOUNT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_DISCOUNT
  left join h_spabiz_discount
    on stage_hash_spabiz_DISCOUNT.bk_hash = h_spabiz_discount.bk_hash
 where h_spabiz_discount_id is null
   and stage_hash_spabiz_DISCOUNT.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_discount
if object_id('tempdb..#l_spabiz_discount_inserts') is not null drop table #l_spabiz_discount_inserts
create table #l_spabiz_discount_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_DISCOUNT.bk_hash,
       stage_hash_spabiz_DISCOUNT.ID discount_id,
       stage_hash_spabiz_DISCOUNT.STOREID store_id,
       stage_hash_spabiz_DISCOUNT.DEPTCAT dept_cat,
       stage_hash_spabiz_DISCOUNT.SEARCHCAT search_cat,
       stage_hash_spabiz_DISCOUNT.STORE_NUMBER store_number,
       stage_hash_spabiz_DISCOUNT.GLACCOUNT gl_account,
       stage_hash_spabiz_DISCOUNT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.DEPTCAT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.SEARCHCAT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DISCOUNT.GLACCOUNT,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_DISCOUNT
 where stage_hash_spabiz_DISCOUNT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_discount records
set @insert_date_time = getdate()
insert into l_spabiz_discount (
       bk_hash,
       discount_id,
       store_id,
       dept_cat,
       search_cat,
       store_number,
       gl_account,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_discount_inserts.bk_hash,
       #l_spabiz_discount_inserts.discount_id,
       #l_spabiz_discount_inserts.store_id,
       #l_spabiz_discount_inserts.dept_cat,
       #l_spabiz_discount_inserts.search_cat,
       #l_spabiz_discount_inserts.store_number,
       #l_spabiz_discount_inserts.gl_account,
       case when l_spabiz_discount.l_spabiz_discount_id is null then isnull(#l_spabiz_discount_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_discount_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_discount_inserts
  left join p_spabiz_discount
    on #l_spabiz_discount_inserts.bk_hash = p_spabiz_discount.bk_hash
   and p_spabiz_discount.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_discount
    on p_spabiz_discount.bk_hash = l_spabiz_discount.bk_hash
   and p_spabiz_discount.l_spabiz_discount_id = l_spabiz_discount.l_spabiz_discount_id
 where l_spabiz_discount.l_spabiz_discount_id is null
    or (l_spabiz_discount.l_spabiz_discount_id is not null
        and l_spabiz_discount.dv_hash <> #l_spabiz_discount_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_discount
if object_id('tempdb..#s_spabiz_discount_inserts') is not null drop table #s_spabiz_discount_inserts
create table #s_spabiz_discount_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_DISCOUNT.bk_hash,
       stage_hash_spabiz_DISCOUNT.ID discount_id,
       stage_hash_spabiz_DISCOUNT.COUNTERID counter_id,
       stage_hash_spabiz_DISCOUNT.EDITTIME edit_time,
       stage_hash_spabiz_DISCOUNT.[Delete] discount_delete,
       stage_hash_spabiz_DISCOUNT.DELETEDATE delete_date,
       stage_hash_spabiz_DISCOUNT.NAME name,
       stage_hash_spabiz_DISCOUNT.QUICKID quick_id,
       stage_hash_spabiz_DISCOUNT.AMOUNT amount,
       stage_hash_spabiz_DISCOUNT.PAYRETAILCOMISH pay_retail_comish,
       stage_hash_spabiz_DISCOUNT.PAYSERVICECOMISH pay_service_comish,
       stage_hash_spabiz_DISCOUNT.PAYCOMISH pay_comish,
       stage_hash_spabiz_DISCOUNT.ISPROMO is_promo,
       stage_hash_spabiz_DISCOUNT.USEDATERANGE use_date_range,
       stage_hash_spabiz_DISCOUNT.FROMDATE from_date,
       stage_hash_spabiz_DISCOUNT.TODATE to_date,
       stage_hash_spabiz_DISCOUNT.APPLYTO apply_to,
       stage_hash_spabiz_DISCOUNT.DISCOUNTFILTER discount_filter,
       stage_hash_spabiz_DISCOUNT.APPLYWHEN apply_when,
       stage_hash_spabiz_DISCOUNT.Percent_Discount percent_discount,
       stage_hash_spabiz_DISCOUNT.Percent_Dollar percent_dollar,
       stage_hash_spabiz_DISCOUNT.STORE_NUMBER store_number,
       stage_hash_spabiz_DISCOUNT.DESCRIPTION description,
       stage_hash_spabiz_DISCOUNT.DESCRIPTITON descriptiton,
       stage_hash_spabiz_DISCOUNT.SECURITYLEVEL security_level,
       stage_hash_spabiz_DISCOUNT.ONETIME one_time,
       stage_hash_spabiz_DISCOUNT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_DISCOUNT.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_DISCOUNT.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DISCOUNT.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DISCOUNT.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.AMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.PAYRETAILCOMISH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.PAYSERVICECOMISH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.PAYCOMISH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.ISPROMO as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.USEDATERANGE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_DISCOUNT.FROMDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_DISCOUNT.TODATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.APPLYTO as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.DISCOUNTFILTER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.APPLYWHEN as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.Percent_Discount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.Percent_Dollar as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DISCOUNT.DESCRIPTION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DISCOUNT.DESCRIPTITON,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.SECURITYLEVEL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DISCOUNT.ONETIME as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_DISCOUNT
 where stage_hash_spabiz_DISCOUNT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_discount records
set @insert_date_time = getdate()
insert into s_spabiz_discount (
       bk_hash,
       discount_id,
       counter_id,
       edit_time,
       discount_delete,
       delete_date,
       name,
       quick_id,
       amount,
       pay_retail_comish,
       pay_service_comish,
       pay_comish,
       is_promo,
       use_date_range,
       from_date,
       to_date,
       apply_to,
       discount_filter,
       apply_when,
       percent_discount,
       percent_dollar,
       store_number,
       description,
       descriptiton,
       security_level,
       one_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_discount_inserts.bk_hash,
       #s_spabiz_discount_inserts.discount_id,
       #s_spabiz_discount_inserts.counter_id,
       #s_spabiz_discount_inserts.edit_time,
       #s_spabiz_discount_inserts.discount_delete,
       #s_spabiz_discount_inserts.delete_date,
       #s_spabiz_discount_inserts.name,
       #s_spabiz_discount_inserts.quick_id,
       #s_spabiz_discount_inserts.amount,
       #s_spabiz_discount_inserts.pay_retail_comish,
       #s_spabiz_discount_inserts.pay_service_comish,
       #s_spabiz_discount_inserts.pay_comish,
       #s_spabiz_discount_inserts.is_promo,
       #s_spabiz_discount_inserts.use_date_range,
       #s_spabiz_discount_inserts.from_date,
       #s_spabiz_discount_inserts.to_date,
       #s_spabiz_discount_inserts.apply_to,
       #s_spabiz_discount_inserts.discount_filter,
       #s_spabiz_discount_inserts.apply_when,
       #s_spabiz_discount_inserts.percent_discount,
       #s_spabiz_discount_inserts.percent_dollar,
       #s_spabiz_discount_inserts.store_number,
       #s_spabiz_discount_inserts.description,
       #s_spabiz_discount_inserts.descriptiton,
       #s_spabiz_discount_inserts.security_level,
       #s_spabiz_discount_inserts.one_time,
       case when s_spabiz_discount.s_spabiz_discount_id is null then isnull(#s_spabiz_discount_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_discount_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_discount_inserts
  left join p_spabiz_discount
    on #s_spabiz_discount_inserts.bk_hash = p_spabiz_discount.bk_hash
   and p_spabiz_discount.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_discount
    on p_spabiz_discount.bk_hash = s_spabiz_discount.bk_hash
   and p_spabiz_discount.s_spabiz_discount_id = s_spabiz_discount.s_spabiz_discount_id
 where s_spabiz_discount.s_spabiz_discount_id is null
    or (s_spabiz_discount.s_spabiz_discount_id is not null
        and s_spabiz_discount.dv_hash <> #s_spabiz_discount_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_discount @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_discount @current_dv_batch_id

end
