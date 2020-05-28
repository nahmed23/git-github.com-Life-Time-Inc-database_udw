CREATE PROC [dbo].[proc_etl_spabiz_gift] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_GIFT

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_GIFT (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       PAYCOMMISSION,
       RETAILPRICE,
       PRICECHANGABLE,
       DAYSGOODFOR,
       USEFOR,
       REFUNDABLE,
       STORE_NUMBER,
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
       PAYCOMMISSION,
       RETAILPRICE,
       PRICECHANGABLE,
       DAYSGOODFOR,
       USEFOR,
       REFUNDABLE,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_GIFT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_GIFT
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_gift @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_gift (
       bk_hash,
       gift_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_GIFT.bk_hash,
       stage_hash_spabiz_GIFT.ID gift_id,
       stage_hash_spabiz_GIFT.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_GIFT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_GIFT
  left join h_spabiz_gift
    on stage_hash_spabiz_GIFT.bk_hash = h_spabiz_gift.bk_hash
 where h_spabiz_gift_id is null
   and stage_hash_spabiz_GIFT.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_gift
if object_id('tempdb..#l_spabiz_gift_inserts') is not null drop table #l_spabiz_gift_inserts
create table #l_spabiz_gift_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_GIFT.bk_hash,
       stage_hash_spabiz_GIFT.ID gift_id,
       stage_hash_spabiz_GIFT.STOREID store_id,
       stage_hash_spabiz_GIFT.STORE_NUMBER store_number,
       stage_hash_spabiz_GIFT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_GIFT
 where stage_hash_spabiz_GIFT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_gift records
set @insert_date_time = getdate()
insert into l_spabiz_gift (
       bk_hash,
       gift_id,
       store_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_gift_inserts.bk_hash,
       #l_spabiz_gift_inserts.gift_id,
       #l_spabiz_gift_inserts.store_id,
       #l_spabiz_gift_inserts.store_number,
       case when l_spabiz_gift.l_spabiz_gift_id is null then isnull(#l_spabiz_gift_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_gift_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_gift_inserts
  left join p_spabiz_gift
    on #l_spabiz_gift_inserts.bk_hash = p_spabiz_gift.bk_hash
   and p_spabiz_gift.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_gift
    on p_spabiz_gift.bk_hash = l_spabiz_gift.bk_hash
   and p_spabiz_gift.l_spabiz_gift_id = l_spabiz_gift.l_spabiz_gift_id
 where l_spabiz_gift.l_spabiz_gift_id is null
    or (l_spabiz_gift.l_spabiz_gift_id is not null
        and l_spabiz_gift.dv_hash <> #l_spabiz_gift_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_gift
if object_id('tempdb..#s_spabiz_gift_inserts') is not null drop table #s_spabiz_gift_inserts
create table #s_spabiz_gift_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_GIFT.bk_hash,
       stage_hash_spabiz_GIFT.ID gift_id,
       stage_hash_spabiz_GIFT.COUNTERID counter_id,
       stage_hash_spabiz_GIFT.EDITTIME edit_time,
       stage_hash_spabiz_GIFT.[Delete] gift_delete,
       stage_hash_spabiz_GIFT.DELETEDATE delete_date,
       stage_hash_spabiz_GIFT.NAME name,
       stage_hash_spabiz_GIFT.PAYCOMMISSION pay_commission,
       stage_hash_spabiz_GIFT.RETAILPRICE retail_price,
       stage_hash_spabiz_GIFT.PRICECHANGABLE price_changable,
       stage_hash_spabiz_GIFT.DAYSGOODFOR days_good_for,
       stage_hash_spabiz_GIFT.USEFOR use_for,
       stage_hash_spabiz_GIFT.REFUNDABLE refundable,
       stage_hash_spabiz_GIFT.STORE_NUMBER store_number,
       stage_hash_spabiz_GIFT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_GIFT.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_GIFT.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_GIFT.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.PAYCOMMISSION as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.RETAILPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.PRICECHANGABLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.DAYSGOODFOR as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.USEFOR as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.REFUNDABLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFT.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_GIFT
 where stage_hash_spabiz_GIFT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_gift records
set @insert_date_time = getdate()
insert into s_spabiz_gift (
       bk_hash,
       gift_id,
       counter_id,
       edit_time,
       gift_delete,
       delete_date,
       name,
       pay_commission,
       retail_price,
       price_changable,
       days_good_for,
       use_for,
       refundable,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_gift_inserts.bk_hash,
       #s_spabiz_gift_inserts.gift_id,
       #s_spabiz_gift_inserts.counter_id,
       #s_spabiz_gift_inserts.edit_time,
       #s_spabiz_gift_inserts.gift_delete,
       #s_spabiz_gift_inserts.delete_date,
       #s_spabiz_gift_inserts.name,
       #s_spabiz_gift_inserts.pay_commission,
       #s_spabiz_gift_inserts.retail_price,
       #s_spabiz_gift_inserts.price_changable,
       #s_spabiz_gift_inserts.days_good_for,
       #s_spabiz_gift_inserts.use_for,
       #s_spabiz_gift_inserts.refundable,
       #s_spabiz_gift_inserts.store_number,
       case when s_spabiz_gift.s_spabiz_gift_id is null then isnull(#s_spabiz_gift_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_gift_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_gift_inserts
  left join p_spabiz_gift
    on #s_spabiz_gift_inserts.bk_hash = p_spabiz_gift.bk_hash
   and p_spabiz_gift.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_gift
    on p_spabiz_gift.bk_hash = s_spabiz_gift.bk_hash
   and p_spabiz_gift.s_spabiz_gift_id = s_spabiz_gift.s_spabiz_gift_id
 where s_spabiz_gift.s_spabiz_gift_id is null
    or (s_spabiz_gift.s_spabiz_gift_id is not null
        and s_spabiz_gift.dv_hash <> #s_spabiz_gift_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_gift @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_gift @current_dv_batch_id

end
