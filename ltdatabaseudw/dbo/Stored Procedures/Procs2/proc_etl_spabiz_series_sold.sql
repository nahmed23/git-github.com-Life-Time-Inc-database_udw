CREATE PROC [dbo].[proc_etl_spabiz_series_sold] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_SERIESSOLD

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_SERIESSOLD (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TICKETID,
       SERIALNUM,
       Date,
       SERIESID,
       STAFFIDCREATE,
       STAFFID1,
       STAFFID2,
       BUY_CUSTID,
       CUSTID,
       STATUS,
       LASTUSED,
       RETAILPRICE,
       BALANCE,
       TICKETNUM,
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
       TICKETID,
       SERIALNUM,
       Date,
       SERIESID,
       STAFFIDCREATE,
       STAFFID1,
       STAFFID2,
       BUY_CUSTID,
       CUSTID,
       STATUS,
       LASTUSED,
       RETAILPRICE,
       BALANCE,
       TICKETNUM,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_SERIESSOLD.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_SERIESSOLD
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_series_sold @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_series_sold (
       bk_hash,
       series_sold_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_SERIESSOLD.bk_hash,
       stage_hash_spabiz_SERIESSOLD.ID series_sold_id,
       stage_hash_spabiz_SERIESSOLD.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_SERIESSOLD.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_SERIESSOLD
  left join h_spabiz_series_sold
    on stage_hash_spabiz_SERIESSOLD.bk_hash = h_spabiz_series_sold.bk_hash
 where h_spabiz_series_sold_id is null
   and stage_hash_spabiz_SERIESSOLD.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_series_sold
if object_id('tempdb..#l_spabiz_series_sold_inserts') is not null drop table #l_spabiz_series_sold_inserts
create table #l_spabiz_series_sold_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SERIESSOLD.bk_hash,
       stage_hash_spabiz_SERIESSOLD.ID series_sold_id,
       stage_hash_spabiz_SERIESSOLD.STOREID store_id,
       stage_hash_spabiz_SERIESSOLD.TICKETID ticket_id,
       stage_hash_spabiz_SERIESSOLD.SERIESID series_id,
       stage_hash_spabiz_SERIESSOLD.STAFFID1 staff_id_1,
       stage_hash_spabiz_SERIESSOLD.STAFFID2 staff_id_2,
       stage_hash_spabiz_SERIESSOLD.BUY_CUSTID buy_cust_id,
       stage_hash_spabiz_SERIESSOLD.CUSTID cust_id,
       stage_hash_spabiz_SERIESSOLD.STORE_NUMBER store_number,
       stage_hash_spabiz_SERIESSOLD.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.TICKETID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.SERIESID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.STAFFID1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.STAFFID2 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.BUY_CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SERIESSOLD
 where stage_hash_spabiz_SERIESSOLD.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_series_sold records
set @insert_date_time = getdate()
insert into l_spabiz_series_sold (
       bk_hash,
       series_sold_id,
       store_id,
       ticket_id,
       series_id,
       staff_id_1,
       staff_id_2,
       buy_cust_id,
       cust_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_series_sold_inserts.bk_hash,
       #l_spabiz_series_sold_inserts.series_sold_id,
       #l_spabiz_series_sold_inserts.store_id,
       #l_spabiz_series_sold_inserts.ticket_id,
       #l_spabiz_series_sold_inserts.series_id,
       #l_spabiz_series_sold_inserts.staff_id_1,
       #l_spabiz_series_sold_inserts.staff_id_2,
       #l_spabiz_series_sold_inserts.buy_cust_id,
       #l_spabiz_series_sold_inserts.cust_id,
       #l_spabiz_series_sold_inserts.store_number,
       case when l_spabiz_series_sold.l_spabiz_series_sold_id is null then isnull(#l_spabiz_series_sold_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_series_sold_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_series_sold_inserts
  left join p_spabiz_series_sold
    on #l_spabiz_series_sold_inserts.bk_hash = p_spabiz_series_sold.bk_hash
   and p_spabiz_series_sold.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_series_sold
    on p_spabiz_series_sold.bk_hash = l_spabiz_series_sold.bk_hash
   and p_spabiz_series_sold.l_spabiz_series_sold_id = l_spabiz_series_sold.l_spabiz_series_sold_id
 where l_spabiz_series_sold.l_spabiz_series_sold_id is null
    or (l_spabiz_series_sold.l_spabiz_series_sold_id is not null
        and l_spabiz_series_sold.dv_hash <> #l_spabiz_series_sold_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_series_sold
if object_id('tempdb..#s_spabiz_series_sold_inserts') is not null drop table #s_spabiz_series_sold_inserts
create table #s_spabiz_series_sold_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SERIESSOLD.bk_hash,
       stage_hash_spabiz_SERIESSOLD.ID series_sold_id,
       stage_hash_spabiz_SERIESSOLD.COUNTERID counter_id,
       stage_hash_spabiz_SERIESSOLD.EDITTIME edit_time,
       stage_hash_spabiz_SERIESSOLD.SERIALNUM serial_num,
       stage_hash_spabiz_SERIESSOLD.Date date,
       stage_hash_spabiz_SERIESSOLD.STAFFIDCREATE staff_id_create,
       stage_hash_spabiz_SERIESSOLD.STATUS status,
       stage_hash_spabiz_SERIESSOLD.LASTUSED last_used,
       stage_hash_spabiz_SERIESSOLD.RETAILPRICE retail_price,
       stage_hash_spabiz_SERIESSOLD.BALANCE balance,
       stage_hash_spabiz_SERIESSOLD.TICKETNUM ticket_num,
       stage_hash_spabiz_SERIESSOLD.STORE_NUMBER store_number,
       stage_hash_spabiz_SERIESSOLD.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERIESSOLD.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERIESSOLD.SERIALNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERIESSOLD.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.STAFFIDCREATE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERIESSOLD.LASTUSED,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.RETAILPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.BALANCE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERIESSOLD.TICKETNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESSOLD.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SERIESSOLD
 where stage_hash_spabiz_SERIESSOLD.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_series_sold records
set @insert_date_time = getdate()
insert into s_spabiz_series_sold (
       bk_hash,
       series_sold_id,
       counter_id,
       edit_time,
       serial_num,
       date,
       staff_id_create,
       status,
       last_used,
       retail_price,
       balance,
       ticket_num,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_series_sold_inserts.bk_hash,
       #s_spabiz_series_sold_inserts.series_sold_id,
       #s_spabiz_series_sold_inserts.counter_id,
       #s_spabiz_series_sold_inserts.edit_time,
       #s_spabiz_series_sold_inserts.serial_num,
       #s_spabiz_series_sold_inserts.date,
       #s_spabiz_series_sold_inserts.staff_id_create,
       #s_spabiz_series_sold_inserts.status,
       #s_spabiz_series_sold_inserts.last_used,
       #s_spabiz_series_sold_inserts.retail_price,
       #s_spabiz_series_sold_inserts.balance,
       #s_spabiz_series_sold_inserts.ticket_num,
       #s_spabiz_series_sold_inserts.store_number,
       case when s_spabiz_series_sold.s_spabiz_series_sold_id is null then isnull(#s_spabiz_series_sold_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_series_sold_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_series_sold_inserts
  left join p_spabiz_series_sold
    on #s_spabiz_series_sold_inserts.bk_hash = p_spabiz_series_sold.bk_hash
   and p_spabiz_series_sold.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_series_sold
    on p_spabiz_series_sold.bk_hash = s_spabiz_series_sold.bk_hash
   and p_spabiz_series_sold.s_spabiz_series_sold_id = s_spabiz_series_sold.s_spabiz_series_sold_id
 where s_spabiz_series_sold.s_spabiz_series_sold_id is null
    or (s_spabiz_series_sold.s_spabiz_series_sold_id is not null
        and s_spabiz_series_sold.dv_hash <> #s_spabiz_series_sold_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_series_sold @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_series_sold @current_dv_batch_id

end
