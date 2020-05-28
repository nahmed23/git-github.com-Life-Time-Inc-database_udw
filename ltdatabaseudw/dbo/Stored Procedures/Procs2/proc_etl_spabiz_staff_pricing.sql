﻿CREATE PROC [dbo].[proc_etl_spabiz_staff_pricing] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_STAFFPRICING

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_STAFFPRICING (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       SERVICEID,
       STAFFID,
       STAFFSERVICEINDEX,
       USEPRICESPECIAL,
       RETAILPRICE,
       COST,
       USETIMESPECIAL,
       TIME,
       PROCESS,
       FINISH,
       NEWEXTRATIME,
       SALES_SERVICETOTAL,
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
       SERVICEID,
       STAFFID,
       STAFFSERVICEINDEX,
       USEPRICESPECIAL,
       RETAILPRICE,
       COST,
       USETIMESPECIAL,
       TIME,
       PROCESS,
       FINISH,
       NEWEXTRATIME,
       SALES_SERVICETOTAL,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_STAFFPRICING.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_STAFFPRICING
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_staff_pricing @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_staff_pricing (
       bk_hash,
       staff_pricing_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_STAFFPRICING.bk_hash,
       stage_hash_spabiz_STAFFPRICING.ID staff_pricing_id,
       stage_hash_spabiz_STAFFPRICING.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_STAFFPRICING.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_STAFFPRICING
  left join h_spabiz_staff_pricing
    on stage_hash_spabiz_STAFFPRICING.bk_hash = h_spabiz_staff_pricing.bk_hash
 where h_spabiz_staff_pricing_id is null
   and stage_hash_spabiz_STAFFPRICING.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_staff_pricing
if object_id('tempdb..#l_spabiz_staff_pricing_inserts') is not null drop table #l_spabiz_staff_pricing_inserts
create table #l_spabiz_staff_pricing_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_STAFFPRICING.bk_hash,
       stage_hash_spabiz_STAFFPRICING.ID staff_pricing_id,
       stage_hash_spabiz_STAFFPRICING.STOREID store_id,
       stage_hash_spabiz_STAFFPRICING.SERVICEID service_id,
       stage_hash_spabiz_STAFFPRICING.STAFFID staff_id,
       stage_hash_spabiz_STAFFPRICING.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_STAFFPRICING.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.SERVICEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_STAFFPRICING
 where stage_hash_spabiz_STAFFPRICING.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_staff_pricing records
set @insert_date_time = getdate()
insert into l_spabiz_staff_pricing (
       bk_hash,
       staff_pricing_id,
       store_id,
       service_id,
       staff_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_staff_pricing_inserts.bk_hash,
       #l_spabiz_staff_pricing_inserts.staff_pricing_id,
       #l_spabiz_staff_pricing_inserts.store_id,
       #l_spabiz_staff_pricing_inserts.service_id,
       #l_spabiz_staff_pricing_inserts.staff_id,
       #l_spabiz_staff_pricing_inserts.store_number,
       case when l_spabiz_staff_pricing.l_spabiz_staff_pricing_id is null then isnull(#l_spabiz_staff_pricing_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_staff_pricing_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_staff_pricing_inserts
  left join p_spabiz_staff_pricing
    on #l_spabiz_staff_pricing_inserts.bk_hash = p_spabiz_staff_pricing.bk_hash
   and p_spabiz_staff_pricing.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_staff_pricing
    on p_spabiz_staff_pricing.bk_hash = l_spabiz_staff_pricing.bk_hash
   and p_spabiz_staff_pricing.l_spabiz_staff_pricing_id = l_spabiz_staff_pricing.l_spabiz_staff_pricing_id
 where l_spabiz_staff_pricing.l_spabiz_staff_pricing_id is null
    or (l_spabiz_staff_pricing.l_spabiz_staff_pricing_id is not null
        and l_spabiz_staff_pricing.dv_hash <> #l_spabiz_staff_pricing_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_staff_pricing
if object_id('tempdb..#s_spabiz_staff_pricing_inserts') is not null drop table #s_spabiz_staff_pricing_inserts
create table #s_spabiz_staff_pricing_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_STAFFPRICING.bk_hash,
       stage_hash_spabiz_STAFFPRICING.ID staff_pricing_id,
       stage_hash_spabiz_STAFFPRICING.COUNTERID counter_id,
       stage_hash_spabiz_STAFFPRICING.EDITTIME edit_time,
       stage_hash_spabiz_STAFFPRICING.STAFFSERVICEINDEX staff_service_index,
       stage_hash_spabiz_STAFFPRICING.USEPRICESPECIAL use_price_special,
       stage_hash_spabiz_STAFFPRICING.RETAILPRICE retail_price,
       stage_hash_spabiz_STAFFPRICING.COST cost,
       stage_hash_spabiz_STAFFPRICING.USETIMESPECIAL use_time_special,
       stage_hash_spabiz_STAFFPRICING.TIME time,
       stage_hash_spabiz_STAFFPRICING.PROCESS process,
       stage_hash_spabiz_STAFFPRICING.FINISH finish,
       stage_hash_spabiz_STAFFPRICING.NEWEXTRATIME new_extra_time,
       stage_hash_spabiz_STAFFPRICING.SALES_SERVICETOTAL sales_service_total,
       stage_hash_spabiz_STAFFPRICING.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_STAFFPRICING.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_STAFFPRICING.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFFPRICING.STAFFSERVICEINDEX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.USEPRICESPECIAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.RETAILPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.COST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.USETIMESPECIAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFFPRICING.TIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFFPRICING.PROCESS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFFPRICING.FINISH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFFPRICING.NEWEXTRATIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.SALES_SERVICETOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFFPRICING.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_STAFFPRICING
 where stage_hash_spabiz_STAFFPRICING.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_staff_pricing records
set @insert_date_time = getdate()
insert into s_spabiz_staff_pricing (
       bk_hash,
       staff_pricing_id,
       counter_id,
       edit_time,
       staff_service_index,
       use_price_special,
       retail_price,
       cost,
       use_time_special,
       time,
       process,
       finish,
       new_extra_time,
       sales_service_total,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_staff_pricing_inserts.bk_hash,
       #s_spabiz_staff_pricing_inserts.staff_pricing_id,
       #s_spabiz_staff_pricing_inserts.counter_id,
       #s_spabiz_staff_pricing_inserts.edit_time,
       #s_spabiz_staff_pricing_inserts.staff_service_index,
       #s_spabiz_staff_pricing_inserts.use_price_special,
       #s_spabiz_staff_pricing_inserts.retail_price,
       #s_spabiz_staff_pricing_inserts.cost,
       #s_spabiz_staff_pricing_inserts.use_time_special,
       #s_spabiz_staff_pricing_inserts.time,
       #s_spabiz_staff_pricing_inserts.process,
       #s_spabiz_staff_pricing_inserts.finish,
       #s_spabiz_staff_pricing_inserts.new_extra_time,
       #s_spabiz_staff_pricing_inserts.sales_service_total,
       #s_spabiz_staff_pricing_inserts.store_number,
       case when s_spabiz_staff_pricing.s_spabiz_staff_pricing_id is null then isnull(#s_spabiz_staff_pricing_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_staff_pricing_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_staff_pricing_inserts
  left join p_spabiz_staff_pricing
    on #s_spabiz_staff_pricing_inserts.bk_hash = p_spabiz_staff_pricing.bk_hash
   and p_spabiz_staff_pricing.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_staff_pricing
    on p_spabiz_staff_pricing.bk_hash = s_spabiz_staff_pricing.bk_hash
   and p_spabiz_staff_pricing.s_spabiz_staff_pricing_id = s_spabiz_staff_pricing.s_spabiz_staff_pricing_id
 where s_spabiz_staff_pricing.s_spabiz_staff_pricing_id is null
    or (s_spabiz_staff_pricing.s_spabiz_staff_pricing_id is not null
        and s_spabiz_staff_pricing.dv_hash <> #s_spabiz_staff_pricing_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_staff_pricing @current_dv_batch_id

end
