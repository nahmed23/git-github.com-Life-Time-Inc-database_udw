CREATE PROC [dbo].[proc_etl_spabiz_service_charge] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_SERVICECHARGE

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_SERVICECHARGE (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       DISPLAYNAME,
       STORE_NUMBER,
       PAYCOMMISSION,
       ENABLED,
       ENABLEDTEXT,
       TAXABLE,
       AMOUNT,
       DOLLARPERCENT,
       STAFFID,
       GLACCT,
       COMPUTEDON,
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
       DISPLAYNAME,
       STORE_NUMBER,
       PAYCOMMISSION,
       ENABLED,
       ENABLEDTEXT,
       TAXABLE,
       AMOUNT,
       DOLLARPERCENT,
       STAFFID,
       GLACCT,
       COMPUTEDON,
       isnull(cast(stage_spabiz_SERVICECHARGE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_SERVICECHARGE
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_service_charge @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_service_charge (
       bk_hash,
       service_charge_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_SERVICECHARGE.bk_hash,
       stage_hash_spabiz_SERVICECHARGE.ID service_charge_id,
       stage_hash_spabiz_SERVICECHARGE.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_SERVICECHARGE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_SERVICECHARGE
  left join h_spabiz_service_charge
    on stage_hash_spabiz_SERVICECHARGE.bk_hash = h_spabiz_service_charge.bk_hash
 where h_spabiz_service_charge_id is null
   and stage_hash_spabiz_SERVICECHARGE.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_service_charge
if object_id('tempdb..#l_spabiz_service_charge_inserts') is not null drop table #l_spabiz_service_charge_inserts
create table #l_spabiz_service_charge_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SERVICECHARGE.bk_hash,
       stage_hash_spabiz_SERVICECHARGE.ID service_charge_id,
       stage_hash_spabiz_SERVICECHARGE.STOREID store_id,
       stage_hash_spabiz_SERVICECHARGE.QUICKID quick_id,
       stage_hash_spabiz_SERVICECHARGE.STORE_NUMBER store_number,
       stage_hash_spabiz_SERVICECHARGE.STAFFID staff_id,
       stage_hash_spabiz_SERVICECHARGE.GLACCT gl_acct,
       stage_hash_spabiz_SERVICECHARGE.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICECHARGE.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICECHARGE.GLACCT,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SERVICECHARGE
 where stage_hash_spabiz_SERVICECHARGE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_service_charge records
set @insert_date_time = getdate()
insert into l_spabiz_service_charge (
       bk_hash,
       service_charge_id,
       store_id,
       quick_id,
       store_number,
       staff_id,
       gl_acct,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_service_charge_inserts.bk_hash,
       #l_spabiz_service_charge_inserts.service_charge_id,
       #l_spabiz_service_charge_inserts.store_id,
       #l_spabiz_service_charge_inserts.quick_id,
       #l_spabiz_service_charge_inserts.store_number,
       #l_spabiz_service_charge_inserts.staff_id,
       #l_spabiz_service_charge_inserts.gl_acct,
       case when l_spabiz_service_charge.l_spabiz_service_charge_id is null then isnull(#l_spabiz_service_charge_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_service_charge_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_service_charge_inserts
  left join p_spabiz_service_charge
    on #l_spabiz_service_charge_inserts.bk_hash = p_spabiz_service_charge.bk_hash
   and p_spabiz_service_charge.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_service_charge
    on p_spabiz_service_charge.bk_hash = l_spabiz_service_charge.bk_hash
   and p_spabiz_service_charge.l_spabiz_service_charge_id = l_spabiz_service_charge.l_spabiz_service_charge_id
 where l_spabiz_service_charge.l_spabiz_service_charge_id is null
    or (l_spabiz_service_charge.l_spabiz_service_charge_id is not null
        and l_spabiz_service_charge.dv_hash <> #l_spabiz_service_charge_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_service_charge
if object_id('tempdb..#s_spabiz_service_charge_inserts') is not null drop table #s_spabiz_service_charge_inserts
create table #s_spabiz_service_charge_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SERVICECHARGE.bk_hash,
       stage_hash_spabiz_SERVICECHARGE.ID service_charge_id,
       stage_hash_spabiz_SERVICECHARGE.COUNTERID counter_id,
       stage_hash_spabiz_SERVICECHARGE.EDITTIME edit_time,
       stage_hash_spabiz_SERVICECHARGE.[Delete] service_charge_delete,
       stage_hash_spabiz_SERVICECHARGE.DELETEDATE delete_date,
       stage_hash_spabiz_SERVICECHARGE.NAME name,
       stage_hash_spabiz_SERVICECHARGE.DISPLAYNAME display_name,
       stage_hash_spabiz_SERVICECHARGE.STORE_NUMBER store_number,
       stage_hash_spabiz_SERVICECHARGE.PAYCOMMISSION pay_commission,
       stage_hash_spabiz_SERVICECHARGE.ENABLED enabled,
       stage_hash_spabiz_SERVICECHARGE.ENABLEDTEXT enabled_text,
       stage_hash_spabiz_SERVICECHARGE.TAXABLE taxable,
       stage_hash_spabiz_SERVICECHARGE.AMOUNT amount,
       stage_hash_spabiz_SERVICECHARGE.DOLLARPERCENT dollar_percent,
       stage_hash_spabiz_SERVICECHARGE.COMPUTEDON computed_on,
       stage_hash_spabiz_SERVICECHARGE.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERVICECHARGE.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERVICECHARGE.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICECHARGE.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICECHARGE.DISPLAYNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.PAYCOMMISSION as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.ENABLED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICECHARGE.ENABLEDTEXT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.TAXABLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.AMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.DOLLARPERCENT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICECHARGE.COMPUTEDON as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SERVICECHARGE
 where stage_hash_spabiz_SERVICECHARGE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_service_charge records
set @insert_date_time = getdate()
insert into s_spabiz_service_charge (
       bk_hash,
       service_charge_id,
       counter_id,
       edit_time,
       service_charge_delete,
       delete_date,
       name,
       display_name,
       store_number,
       pay_commission,
       enabled,
       enabled_text,
       taxable,
       amount,
       dollar_percent,
       computed_on,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_service_charge_inserts.bk_hash,
       #s_spabiz_service_charge_inserts.service_charge_id,
       #s_spabiz_service_charge_inserts.counter_id,
       #s_spabiz_service_charge_inserts.edit_time,
       #s_spabiz_service_charge_inserts.service_charge_delete,
       #s_spabiz_service_charge_inserts.delete_date,
       #s_spabiz_service_charge_inserts.name,
       #s_spabiz_service_charge_inserts.display_name,
       #s_spabiz_service_charge_inserts.store_number,
       #s_spabiz_service_charge_inserts.pay_commission,
       #s_spabiz_service_charge_inserts.enabled,
       #s_spabiz_service_charge_inserts.enabled_text,
       #s_spabiz_service_charge_inserts.taxable,
       #s_spabiz_service_charge_inserts.amount,
       #s_spabiz_service_charge_inserts.dollar_percent,
       #s_spabiz_service_charge_inserts.computed_on,
       case when s_spabiz_service_charge.s_spabiz_service_charge_id is null then isnull(#s_spabiz_service_charge_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_service_charge_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_service_charge_inserts
  left join p_spabiz_service_charge
    on #s_spabiz_service_charge_inserts.bk_hash = p_spabiz_service_charge.bk_hash
   and p_spabiz_service_charge.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_service_charge
    on p_spabiz_service_charge.bk_hash = s_spabiz_service_charge.bk_hash
   and p_spabiz_service_charge.s_spabiz_service_charge_id = s_spabiz_service_charge.s_spabiz_service_charge_id
 where s_spabiz_service_charge.s_spabiz_service_charge_id is null
    or (s_spabiz_service_charge.s_spabiz_service_charge_id is not null
        and s_spabiz_service_charge.dv_hash <> #s_spabiz_service_charge_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_service_charge @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_service_charge @current_dv_batch_id

end
