CREATE PROC [dbo].[proc_etl_spabiz_tax] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_TAX

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_TAX (
       bk_hash,
       [ID],
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       TAXAUTHNAME,
       DEPT,
       ADDRESS1,
       ADDRESS2,
       CITY,
       STATE,
       ZIP,
       PHONE,
       CONTACT,
       CONTACTTITLE,
       REPORTCYCLE,
       TAXTYPE,
       AMOUNT,
       NODEID,
       STORE_NUMBER,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([ID] as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [ID],
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       TAXAUTHNAME,
       DEPT,
       ADDRESS1,
       ADDRESS2,
       CITY,
       STATE,
       ZIP,
       PHONE,
       CONTACT,
       CONTACTTITLE,
       REPORTCYCLE,
       TAXTYPE,
       AMOUNT,
       NODEID,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_TAX.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_TAX
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_tax @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_tax (
       bk_hash,
       tax_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_TAX.bk_hash,
       stage_hash_spabiz_TAX.[ID] tax_id,
       stage_hash_spabiz_TAX.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_TAX.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_TAX
  left join h_spabiz_tax
    on stage_hash_spabiz_TAX.bk_hash = h_spabiz_tax.bk_hash
 where h_spabiz_tax_id is null
   and stage_hash_spabiz_TAX.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_tax
if object_id('tempdb..#l_spabiz_tax_inserts') is not null drop table #l_spabiz_tax_inserts
create table #l_spabiz_tax_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TAX.bk_hash,
       stage_hash_spabiz_TAX.[ID] tax_id,
       stage_hash_spabiz_TAX.COUNTERID counter_id,
       stage_hash_spabiz_TAX.STOREID store_id,
       stage_hash_spabiz_TAX.NODEID node_id,
       stage_hash_spabiz_TAX.STORE_NUMBER store_number,
       stage_hash_spabiz_TAX.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.[ID] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.NODEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TAX
 where stage_hash_spabiz_TAX.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_tax records
set @insert_date_time = getdate()
insert into l_spabiz_tax (
       bk_hash,
       tax_id,
       counter_id,
       store_id,
       node_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_tax_inserts.bk_hash,
       #l_spabiz_tax_inserts.tax_id,
       #l_spabiz_tax_inserts.counter_id,
       #l_spabiz_tax_inserts.store_id,
       #l_spabiz_tax_inserts.node_id,
       #l_spabiz_tax_inserts.store_number,
       case when l_spabiz_tax.l_spabiz_tax_id is null then isnull(#l_spabiz_tax_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_tax_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_tax_inserts
  left join p_spabiz_tax
    on #l_spabiz_tax_inserts.bk_hash = p_spabiz_tax.bk_hash
   and p_spabiz_tax.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_tax
    on p_spabiz_tax.bk_hash = l_spabiz_tax.bk_hash
   and p_spabiz_tax.l_spabiz_tax_id = l_spabiz_tax.l_spabiz_tax_id
 where l_spabiz_tax.l_spabiz_tax_id is null
    or (l_spabiz_tax.l_spabiz_tax_id is not null
        and l_spabiz_tax.dv_hash <> #l_spabiz_tax_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_tax
if object_id('tempdb..#s_spabiz_tax_inserts') is not null drop table #s_spabiz_tax_inserts
create table #s_spabiz_tax_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TAX.bk_hash,
       stage_hash_spabiz_TAX.[ID] tax_id,
       stage_hash_spabiz_TAX.EDITTIME edit_time,
       stage_hash_spabiz_TAX.[Delete] tax_delete,
       stage_hash_spabiz_TAX.DELETEDATE delete_date,
       stage_hash_spabiz_TAX.NAME name,
       stage_hash_spabiz_TAX.QUICKID quick_id,
       stage_hash_spabiz_TAX.TAXAUTHNAME tax_auth_name,
       stage_hash_spabiz_TAX.DEPT dept,
       stage_hash_spabiz_TAX.ADDRESS1 address_1,
       stage_hash_spabiz_TAX.ADDRESS2 address_2,
       stage_hash_spabiz_TAX.CITY city,
       stage_hash_spabiz_TAX.STATE state,
       stage_hash_spabiz_TAX.ZIP zip,
       stage_hash_spabiz_TAX.PHONE phone,
       stage_hash_spabiz_TAX.CONTACT contact,
       stage_hash_spabiz_TAX.CONTACTTITLE contact_title,
       stage_hash_spabiz_TAX.REPORTCYCLE report_cycle,
       stage_hash_spabiz_TAX.TAXTYPE tax_type,
       stage_hash_spabiz_TAX.AMOUNT amount,
       stage_hash_spabiz_TAX.STORE_NUMBER store_number,
       stage_hash_spabiz_TAX.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.[ID] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TAX.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TAX.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.TAXAUTHNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.DEPT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.ADDRESS1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.ADDRESS2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.CITY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.STATE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.ZIP,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.PHONE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.CONTACT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TAX.CONTACTTITLE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.REPORTCYCLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.TAXTYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.AMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TAX.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TAX
 where stage_hash_spabiz_TAX.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_tax records
set @insert_date_time = getdate()
insert into s_spabiz_tax (
       bk_hash,
       tax_id,
       edit_time,
       tax_delete,
       delete_date,
       name,
       quick_id,
       tax_auth_name,
       dept,
       address_1,
       address_2,
       city,
       state,
       zip,
       phone,
       contact,
       contact_title,
       report_cycle,
       tax_type,
       amount,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_tax_inserts.bk_hash,
       #s_spabiz_tax_inserts.tax_id,
       #s_spabiz_tax_inserts.edit_time,
       #s_spabiz_tax_inserts.tax_delete,
       #s_spabiz_tax_inserts.delete_date,
       #s_spabiz_tax_inserts.name,
       #s_spabiz_tax_inserts.quick_id,
       #s_spabiz_tax_inserts.tax_auth_name,
       #s_spabiz_tax_inserts.dept,
       #s_spabiz_tax_inserts.address_1,
       #s_spabiz_tax_inserts.address_2,
       #s_spabiz_tax_inserts.city,
       #s_spabiz_tax_inserts.state,
       #s_spabiz_tax_inserts.zip,
       #s_spabiz_tax_inserts.phone,
       #s_spabiz_tax_inserts.contact,
       #s_spabiz_tax_inserts.contact_title,
       #s_spabiz_tax_inserts.report_cycle,
       #s_spabiz_tax_inserts.tax_type,
       #s_spabiz_tax_inserts.amount,
       #s_spabiz_tax_inserts.store_number,
       case when s_spabiz_tax.s_spabiz_tax_id is null then isnull(#s_spabiz_tax_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_tax_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_tax_inserts
  left join p_spabiz_tax
    on #s_spabiz_tax_inserts.bk_hash = p_spabiz_tax.bk_hash
   and p_spabiz_tax.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_tax
    on p_spabiz_tax.bk_hash = s_spabiz_tax.bk_hash
   and p_spabiz_tax.s_spabiz_tax_id = s_spabiz_tax.s_spabiz_tax_id
 where s_spabiz_tax.s_spabiz_tax_id is null
    or (s_spabiz_tax.s_spabiz_tax_id is not null
        and s_spabiz_tax.dv_hash <> #s_spabiz_tax_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_tax @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_tax @current_dv_batch_id

end
