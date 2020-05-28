CREATE PROC [dbo].[proc_etl_spabiz_package] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_PACKAGE

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_PACKAGE (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       RETAILPRICE,
       DEPTCAT,
       TYPE,
       TAXABLE,
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
       QUICKID,
       RETAILPRICE,
       DEPTCAT,
       TYPE,
       TAXABLE,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_PACKAGE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_PACKAGE
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_package @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_package (
       bk_hash,
       package_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_PACKAGE.bk_hash,
       stage_hash_spabiz_PACKAGE.ID package_id,
       stage_hash_spabiz_PACKAGE.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_PACKAGE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_PACKAGE
  left join h_spabiz_package
    on stage_hash_spabiz_PACKAGE.bk_hash = h_spabiz_package.bk_hash
 where h_spabiz_package_id is null
   and stage_hash_spabiz_PACKAGE.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_package
if object_id('tempdb..#l_spabiz_package_inserts') is not null drop table #l_spabiz_package_inserts
create table #l_spabiz_package_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PACKAGE.bk_hash,
       stage_hash_spabiz_PACKAGE.ID package_id,
       stage_hash_spabiz_PACKAGE.STOREID store_id,
       stage_hash_spabiz_PACKAGE.DEPTCAT dept_cat,
       stage_hash_spabiz_PACKAGE.STORE_NUMBER store_number,
       stage_hash_spabiz_PACKAGE.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.DEPTCAT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PACKAGE
 where stage_hash_spabiz_PACKAGE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_package records
set @insert_date_time = getdate()
insert into l_spabiz_package (
       bk_hash,
       package_id,
       store_id,
       dept_cat,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_package_inserts.bk_hash,
       #l_spabiz_package_inserts.package_id,
       #l_spabiz_package_inserts.store_id,
       #l_spabiz_package_inserts.dept_cat,
       #l_spabiz_package_inserts.store_number,
       case when l_spabiz_package.l_spabiz_package_id is null then isnull(#l_spabiz_package_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_package_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_package_inserts
  left join p_spabiz_package
    on #l_spabiz_package_inserts.bk_hash = p_spabiz_package.bk_hash
   and p_spabiz_package.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_package
    on p_spabiz_package.bk_hash = l_spabiz_package.bk_hash
   and p_spabiz_package.l_spabiz_package_id = l_spabiz_package.l_spabiz_package_id
 where l_spabiz_package.l_spabiz_package_id is null
    or (l_spabiz_package.l_spabiz_package_id is not null
        and l_spabiz_package.dv_hash <> #l_spabiz_package_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_package
if object_id('tempdb..#s_spabiz_package_inserts') is not null drop table #s_spabiz_package_inserts
create table #s_spabiz_package_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PACKAGE.bk_hash,
       stage_hash_spabiz_PACKAGE.ID package_id,
       stage_hash_spabiz_PACKAGE.COUNTERID counter_id,
       stage_hash_spabiz_PACKAGE.EDITTIME edit_time,
       stage_hash_spabiz_PACKAGE.[Delete] package_delete,
       stage_hash_spabiz_PACKAGE.DELETEDATE delete_date,
       stage_hash_spabiz_PACKAGE.NAME name,
       stage_hash_spabiz_PACKAGE.QUICKID quick_id,
       stage_hash_spabiz_PACKAGE.RETAILPRICE retail_price,
       stage_hash_spabiz_PACKAGE.TYPE type,
       stage_hash_spabiz_PACKAGE.TAXABLE taxable,
       stage_hash_spabiz_PACKAGE.STORE_NUMBER store_number,
       stage_hash_spabiz_PACKAGE.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PACKAGE.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PACKAGE.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PACKAGE.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PACKAGE.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.RETAILPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.TYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.TAXABLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PACKAGE.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PACKAGE
 where stage_hash_spabiz_PACKAGE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_package records
set @insert_date_time = getdate()
insert into s_spabiz_package (
       bk_hash,
       package_id,
       counter_id,
       edit_time,
       package_delete,
       delete_date,
       name,
       quick_id,
       retail_price,
       type,
       taxable,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_package_inserts.bk_hash,
       #s_spabiz_package_inserts.package_id,
       #s_spabiz_package_inserts.counter_id,
       #s_spabiz_package_inserts.edit_time,
       #s_spabiz_package_inserts.package_delete,
       #s_spabiz_package_inserts.delete_date,
       #s_spabiz_package_inserts.name,
       #s_spabiz_package_inserts.quick_id,
       #s_spabiz_package_inserts.retail_price,
       #s_spabiz_package_inserts.type,
       #s_spabiz_package_inserts.taxable,
       #s_spabiz_package_inserts.store_number,
       case when s_spabiz_package.s_spabiz_package_id is null then isnull(#s_spabiz_package_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_package_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_package_inserts
  left join p_spabiz_package
    on #s_spabiz_package_inserts.bk_hash = p_spabiz_package.bk_hash
   and p_spabiz_package.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_package
    on p_spabiz_package.bk_hash = s_spabiz_package.bk_hash
   and p_spabiz_package.s_spabiz_package_id = s_spabiz_package.s_spabiz_package_id
 where s_spabiz_package.s_spabiz_package_id is null
    or (s_spabiz_package.s_spabiz_package_id is not null
        and s_spabiz_package.dv_hash <> #s_spabiz_package_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_package @current_dv_batch_id

end
