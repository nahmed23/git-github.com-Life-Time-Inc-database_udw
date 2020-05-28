CREATE PROC [dbo].[proc_etl_spabiz_category] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_CATEGORY

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_CATEGORY (
       bk_hash,
       ID,
       STORE_NUMBER,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       DATATYPE,
       PARENTID,
       FASTINDEX,
       COSMETIC,
       DISPLAYCOLOR,
       GLACCOUNT,
       LVL,
       Level,
       WEBBOOK,
       NEWID,
       CATEGORYBACKUPID,
       WEBVIEW,
       CLASS,
       DEPARTMENT,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       STORE_NUMBER,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       DATATYPE,
       PARENTID,
       FASTINDEX,
       COSMETIC,
       DISPLAYCOLOR,
       GLACCOUNT,
       LVL,
       Level,
       WEBBOOK,
       NEWID,
       CATEGORYBACKUPID,
       WEBVIEW,
       CLASS,
       DEPARTMENT,
       isnull(cast(stage_spabiz_CATEGORY.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_CATEGORY
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_category @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_category (
       bk_hash,
       category_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_CATEGORY.bk_hash,
       stage_hash_spabiz_CATEGORY.ID category_id,
       stage_hash_spabiz_CATEGORY.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_CATEGORY.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_CATEGORY
  left join h_spabiz_category
    on stage_hash_spabiz_CATEGORY.bk_hash = h_spabiz_category.bk_hash
 where h_spabiz_category_id is null
   and stage_hash_spabiz_CATEGORY.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_category
if object_id('tempdb..#l_spabiz_category_inserts') is not null drop table #l_spabiz_category_inserts
create table #l_spabiz_category_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_CATEGORY.bk_hash,
       stage_hash_spabiz_CATEGORY.ID category_id,
       stage_hash_spabiz_CATEGORY.STORE_NUMBER store_number,
       stage_hash_spabiz_CATEGORY.COUNTERID counter_id,
       stage_hash_spabiz_CATEGORY.STOREID store_id,
       stage_hash_spabiz_CATEGORY.DATATYPE data_type,
       stage_hash_spabiz_CATEGORY.PARENTID parent_id,
       stage_hash_spabiz_CATEGORY.GLACCOUNT gl_account,
       stage_hash_spabiz_CATEGORY.NEWID new_id,
       stage_hash_spabiz_CATEGORY.CATEGORYBACKUPID category_backup_id,
       stage_hash_spabiz_CATEGORY.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.DATATYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.PARENTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CATEGORY.GLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.NEWID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.CATEGORYBACKUPID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_CATEGORY
 where stage_hash_spabiz_CATEGORY.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_category records
set @insert_date_time = getdate()
insert into l_spabiz_category (
       bk_hash,
       category_id,
       store_number,
       counter_id,
       store_id,
       data_type,
       parent_id,
       gl_account,
       new_id,
       category_backup_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_category_inserts.bk_hash,
       #l_spabiz_category_inserts.category_id,
       #l_spabiz_category_inserts.store_number,
       #l_spabiz_category_inserts.counter_id,
       #l_spabiz_category_inserts.store_id,
       #l_spabiz_category_inserts.data_type,
       #l_spabiz_category_inserts.parent_id,
       #l_spabiz_category_inserts.gl_account,
       #l_spabiz_category_inserts.new_id,
       #l_spabiz_category_inserts.category_backup_id,
       case when l_spabiz_category.l_spabiz_category_id is null then isnull(#l_spabiz_category_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_category_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_category_inserts
  left join p_spabiz_category
    on #l_spabiz_category_inserts.bk_hash = p_spabiz_category.bk_hash
   and p_spabiz_category.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_category
    on p_spabiz_category.bk_hash = l_spabiz_category.bk_hash
   and p_spabiz_category.l_spabiz_category_id = l_spabiz_category.l_spabiz_category_id
 where l_spabiz_category.l_spabiz_category_id is null
    or (l_spabiz_category.l_spabiz_category_id is not null
        and l_spabiz_category.dv_hash <> #l_spabiz_category_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_category
if object_id('tempdb..#s_spabiz_category_inserts') is not null drop table #s_spabiz_category_inserts
create table #s_spabiz_category_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_CATEGORY.bk_hash,
       stage_hash_spabiz_CATEGORY.ID category_id,
       stage_hash_spabiz_CATEGORY.STORE_NUMBER store_number,
       stage_hash_spabiz_CATEGORY.EDITTIME edit_time,
       stage_hash_spabiz_CATEGORY.[Delete] deleted,
       stage_hash_spabiz_CATEGORY.DELETEDATE delete_date,
       stage_hash_spabiz_CATEGORY.NAME name,
       stage_hash_spabiz_CATEGORY.QUICKID quick_id,
       stage_hash_spabiz_CATEGORY.FASTINDEX fast_index,
       stage_hash_spabiz_CATEGORY.COSMETIC cosmetic,
       stage_hash_spabiz_CATEGORY.DISPLAYCOLOR display_color,
       stage_hash_spabiz_CATEGORY.LVL lvl,
       stage_hash_spabiz_CATEGORY.Level level,
       stage_hash_spabiz_CATEGORY.WEBBOOK web_book,
       stage_hash_spabiz_CATEGORY.WEBVIEW web_view,
       stage_hash_spabiz_CATEGORY.CLASS class,
       stage_hash_spabiz_CATEGORY.DEPARTMENT department,
       stage_hash_spabiz_CATEGORY.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CATEGORY.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CATEGORY.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CATEGORY.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CATEGORY.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CATEGORY.FASTINDEX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.COSMETIC as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CATEGORY.DISPLAYCOLOR,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.LVL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.Level as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.WEBBOOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.WEBVIEW as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CATEGORY.CLASS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CATEGORY.DEPARTMENT as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_CATEGORY
 where stage_hash_spabiz_CATEGORY.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_category records
set @insert_date_time = getdate()
insert into s_spabiz_category (
       bk_hash,
       category_id,
       store_number,
       edit_time,
       deleted,
       delete_date,
       name,
       quick_id,
       fast_index,
       cosmetic,
       display_color,
       lvl,
       level,
       web_book,
       web_view,
       class,
       department,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_category_inserts.bk_hash,
       #s_spabiz_category_inserts.category_id,
       #s_spabiz_category_inserts.store_number,
       #s_spabiz_category_inserts.edit_time,
       #s_spabiz_category_inserts.deleted,
       #s_spabiz_category_inserts.delete_date,
       #s_spabiz_category_inserts.name,
       #s_spabiz_category_inserts.quick_id,
       #s_spabiz_category_inserts.fast_index,
       #s_spabiz_category_inserts.cosmetic,
       #s_spabiz_category_inserts.display_color,
       #s_spabiz_category_inserts.lvl,
       #s_spabiz_category_inserts.level,
       #s_spabiz_category_inserts.web_book,
       #s_spabiz_category_inserts.web_view,
       #s_spabiz_category_inserts.class,
       #s_spabiz_category_inserts.department,
       case when s_spabiz_category.s_spabiz_category_id is null then isnull(#s_spabiz_category_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_category_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_category_inserts
  left join p_spabiz_category
    on #s_spabiz_category_inserts.bk_hash = p_spabiz_category.bk_hash
   and p_spabiz_category.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_category
    on p_spabiz_category.bk_hash = s_spabiz_category.bk_hash
   and p_spabiz_category.s_spabiz_category_id = s_spabiz_category.s_spabiz_category_id
 where s_spabiz_category.s_spabiz_category_id is null
    or (s_spabiz_category.s_spabiz_category_id is not null
        and s_spabiz_category.dv_hash <> #s_spabiz_category_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_category @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_category @current_dv_batch_id

end
