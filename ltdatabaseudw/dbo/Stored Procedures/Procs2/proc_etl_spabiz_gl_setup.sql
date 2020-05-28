CREATE PROC [dbo].[proc_etl_spabiz_gl_setup] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_GLSETUP

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_GLSETUP (
       bk_hash,
       STORE_NUMBER,
       DESCRIPTION,
       GLACCOUNT,
       EDITTIME,
       STATUS,
       DELETED,
       EXPENSE,
       OPTIONAL,
       RANK,
       ID,
       COUNTERID,
       STOREID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       STORE_NUMBER,
       DESCRIPTION,
       GLACCOUNT,
       EDITTIME,
       STATUS,
       DELETED,
       EXPENSE,
       OPTIONAL,
       RANK,
       ID,
       COUNTERID,
       STOREID,
       isnull(cast(stage_spabiz_GLSETUP.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_GLSETUP
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_gl_setup @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_gl_setup (
       bk_hash,
       store_number,
       gl_setup_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_GLSETUP.bk_hash,
       stage_hash_spabiz_GLSETUP.STORE_NUMBER store_number,
       stage_hash_spabiz_GLSETUP.ID gl_setup_id,
       isnull(cast(stage_hash_spabiz_GLSETUP.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_GLSETUP
  left join h_spabiz_gl_setup
    on stage_hash_spabiz_GLSETUP.bk_hash = h_spabiz_gl_setup.bk_hash
 where h_spabiz_gl_setup_id is null
   and stage_hash_spabiz_GLSETUP.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_spabiz_gl_setup
if object_id('tempdb..#s_spabiz_gl_setup_inserts') is not null drop table #s_spabiz_gl_setup_inserts
create table #s_spabiz_gl_setup_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_GLSETUP.bk_hash,
       stage_hash_spabiz_GLSETUP.STORE_NUMBER store_number,
       stage_hash_spabiz_GLSETUP.DESCRIPTION description,
       stage_hash_spabiz_GLSETUP.GLACCOUNT gl_account,
       stage_hash_spabiz_GLSETUP.EDITTIME edit_time,
       stage_hash_spabiz_GLSETUP.STATUS status,
       stage_hash_spabiz_GLSETUP.DELETED deleted,
       stage_hash_spabiz_GLSETUP.EXPENSE expense,
       stage_hash_spabiz_GLSETUP.OPTIONAL optional,
       stage_hash_spabiz_GLSETUP.RANK rank,
       stage_hash_spabiz_GLSETUP.ID gl_setup_id,
       stage_hash_spabiz_GLSETUP.COUNTERID counter_id,
       stage_hash_spabiz_GLSETUP.STOREID store_id,
       stage_hash_spabiz_GLSETUP.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_GLSETUP.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_GLSETUP.DESCRIPTION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_GLSETUP.GLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_GLSETUP.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GLSETUP.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GLSETUP.DELETED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GLSETUP.EXPENSE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GLSETUP.OPTIONAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GLSETUP.RANK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GLSETUP.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GLSETUP.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GLSETUP.STOREID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_GLSETUP
 where stage_hash_spabiz_GLSETUP.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_gl_setup records
set @insert_date_time = getdate()
insert into s_spabiz_gl_setup (
       bk_hash,
       store_number,
       description,
       gl_account,
       edit_time,
       status,
       deleted,
       expense,
       optional,
       rank,
       gl_setup_id,
       counter_id,
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_gl_setup_inserts.bk_hash,
       #s_spabiz_gl_setup_inserts.store_number,
       #s_spabiz_gl_setup_inserts.description,
       #s_spabiz_gl_setup_inserts.gl_account,
       #s_spabiz_gl_setup_inserts.edit_time,
       #s_spabiz_gl_setup_inserts.status,
       #s_spabiz_gl_setup_inserts.deleted,
       #s_spabiz_gl_setup_inserts.expense,
       #s_spabiz_gl_setup_inserts.optional,
       #s_spabiz_gl_setup_inserts.rank,
       #s_spabiz_gl_setup_inserts.gl_setup_id,
       #s_spabiz_gl_setup_inserts.counter_id,
       #s_spabiz_gl_setup_inserts.store_id,
       case when s_spabiz_gl_setup.s_spabiz_gl_setup_id is null then isnull(#s_spabiz_gl_setup_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_gl_setup_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_gl_setup_inserts
  left join p_spabiz_gl_setup
    on #s_spabiz_gl_setup_inserts.bk_hash = p_spabiz_gl_setup.bk_hash
   and p_spabiz_gl_setup.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_gl_setup
    on p_spabiz_gl_setup.bk_hash = s_spabiz_gl_setup.bk_hash
   and p_spabiz_gl_setup.s_spabiz_gl_setup_id = s_spabiz_gl_setup.s_spabiz_gl_setup_id
 where s_spabiz_gl_setup.s_spabiz_gl_setup_id is null
    or (s_spabiz_gl_setup.s_spabiz_gl_setup_id is not null
        and s_spabiz_gl_setup.dv_hash <> #s_spabiz_gl_setup_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_gl_setup @current_dv_batch_id

end
