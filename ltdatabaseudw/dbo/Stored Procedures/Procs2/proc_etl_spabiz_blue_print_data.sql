CREATE PROC [dbo].[proc_etl_spabiz_blue_print_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_BLUEPRINTDATA

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_BLUEPRINTDATA (
       bk_hash,
       ID,
       ANSWER,
       ANSWERTEXT,
       STORE_NUMBER,
       COUNTERID,
       STOREID,
       EDITTIME,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       ANSWER,
       ANSWERTEXT,
       STORE_NUMBER,
       COUNTERID,
       STOREID,
       EDITTIME,
       isnull(cast(stage_spabiz_BLUEPRINTDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_BLUEPRINTDATA
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_blue_print_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_blue_print_data (
       bk_hash,
       blue_print_data_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_BLUEPRINTDATA.bk_hash,
       stage_hash_spabiz_BLUEPRINTDATA.ID blue_print_data_id,
       stage_hash_spabiz_BLUEPRINTDATA.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_BLUEPRINTDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_BLUEPRINTDATA
  left join h_spabiz_blue_print_data
    on stage_hash_spabiz_BLUEPRINTDATA.bk_hash = h_spabiz_blue_print_data.bk_hash
 where h_spabiz_blue_print_data_id is null
   and stage_hash_spabiz_BLUEPRINTDATA.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_blue_print_data
if object_id('tempdb..#l_spabiz_blue_print_data_inserts') is not null drop table #l_spabiz_blue_print_data_inserts
create table #l_spabiz_blue_print_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_BLUEPRINTDATA.bk_hash,
       stage_hash_spabiz_BLUEPRINTDATA.ID blue_print_data_id,
       stage_hash_spabiz_BLUEPRINTDATA.STORE_NUMBER store_number,
       stage_hash_spabiz_BLUEPRINTDATA.STOREID store_id,
       stage_hash_spabiz_BLUEPRINTDATA.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLUEPRINTDATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLUEPRINTDATA.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLUEPRINTDATA.STOREID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_BLUEPRINTDATA
 where stage_hash_spabiz_BLUEPRINTDATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_blue_print_data records
set @insert_date_time = getdate()
insert into l_spabiz_blue_print_data (
       bk_hash,
       blue_print_data_id,
       store_number,
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_blue_print_data_inserts.bk_hash,
       #l_spabiz_blue_print_data_inserts.blue_print_data_id,
       #l_spabiz_blue_print_data_inserts.store_number,
       #l_spabiz_blue_print_data_inserts.store_id,
       case when l_spabiz_blue_print_data.l_spabiz_blue_print_data_id is null then isnull(#l_spabiz_blue_print_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_blue_print_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_blue_print_data_inserts
  left join p_spabiz_blue_print_data
    on #l_spabiz_blue_print_data_inserts.bk_hash = p_spabiz_blue_print_data.bk_hash
   and p_spabiz_blue_print_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_blue_print_data
    on p_spabiz_blue_print_data.bk_hash = l_spabiz_blue_print_data.bk_hash
   and p_spabiz_blue_print_data.l_spabiz_blue_print_data_id = l_spabiz_blue_print_data.l_spabiz_blue_print_data_id
 where l_spabiz_blue_print_data.l_spabiz_blue_print_data_id is null
    or (l_spabiz_blue_print_data.l_spabiz_blue_print_data_id is not null
        and l_spabiz_blue_print_data.dv_hash <> #l_spabiz_blue_print_data_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_blue_print_data
if object_id('tempdb..#s_spabiz_blue_print_data_inserts') is not null drop table #s_spabiz_blue_print_data_inserts
create table #s_spabiz_blue_print_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_BLUEPRINTDATA.bk_hash,
       stage_hash_spabiz_BLUEPRINTDATA.ID blue_print_data_id,
       stage_hash_spabiz_BLUEPRINTDATA.ANSWER answer,
       stage_hash_spabiz_BLUEPRINTDATA.ANSWERTEXT answer_text,
       stage_hash_spabiz_BLUEPRINTDATA.STORE_NUMBER store_number,
       stage_hash_spabiz_BLUEPRINTDATA.COUNTERID counter_id,
       stage_hash_spabiz_BLUEPRINTDATA.EDITTIME edit_time,
       stage_hash_spabiz_BLUEPRINTDATA.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLUEPRINTDATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_BLUEPRINTDATA.ANSWER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_BLUEPRINTDATA.ANSWERTEXT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLUEPRINTDATA.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLUEPRINTDATA.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_BLUEPRINTDATA.EDITTIME,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_BLUEPRINTDATA
 where stage_hash_spabiz_BLUEPRINTDATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_blue_print_data records
set @insert_date_time = getdate()
insert into s_spabiz_blue_print_data (
       bk_hash,
       blue_print_data_id,
       answer,
       answer_text,
       store_number,
       counter_id,
       edit_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_blue_print_data_inserts.bk_hash,
       #s_spabiz_blue_print_data_inserts.blue_print_data_id,
       #s_spabiz_blue_print_data_inserts.answer,
       #s_spabiz_blue_print_data_inserts.answer_text,
       #s_spabiz_blue_print_data_inserts.store_number,
       #s_spabiz_blue_print_data_inserts.counter_id,
       #s_spabiz_blue_print_data_inserts.edit_time,
       case when s_spabiz_blue_print_data.s_spabiz_blue_print_data_id is null then isnull(#s_spabiz_blue_print_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_blue_print_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_blue_print_data_inserts
  left join p_spabiz_blue_print_data
    on #s_spabiz_blue_print_data_inserts.bk_hash = p_spabiz_blue_print_data.bk_hash
   and p_spabiz_blue_print_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_blue_print_data
    on p_spabiz_blue_print_data.bk_hash = s_spabiz_blue_print_data.bk_hash
   and p_spabiz_blue_print_data.s_spabiz_blue_print_data_id = s_spabiz_blue_print_data.s_spabiz_blue_print_data_id
 where s_spabiz_blue_print_data.s_spabiz_blue_print_data_id is null
    or (s_spabiz_blue_print_data.s_spabiz_blue_print_data_id is not null
        and s_spabiz_blue_print_data.dv_hash <> #s_spabiz_blue_print_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_blue_print_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_blue_print_data @current_dv_batch_id

end
