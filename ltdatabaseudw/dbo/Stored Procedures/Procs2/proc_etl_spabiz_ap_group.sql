CREATE PROC [dbo].[proc_etl_spabiz_ap_group] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_APGROUP

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_APGROUP (
       bk_hash,
       ID,
       STORE_NUMBER,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       COLS,
       QUICKKEY,
       TAB,
       TABORDER,
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
       COLS,
       QUICKKEY,
       TAB,
       TABORDER,
       isnull(cast(stage_spabiz_APGROUP.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_APGROUP
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ap_group @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ap_group (
       bk_hash,
       ap_group_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_APGROUP.bk_hash,
       stage_hash_spabiz_APGROUP.ID ap_group_id,
       stage_hash_spabiz_APGROUP.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_APGROUP.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_APGROUP
  left join h_spabiz_ap_group
    on stage_hash_spabiz_APGROUP.bk_hash = h_spabiz_ap_group.bk_hash
 where h_spabiz_ap_group_id is null
   and stage_hash_spabiz_APGROUP.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ap_group
if object_id('tempdb..#l_spabiz_ap_group_inserts') is not null drop table #l_spabiz_ap_group_inserts
create table #l_spabiz_ap_group_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_APGROUP.bk_hash,
       stage_hash_spabiz_APGROUP.ID ap_group_id,
       stage_hash_spabiz_APGROUP.STORE_NUMBER store_number,
       stage_hash_spabiz_APGROUP.COUNTERID counter_id,
       stage_hash_spabiz_APGROUP.STOREID store_id,
       stage_hash_spabiz_APGROUP.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_APGROUP.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APGROUP.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APGROUP.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APGROUP.STOREID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_APGROUP
 where stage_hash_spabiz_APGROUP.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ap_group records
set @insert_date_time = getdate()
insert into l_spabiz_ap_group (
       bk_hash,
       ap_group_id,
       store_number,
       counter_id,
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_ap_group_inserts.bk_hash,
       #l_spabiz_ap_group_inserts.ap_group_id,
       #l_spabiz_ap_group_inserts.store_number,
       #l_spabiz_ap_group_inserts.counter_id,
       #l_spabiz_ap_group_inserts.store_id,
       case when l_spabiz_ap_group.l_spabiz_ap_group_id is null then isnull(#l_spabiz_ap_group_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ap_group_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ap_group_inserts
  left join p_spabiz_ap_group
    on #l_spabiz_ap_group_inserts.bk_hash = p_spabiz_ap_group.bk_hash
   and p_spabiz_ap_group.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ap_group
    on p_spabiz_ap_group.bk_hash = l_spabiz_ap_group.bk_hash
   and p_spabiz_ap_group.l_spabiz_ap_group_id = l_spabiz_ap_group.l_spabiz_ap_group_id
 where l_spabiz_ap_group.l_spabiz_ap_group_id is null
    or (l_spabiz_ap_group.l_spabiz_ap_group_id is not null
        and l_spabiz_ap_group.dv_hash <> #l_spabiz_ap_group_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ap_group
if object_id('tempdb..#s_spabiz_ap_group_inserts') is not null drop table #s_spabiz_ap_group_inserts
create table #s_spabiz_ap_group_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_APGROUP.bk_hash,
       stage_hash_spabiz_APGROUP.ID ap_group_id,
       stage_hash_spabiz_APGROUP.STORE_NUMBER store_number,
       stage_hash_spabiz_APGROUP.EDITTIME edit_time,
       stage_hash_spabiz_APGROUP.[Delete] deleted,
       stage_hash_spabiz_APGROUP.DELETEDATE delete_date,
       stage_hash_spabiz_APGROUP.NAME name,
       stage_hash_spabiz_APGROUP.COLS cols,
       stage_hash_spabiz_APGROUP.QUICKKEY quick_key,
       stage_hash_spabiz_APGROUP.TAB tab,
       stage_hash_spabiz_APGROUP.TABORDER tab_order,
       stage_hash_spabiz_APGROUP.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_APGROUP.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APGROUP.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_APGROUP.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APGROUP.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_APGROUP.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APGROUP.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APGROUP.COLS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APGROUP.QUICKKEY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APGROUP.TAB as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APGROUP.TABORDER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_APGROUP
 where stage_hash_spabiz_APGROUP.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ap_group records
set @insert_date_time = getdate()
insert into s_spabiz_ap_group (
       bk_hash,
       ap_group_id,
       store_number,
       edit_time,
       deleted,
       delete_date,
       name,
       cols,
       quick_key,
       tab,
       tab_order,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ap_group_inserts.bk_hash,
       #s_spabiz_ap_group_inserts.ap_group_id,
       #s_spabiz_ap_group_inserts.store_number,
       #s_spabiz_ap_group_inserts.edit_time,
       #s_spabiz_ap_group_inserts.deleted,
       #s_spabiz_ap_group_inserts.delete_date,
       #s_spabiz_ap_group_inserts.name,
       #s_spabiz_ap_group_inserts.cols,
       #s_spabiz_ap_group_inserts.quick_key,
       #s_spabiz_ap_group_inserts.tab,
       #s_spabiz_ap_group_inserts.tab_order,
       case when s_spabiz_ap_group.s_spabiz_ap_group_id is null then isnull(#s_spabiz_ap_group_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ap_group_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ap_group_inserts
  left join p_spabiz_ap_group
    on #s_spabiz_ap_group_inserts.bk_hash = p_spabiz_ap_group.bk_hash
   and p_spabiz_ap_group.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ap_group
    on p_spabiz_ap_group.bk_hash = s_spabiz_ap_group.bk_hash
   and p_spabiz_ap_group.s_spabiz_ap_group_id = s_spabiz_ap_group.s_spabiz_ap_group_id
 where s_spabiz_ap_group.s_spabiz_ap_group_id is null
    or (s_spabiz_ap_group.s_spabiz_ap_group_id is not null
        and s_spabiz_ap_group.dv_hash <> #s_spabiz_ap_group_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ap_group @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ap_group @current_dv_batch_id

end
