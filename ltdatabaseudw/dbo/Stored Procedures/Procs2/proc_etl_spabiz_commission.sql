CREATE PROC [dbo].[proc_etl_spabiz_commission] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_COMMISSION

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_COMMISSION (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       USESLIDINGSCALE,
       LEVEL1VALUE,
       LEVEL1COMMISH,
       LEVEL2VALUE,
       LEVEL2COMMISH,
       LEVEL3VALUE,
       LEVEL3COMMISH,
       LEVEL4VALUE,
       LEVEL4COMMISH,
       LEVEL5VALUE,
       LEVEL5COMMISH,
       STORE_NUMBER,
       COMMISSIONBACKUPID,
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
       USESLIDINGSCALE,
       LEVEL1VALUE,
       LEVEL1COMMISH,
       LEVEL2VALUE,
       LEVEL2COMMISH,
       LEVEL3VALUE,
       LEVEL3COMMISH,
       LEVEL4VALUE,
       LEVEL4COMMISH,
       LEVEL5VALUE,
       LEVEL5COMMISH,
       STORE_NUMBER,
       COMMISSIONBACKUPID,
       isnull(cast(stage_spabiz_COMMISSION.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_COMMISSION
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_commission @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_commission (
       bk_hash,
       commission_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_COMMISSION.bk_hash,
       stage_hash_spabiz_COMMISSION.ID commission_id,
       stage_hash_spabiz_COMMISSION.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_COMMISSION.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_COMMISSION
  left join h_spabiz_commission
    on stage_hash_spabiz_COMMISSION.bk_hash = h_spabiz_commission.bk_hash
 where h_spabiz_commission_id is null
   and stage_hash_spabiz_COMMISSION.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_commission
if object_id('tempdb..#l_spabiz_commission_inserts') is not null drop table #l_spabiz_commission_inserts
create table #l_spabiz_commission_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_COMMISSION.bk_hash,
       stage_hash_spabiz_COMMISSION.ID commission_id,
       stage_hash_spabiz_COMMISSION.STOREID store_id,
       stage_hash_spabiz_COMMISSION.STORE_NUMBER store_number,
       stage_hash_spabiz_COMMISSION.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_COMMISSION
 where stage_hash_spabiz_COMMISSION.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_commission records
set @insert_date_time = getdate()
insert into l_spabiz_commission (
       bk_hash,
       commission_id,
       store_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_commission_inserts.bk_hash,
       #l_spabiz_commission_inserts.commission_id,
       #l_spabiz_commission_inserts.store_id,
       #l_spabiz_commission_inserts.store_number,
       case when l_spabiz_commission.l_spabiz_commission_id is null then isnull(#l_spabiz_commission_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_commission_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_commission_inserts
  left join p_spabiz_commission
    on #l_spabiz_commission_inserts.bk_hash = p_spabiz_commission.bk_hash
   and p_spabiz_commission.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_commission
    on p_spabiz_commission.bk_hash = l_spabiz_commission.bk_hash
   and p_spabiz_commission.l_spabiz_commission_id = l_spabiz_commission.l_spabiz_commission_id
 where l_spabiz_commission.l_spabiz_commission_id is null
    or (l_spabiz_commission.l_spabiz_commission_id is not null
        and l_spabiz_commission.dv_hash <> #l_spabiz_commission_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_commission
if object_id('tempdb..#s_spabiz_commission_inserts') is not null drop table #s_spabiz_commission_inserts
create table #s_spabiz_commission_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_COMMISSION.bk_hash,
       stage_hash_spabiz_COMMISSION.ID commission_id,
       stage_hash_spabiz_COMMISSION.COUNTERID counter_id,
       stage_hash_spabiz_COMMISSION.EDITTIME edit_time,
       stage_hash_spabiz_COMMISSION.[Delete] commission_delete,
       stage_hash_spabiz_COMMISSION.DELETEDATE delete_date,
       stage_hash_spabiz_COMMISSION.NAME name,
       stage_hash_spabiz_COMMISSION.USESLIDINGSCALE use_sliding_scale,
       stage_hash_spabiz_COMMISSION.LEVEL1VALUE level_1_value,
       stage_hash_spabiz_COMMISSION.LEVEL1COMMISH level_1_commish,
       stage_hash_spabiz_COMMISSION.LEVEL2VALUE level_2_value,
       stage_hash_spabiz_COMMISSION.LEVEL2COMMISH level_2_commish,
       stage_hash_spabiz_COMMISSION.LEVEL3VALUE level_3_value,
       stage_hash_spabiz_COMMISSION.LEVEL3COMMISH level_3_commish,
       stage_hash_spabiz_COMMISSION.LEVEL4VALUE level_4_value,
       stage_hash_spabiz_COMMISSION.LEVEL4COMMISH level_4_commish,
       stage_hash_spabiz_COMMISSION.LEVEL5VALUE level_5_value,
       stage_hash_spabiz_COMMISSION.LEVEL5COMMISH level_5_commish,
       stage_hash_spabiz_COMMISSION.STORE_NUMBER store_number,
       stage_hash_spabiz_COMMISSION.COMMISSIONBACKUPID commission_backup_id,
       stage_hash_spabiz_COMMISSION.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_COMMISSION.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_COMMISSION.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_COMMISSION.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.USESLIDINGSCALE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.LEVEL1VALUE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.LEVEL1COMMISH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.LEVEL2VALUE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.LEVEL2COMMISH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.LEVEL3VALUE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.LEVEL3COMMISH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.LEVEL4VALUE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.LEVEL4COMMISH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.LEVEL5VALUE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.LEVEL5COMMISH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_COMMISSION.COMMISSIONBACKUPID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_COMMISSION
 where stage_hash_spabiz_COMMISSION.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_commission records
set @insert_date_time = getdate()
insert into s_spabiz_commission (
       bk_hash,
       commission_id,
       counter_id,
       edit_time,
       commission_delete,
       delete_date,
       name,
       use_sliding_scale,
       level_1_value,
       level_1_commish,
       level_2_value,
       level_2_commish,
       level_3_value,
       level_3_commish,
       level_4_value,
       level_4_commish,
       level_5_value,
       level_5_commish,
       store_number,
       commission_backup_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_commission_inserts.bk_hash,
       #s_spabiz_commission_inserts.commission_id,
       #s_spabiz_commission_inserts.counter_id,
       #s_spabiz_commission_inserts.edit_time,
       #s_spabiz_commission_inserts.commission_delete,
       #s_spabiz_commission_inserts.delete_date,
       #s_spabiz_commission_inserts.name,
       #s_spabiz_commission_inserts.use_sliding_scale,
       #s_spabiz_commission_inserts.level_1_value,
       #s_spabiz_commission_inserts.level_1_commish,
       #s_spabiz_commission_inserts.level_2_value,
       #s_spabiz_commission_inserts.level_2_commish,
       #s_spabiz_commission_inserts.level_3_value,
       #s_spabiz_commission_inserts.level_3_commish,
       #s_spabiz_commission_inserts.level_4_value,
       #s_spabiz_commission_inserts.level_4_commish,
       #s_spabiz_commission_inserts.level_5_value,
       #s_spabiz_commission_inserts.level_5_commish,
       #s_spabiz_commission_inserts.store_number,
       #s_spabiz_commission_inserts.commission_backup_id,
       case when s_spabiz_commission.s_spabiz_commission_id is null then isnull(#s_spabiz_commission_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_commission_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_commission_inserts
  left join p_spabiz_commission
    on #s_spabiz_commission_inserts.bk_hash = p_spabiz_commission.bk_hash
   and p_spabiz_commission.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_commission
    on p_spabiz_commission.bk_hash = s_spabiz_commission.bk_hash
   and p_spabiz_commission.s_spabiz_commission_id = s_spabiz_commission.s_spabiz_commission_id
 where s_spabiz_commission.s_spabiz_commission_id is null
    or (s_spabiz_commission.s_spabiz_commission_id is not null
        and s_spabiz_commission.dv_hash <> #s_spabiz_commission_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_commission @current_dv_batch_id

end
