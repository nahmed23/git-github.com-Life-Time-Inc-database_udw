CREATE PROC [dbo].[proc_etl_boss_asi_prod_kit] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_asiprodkit

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_asiprodkit (
       bk_hash,
       parent_upc,
       child_upc,
       sort_order,
       duration,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(parent_upc,'z#@$k%&P')+'P%#&z$@k'+isnull(child_upc,'z#@$k%&P'))),2) bk_hash,
       parent_upc,
       child_upc,
       sort_order,
       duration,
       jan_one,
       isnull(cast(stage_boss_asiprodkit.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_asiprodkit
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_asi_prod_kit @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_asi_prod_kit (
       bk_hash,
       parent_upc,
       child_upc,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_asiprodkit.bk_hash,
       stage_hash_boss_asiprodkit.parent_upc parent_upc,
       stage_hash_boss_asiprodkit.child_upc child_upc,
       isnull(cast(stage_hash_boss_asiprodkit.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_asiprodkit
  left join h_boss_asi_prod_kit
    on stage_hash_boss_asiprodkit.bk_hash = h_boss_asi_prod_kit.bk_hash
 where h_boss_asi_prod_kit_id is null
   and stage_hash_boss_asiprodkit.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_boss_asi_prod_kit
if object_id('tempdb..#s_boss_asi_prod_kit_inserts') is not null drop table #s_boss_asi_prod_kit_inserts
create table #s_boss_asi_prod_kit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiprodkit.bk_hash,
       stage_hash_boss_asiprodkit.parent_upc parent_upc,
       stage_hash_boss_asiprodkit.child_upc child_upc,
       stage_hash_boss_asiprodkit.sort_order sort_order,
       stage_hash_boss_asiprodkit.duration duration,
       stage_hash_boss_asiprodkit.jan_one jan_one,
       isnull(cast(stage_hash_boss_asiprodkit.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_boss_asiprodkit.parent_upc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiprodkit.child_upc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiprodkit.sort_order as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiprodkit.duration as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiprodkit.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiprodkit
 where stage_hash_boss_asiprodkit.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_asi_prod_kit records
set @insert_date_time = getdate()
insert into s_boss_asi_prod_kit (
       bk_hash,
       parent_upc,
       child_upc,
       sort_order,
       duration,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_asi_prod_kit_inserts.bk_hash,
       #s_boss_asi_prod_kit_inserts.parent_upc,
       #s_boss_asi_prod_kit_inserts.child_upc,
       #s_boss_asi_prod_kit_inserts.sort_order,
       #s_boss_asi_prod_kit_inserts.duration,
       #s_boss_asi_prod_kit_inserts.jan_one,
       case when s_boss_asi_prod_kit.s_boss_asi_prod_kit_id is null then isnull(#s_boss_asi_prod_kit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_asi_prod_kit_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_asi_prod_kit_inserts
  left join p_boss_asi_prod_kit
    on #s_boss_asi_prod_kit_inserts.bk_hash = p_boss_asi_prod_kit.bk_hash
   and p_boss_asi_prod_kit.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_asi_prod_kit
    on p_boss_asi_prod_kit.bk_hash = s_boss_asi_prod_kit.bk_hash
   and p_boss_asi_prod_kit.s_boss_asi_prod_kit_id = s_boss_asi_prod_kit.s_boss_asi_prod_kit_id
 where s_boss_asi_prod_kit.s_boss_asi_prod_kit_id is null
    or (s_boss_asi_prod_kit.s_boss_asi_prod_kit_id is not null
        and s_boss_asi_prod_kit.dv_hash <> #s_boss_asi_prod_kit_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_asi_prod_kit @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_asi_prod_kit @current_dv_batch_id

end
