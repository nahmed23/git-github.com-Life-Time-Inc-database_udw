CREATE PROC [dbo].[proc_etl_ec_plan_items] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ec_PlanItems

set @insert_date_time = getdate()
insert into dbo.stage_hash_ec_PlanItems (
       bk_hash,
       PlanItemId,
       SourceId,
       ItemType,
       Date,
       Name,
       Description,
       Completed,
       SourceType,
       PlanId,
       CreatedDate,
       UpdatedDate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PlanItemId as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PlanItemId,
       SourceId,
       ItemType,
       Date,
       Name,
       Description,
       Completed,
       SourceType,
       PlanId,
       CreatedDate,
       UpdatedDate,
       isnull(cast(stage_ec_PlanItems.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ec_PlanItems
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ec_plan_items @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ec_plan_items (
       bk_hash,
       plan_item_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ec_PlanItems.bk_hash,
       stage_hash_ec_PlanItems.PlanItemId plan_item_id,
       isnull(cast(stage_hash_ec_PlanItems.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       34,
       @insert_date_time,
       @user
  from stage_hash_ec_PlanItems
  left join h_ec_plan_items
    on stage_hash_ec_PlanItems.bk_hash = h_ec_plan_items.bk_hash
 where h_ec_plan_items_id is null
   and stage_hash_ec_PlanItems.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ec_plan_items
if object_id('tempdb..#l_ec_plan_items_inserts') is not null drop table #l_ec_plan_items_inserts
create table #l_ec_plan_items_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_PlanItems.bk_hash,
       stage_hash_ec_PlanItems.PlanItemId plan_item_id,
       stage_hash_ec_PlanItems.PlanId plan_id,
       stage_hash_ec_PlanItems.SourceId source_id,
       isnull(cast(stage_hash_ec_PlanItems.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_PlanItems.PlanItemId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_PlanItems.PlanId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_PlanItems.SourceId,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_PlanItems
 where stage_hash_ec_PlanItems.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ec_plan_items records
set @insert_date_time = getdate()
insert into l_ec_plan_items (
       bk_hash,
       plan_item_id,
       plan_id,
       source_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ec_plan_items_inserts.bk_hash,
       #l_ec_plan_items_inserts.plan_item_id,
       #l_ec_plan_items_inserts.plan_id,
       #l_ec_plan_items_inserts.source_id,
       case when l_ec_plan_items.l_ec_plan_items_id is null then isnull(#l_ec_plan_items_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #l_ec_plan_items_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ec_plan_items_inserts
  left join p_ec_plan_items
    on #l_ec_plan_items_inserts.bk_hash = p_ec_plan_items.bk_hash
   and p_ec_plan_items.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ec_plan_items
    on p_ec_plan_items.bk_hash = l_ec_plan_items.bk_hash
   and p_ec_plan_items.l_ec_plan_items_id = l_ec_plan_items.l_ec_plan_items_id
 where l_ec_plan_items.l_ec_plan_items_id is null
    or (l_ec_plan_items.l_ec_plan_items_id is not null
        and l_ec_plan_items.dv_hash <> #l_ec_plan_items_inserts.source_hash)

--calculate hash and lookup to current s_ec_plan_items
if object_id('tempdb..#s_ec_plan_items_inserts') is not null drop table #s_ec_plan_items_inserts
create table #s_ec_plan_items_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_PlanItems.bk_hash,
       stage_hash_ec_PlanItems.PlanItemId plan_item_id,
       stage_hash_ec_PlanItems.ItemType item_type,
       stage_hash_ec_PlanItems.Date date,
       stage_hash_ec_PlanItems.Name name,
       stage_hash_ec_PlanItems.Description description,
       stage_hash_ec_PlanItems.Completed completed,
       stage_hash_ec_PlanItems.SourceType source_type,
       stage_hash_ec_PlanItems.CreatedDate created_date,
       stage_hash_ec_PlanItems.UpdatedDate updated_date,
       isnull(cast(stage_hash_ec_PlanItems.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_PlanItems.PlanItemId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_PlanItems.ItemType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_PlanItems.Date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_PlanItems.Name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_PlanItems.Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_PlanItems.Completed as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_PlanItems.SourceType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_PlanItems.CreatedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_PlanItems.UpdatedDate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_PlanItems
 where stage_hash_ec_PlanItems.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ec_plan_items records
set @insert_date_time = getdate()
insert into s_ec_plan_items (
       bk_hash,
       plan_item_id,
       item_type,
       date,
       name,
       description,
       completed,
       source_type,
       created_date,
       updated_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ec_plan_items_inserts.bk_hash,
       #s_ec_plan_items_inserts.plan_item_id,
       #s_ec_plan_items_inserts.item_type,
       #s_ec_plan_items_inserts.date,
       #s_ec_plan_items_inserts.name,
       #s_ec_plan_items_inserts.description,
       #s_ec_plan_items_inserts.completed,
       #s_ec_plan_items_inserts.source_type,
       #s_ec_plan_items_inserts.created_date,
       #s_ec_plan_items_inserts.updated_date,
       case when s_ec_plan_items.s_ec_plan_items_id is null then isnull(#s_ec_plan_items_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #s_ec_plan_items_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ec_plan_items_inserts
  left join p_ec_plan_items
    on #s_ec_plan_items_inserts.bk_hash = p_ec_plan_items.bk_hash
   and p_ec_plan_items.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ec_plan_items
    on p_ec_plan_items.bk_hash = s_ec_plan_items.bk_hash
   and p_ec_plan_items.s_ec_plan_items_id = s_ec_plan_items.s_ec_plan_items_id
 where s_ec_plan_items.s_ec_plan_items_id is null
    or (s_ec_plan_items.s_ec_plan_items_id is not null
        and s_ec_plan_items.dv_hash <> #s_ec_plan_items_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ec_plan_items @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ec_plan_items @current_dv_batch_id

end
