CREATE PROC [dbo].[proc_etl_lt_bucks_category_items] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_lt_bucks_CategoryItems

set @insert_date_time = getdate()
insert into dbo.stage_hash_lt_bucks_CategoryItems (
       bk_hash,
       citem_id,
       citem_product,
       citem_category,
       citem_active,
       citem_show_inventory,
       citem_conversion,
       citem_order,
       citem_frt,
       citem_conversion_points,
       citem_date_created,
       citem_date_modified,
       citem_needs_approval,
       citem_display_start_date,
       citem_display_end_date,
       LastModifiedTimestamp,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(citem_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       citem_id,
       citem_product,
       citem_category,
       citem_active,
       citem_show_inventory,
       citem_conversion,
       citem_order,
       citem_frt,
       citem_conversion_points,
       citem_date_created,
       citem_date_modified,
       citem_needs_approval,
       citem_display_start_date,
       citem_display_end_date,
       LastModifiedTimestamp,
       isnull(cast(stage_lt_bucks_CategoryItems.citem_date_created as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_lt_bucks_CategoryItems
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_lt_bucks_category_items @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_lt_bucks_category_items (
       bk_hash,
       citem_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_lt_bucks_CategoryItems.bk_hash,
       stage_hash_lt_bucks_CategoryItems.citem_id citem_id,
       isnull(cast(stage_hash_lt_bucks_CategoryItems.citem_date_created as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       5,
       @insert_date_time,
       @user
  from stage_hash_lt_bucks_CategoryItems
  left join h_lt_bucks_category_items
    on stage_hash_lt_bucks_CategoryItems.bk_hash = h_lt_bucks_category_items.bk_hash
 where h_lt_bucks_category_items_id is null
   and stage_hash_lt_bucks_CategoryItems.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_lt_bucks_category_items
if object_id('tempdb..#l_lt_bucks_category_items_inserts') is not null drop table #l_lt_bucks_category_items_inserts
create table #l_lt_bucks_category_items_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_CategoryItems.bk_hash,
       stage_hash_lt_bucks_CategoryItems.citem_id citem_id,
       stage_hash_lt_bucks_CategoryItems.citem_product citem_product,
       stage_hash_lt_bucks_CategoryItems.citem_category citem_category,
       stage_hash_lt_bucks_CategoryItems.citem_date_created dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CategoryItems.citem_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CategoryItems.citem_product as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CategoryItems.citem_category as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_CategoryItems
 where stage_hash_lt_bucks_CategoryItems.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_lt_bucks_category_items records
set @insert_date_time = getdate()
insert into l_lt_bucks_category_items (
       bk_hash,
       citem_id,
       citem_product,
       citem_category,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_lt_bucks_category_items_inserts.bk_hash,
       #l_lt_bucks_category_items_inserts.citem_id,
       #l_lt_bucks_category_items_inserts.citem_product,
       #l_lt_bucks_category_items_inserts.citem_category,
       case when l_lt_bucks_category_items.l_lt_bucks_category_items_id is null then isnull(#l_lt_bucks_category_items_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #l_lt_bucks_category_items_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_lt_bucks_category_items_inserts
  left join p_lt_bucks_category_items
    on #l_lt_bucks_category_items_inserts.bk_hash = p_lt_bucks_category_items.bk_hash
   and p_lt_bucks_category_items.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_lt_bucks_category_items
    on p_lt_bucks_category_items.bk_hash = l_lt_bucks_category_items.bk_hash
   and p_lt_bucks_category_items.l_lt_bucks_category_items_id = l_lt_bucks_category_items.l_lt_bucks_category_items_id
 where l_lt_bucks_category_items.l_lt_bucks_category_items_id is null
    or (l_lt_bucks_category_items.l_lt_bucks_category_items_id is not null
        and l_lt_bucks_category_items.dv_hash <> #l_lt_bucks_category_items_inserts.source_hash)

--calculate hash and lookup to current s_lt_bucks_category_items
if object_id('tempdb..#s_lt_bucks_category_items_inserts') is not null drop table #s_lt_bucks_category_items_inserts
create table #s_lt_bucks_category_items_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_CategoryItems.bk_hash,
       stage_hash_lt_bucks_CategoryItems.citem_id citem_id,
       stage_hash_lt_bucks_CategoryItems.citem_active citem_active,
       stage_hash_lt_bucks_CategoryItems.citem_show_inventory citem_show_inventory,
       stage_hash_lt_bucks_CategoryItems.citem_conversion citem_conversion,
       stage_hash_lt_bucks_CategoryItems.citem_order citem_order,
       stage_hash_lt_bucks_CategoryItems.citem_frt citem_frt,
       stage_hash_lt_bucks_CategoryItems.citem_conversion_points citem_conversion_points,
       stage_hash_lt_bucks_CategoryItems.citem_date_created citem_date_created,
       stage_hash_lt_bucks_CategoryItems.citem_date_modified citem_date_modified,
       stage_hash_lt_bucks_CategoryItems.citem_needs_approval citem_needs_approval,
       stage_hash_lt_bucks_CategoryItems.citem_display_start_date citem_display_start_date,
       stage_hash_lt_bucks_CategoryItems.citem_display_end_date citem_display_end_date,
       stage_hash_lt_bucks_CategoryItems.LastModifiedTimestamp last_modified_timestamp,
       stage_hash_lt_bucks_CategoryItems.citem_date_created dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CategoryItems.citem_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CategoryItems.citem_active as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CategoryItems.citem_show_inventory as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_CategoryItems.citem_conversion,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CategoryItems.citem_order as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CategoryItems.citem_frt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_CategoryItems.citem_conversion_points,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_CategoryItems.citem_date_created,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_CategoryItems.citem_date_modified,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CategoryItems.citem_needs_approval as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_CategoryItems.citem_display_start_date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_CategoryItems.citem_display_end_date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_CategoryItems.LastModifiedTimestamp,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_CategoryItems
 where stage_hash_lt_bucks_CategoryItems.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_lt_bucks_category_items records
set @insert_date_time = getdate()
insert into s_lt_bucks_category_items (
       bk_hash,
       citem_id,
       citem_active,
       citem_show_inventory,
       citem_conversion,
       citem_order,
       citem_frt,
       citem_conversion_points,
       citem_date_created,
       citem_date_modified,
       citem_needs_approval,
       citem_display_start_date,
       citem_display_end_date,
       last_modified_timestamp,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_lt_bucks_category_items_inserts.bk_hash,
       #s_lt_bucks_category_items_inserts.citem_id,
       #s_lt_bucks_category_items_inserts.citem_active,
       #s_lt_bucks_category_items_inserts.citem_show_inventory,
       #s_lt_bucks_category_items_inserts.citem_conversion,
       #s_lt_bucks_category_items_inserts.citem_order,
       #s_lt_bucks_category_items_inserts.citem_frt,
       #s_lt_bucks_category_items_inserts.citem_conversion_points,
       #s_lt_bucks_category_items_inserts.citem_date_created,
       #s_lt_bucks_category_items_inserts.citem_date_modified,
       #s_lt_bucks_category_items_inserts.citem_needs_approval,
       #s_lt_bucks_category_items_inserts.citem_display_start_date,
       #s_lt_bucks_category_items_inserts.citem_display_end_date,
       #s_lt_bucks_category_items_inserts.last_modified_timestamp,
       case when s_lt_bucks_category_items.s_lt_bucks_category_items_id is null then isnull(#s_lt_bucks_category_items_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #s_lt_bucks_category_items_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_lt_bucks_category_items_inserts
  left join p_lt_bucks_category_items
    on #s_lt_bucks_category_items_inserts.bk_hash = p_lt_bucks_category_items.bk_hash
   and p_lt_bucks_category_items.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_lt_bucks_category_items
    on p_lt_bucks_category_items.bk_hash = s_lt_bucks_category_items.bk_hash
   and p_lt_bucks_category_items.s_lt_bucks_category_items_id = s_lt_bucks_category_items.s_lt_bucks_category_items_id
 where s_lt_bucks_category_items.s_lt_bucks_category_items_id is null
    or (s_lt_bucks_category_items.s_lt_bucks_category_items_id is not null
        and s_lt_bucks_category_items.dv_hash <> #s_lt_bucks_category_items_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_lt_bucks_category_items @current_dv_batch_id

end
