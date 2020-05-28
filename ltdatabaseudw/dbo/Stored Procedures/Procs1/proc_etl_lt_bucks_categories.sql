CREATE PROC [dbo].[proc_etl_lt_bucks_categories] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_lt_bucks_Categories

set @insert_date_time = getdate()
insert into dbo.stage_hash_lt_bucks_Categories (
       bk_hash,
       category_id,
       category_catalog,
       category_name,
       category_desc,
       category_parent,
       category_group,
       category_active,
       category_image,
       category_type,
       category_conversion,
       category_isdeleted,
       category_last_user,
       LastModifiedTimestamp,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(category_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       category_id,
       category_catalog,
       category_name,
       category_desc,
       category_parent,
       category_group,
       category_active,
       category_image,
       category_type,
       category_conversion,
       category_isdeleted,
       category_last_user,
       LastModifiedTimestamp,
       isnull(cast(stage_lt_bucks_Categories.LastModifiedTimestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_lt_bucks_Categories
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_lt_bucks_categories @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_lt_bucks_categories (
       bk_hash,
       category_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_lt_bucks_Categories.bk_hash,
       stage_hash_lt_bucks_Categories.category_id category_id,
       isnull(cast(stage_hash_lt_bucks_Categories.LastModifiedTimestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       5,
       @insert_date_time,
       @user
  from stage_hash_lt_bucks_Categories
  left join h_lt_bucks_categories
    on stage_hash_lt_bucks_Categories.bk_hash = h_lt_bucks_categories.bk_hash
 where h_lt_bucks_categories_id is null
   and stage_hash_lt_bucks_Categories.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_lt_bucks_categories
if object_id('tempdb..#l_lt_bucks_categories_inserts') is not null drop table #l_lt_bucks_categories_inserts
create table #l_lt_bucks_categories_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_Categories.bk_hash,
       stage_hash_lt_bucks_Categories.category_id category_id,
       stage_hash_lt_bucks_Categories.category_catalog category_catalog,
       stage_hash_lt_bucks_Categories.category_parent category_parent,
       stage_hash_lt_bucks_Categories.category_image category_image,
       stage_hash_lt_bucks_Categories.category_last_user category_last_user,
       stage_hash_lt_bucks_Categories.LastModifiedTimestamp dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Categories.category_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Categories.category_catalog as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Categories.category_parent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Categories.category_image as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Categories.category_last_user as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_Categories
 where stage_hash_lt_bucks_Categories.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_lt_bucks_categories records
set @insert_date_time = getdate()
insert into l_lt_bucks_categories (
       bk_hash,
       category_id,
       category_catalog,
       category_parent,
       category_image,
       category_last_user,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_lt_bucks_categories_inserts.bk_hash,
       #l_lt_bucks_categories_inserts.category_id,
       #l_lt_bucks_categories_inserts.category_catalog,
       #l_lt_bucks_categories_inserts.category_parent,
       #l_lt_bucks_categories_inserts.category_image,
       #l_lt_bucks_categories_inserts.category_last_user,
       case when l_lt_bucks_categories.l_lt_bucks_categories_id is null then isnull(#l_lt_bucks_categories_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #l_lt_bucks_categories_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_lt_bucks_categories_inserts
  left join p_lt_bucks_categories
    on #l_lt_bucks_categories_inserts.bk_hash = p_lt_bucks_categories.bk_hash
   and p_lt_bucks_categories.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_lt_bucks_categories
    on p_lt_bucks_categories.bk_hash = l_lt_bucks_categories.bk_hash
   and p_lt_bucks_categories.l_lt_bucks_categories_id = l_lt_bucks_categories.l_lt_bucks_categories_id
 where l_lt_bucks_categories.l_lt_bucks_categories_id is null
    or (l_lt_bucks_categories.l_lt_bucks_categories_id is not null
        and l_lt_bucks_categories.dv_hash <> #l_lt_bucks_categories_inserts.source_hash)

--calculate hash and lookup to current s_lt_bucks_categories
if object_id('tempdb..#s_lt_bucks_categories_inserts') is not null drop table #s_lt_bucks_categories_inserts
create table #s_lt_bucks_categories_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_Categories.bk_hash,
       stage_hash_lt_bucks_Categories.category_id category_id,
       stage_hash_lt_bucks_Categories.category_name category_name,
       stage_hash_lt_bucks_Categories.category_desc category_desc,
       stage_hash_lt_bucks_Categories.category_group category_group,
       stage_hash_lt_bucks_Categories.category_active category_active,
       stage_hash_lt_bucks_Categories.category_type category_type,
       stage_hash_lt_bucks_Categories.category_conversion category_conversion,
       stage_hash_lt_bucks_Categories.category_isdeleted category_isdeleted,
       stage_hash_lt_bucks_Categories.LastModifiedTimestamp last_modified_timestamp,
       stage_hash_lt_bucks_Categories.LastModifiedTimestamp dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Categories.category_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Categories.category_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Categories.category_desc,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Categories.category_group as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Categories.category_active as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Categories.category_type as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Categories.category_conversion,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Categories.category_isdeleted as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_Categories.LastModifiedTimestamp,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_Categories
 where stage_hash_lt_bucks_Categories.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_lt_bucks_categories records
set @insert_date_time = getdate()
insert into s_lt_bucks_categories (
       bk_hash,
       category_id,
       category_name,
       category_desc,
       category_group,
       category_active,
       category_type,
       category_conversion,
       category_isdeleted,
       last_modified_timestamp,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_lt_bucks_categories_inserts.bk_hash,
       #s_lt_bucks_categories_inserts.category_id,
       #s_lt_bucks_categories_inserts.category_name,
       #s_lt_bucks_categories_inserts.category_desc,
       #s_lt_bucks_categories_inserts.category_group,
       #s_lt_bucks_categories_inserts.category_active,
       #s_lt_bucks_categories_inserts.category_type,
       #s_lt_bucks_categories_inserts.category_conversion,
       #s_lt_bucks_categories_inserts.category_isdeleted,
       #s_lt_bucks_categories_inserts.last_modified_timestamp,
       case when s_lt_bucks_categories.s_lt_bucks_categories_id is null then isnull(#s_lt_bucks_categories_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #s_lt_bucks_categories_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_lt_bucks_categories_inserts
  left join p_lt_bucks_categories
    on #s_lt_bucks_categories_inserts.bk_hash = p_lt_bucks_categories.bk_hash
   and p_lt_bucks_categories.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_lt_bucks_categories
    on p_lt_bucks_categories.bk_hash = s_lt_bucks_categories.bk_hash
   and p_lt_bucks_categories.s_lt_bucks_categories_id = s_lt_bucks_categories.s_lt_bucks_categories_id
 where s_lt_bucks_categories.s_lt_bucks_categories_id is null
    or (s_lt_bucks_categories.s_lt_bucks_categories_id is not null
        and s_lt_bucks_categories.dv_hash <> #s_lt_bucks_categories_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_lt_bucks_categories @current_dv_batch_id

end
