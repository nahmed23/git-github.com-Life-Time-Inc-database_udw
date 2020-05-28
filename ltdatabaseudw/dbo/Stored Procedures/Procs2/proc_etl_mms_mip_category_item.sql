CREATE PROC [dbo].[proc_etl_mms_mip_category_item] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_MIPCategoryItem

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MIPCategoryItem (
       bk_hash,
       MIPCategoryItemID,
       ValMIPCategoryID,
       ValMIPSubCategoryID,
       ValMIPItemID,
       ActiveFlag,
       AllowCommentFlag,
       SortOrder,
       InsertedDateTime,
       UpdatedDateTime,
       ValMIPInterestCategoryID,
       ProspectEnabledFlag,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MIPCategoryItemID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MIPCategoryItemID,
       ValMIPCategoryID,
       ValMIPSubCategoryID,
       ValMIPItemID,
       ActiveFlag,
       AllowCommentFlag,
       SortOrder,
       InsertedDateTime,
       UpdatedDateTime,
       ValMIPInterestCategoryID,
       ProspectEnabledFlag,
       isnull(cast(stage_mms_MIPCategoryItem.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_MIPCategoryItem
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_mip_category_item @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_mip_category_item (
       bk_hash,
       mip_category_item_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_MIPCategoryItem.bk_hash,
       stage_hash_mms_MIPCategoryItem.MIPCategoryItemID mip_category_item_id,
       isnull(cast(stage_hash_mms_MIPCategoryItem.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MIPCategoryItem
  left join h_mms_mip_category_item
    on stage_hash_mms_MIPCategoryItem.bk_hash = h_mms_mip_category_item.bk_hash
 where h_mms_mip_category_item_id is null
   and stage_hash_mms_MIPCategoryItem.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_mip_category_item
if object_id('tempdb..#l_mms_mip_category_item_inserts') is not null drop table #l_mms_mip_category_item_inserts
create table #l_mms_mip_category_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MIPCategoryItem.bk_hash,
       stage_hash_mms_MIPCategoryItem.MIPCategoryItemID mip_category_item_id,
       stage_hash_mms_MIPCategoryItem.ValMIPCategoryID val_mip_category_id,
       stage_hash_mms_MIPCategoryItem.ValMIPSubCategoryID val_mip_sub_category_id,
       stage_hash_mms_MIPCategoryItem.ValMIPItemID val_mip_item_id,
       stage_hash_mms_MIPCategoryItem.ValMIPInterestCategoryID val_mip_interest_category_id,
       stage_hash_mms_MIPCategoryItem.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MIPCategoryItem.MIPCategoryItemID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MIPCategoryItem.ValMIPCategoryID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MIPCategoryItem.ValMIPSubCategoryID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MIPCategoryItem.ValMIPItemID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MIPCategoryItem.ValMIPInterestCategoryID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MIPCategoryItem
 where stage_hash_mms_MIPCategoryItem.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_mip_category_item records
set @insert_date_time = getdate()
insert into l_mms_mip_category_item (
       bk_hash,
       mip_category_item_id,
       val_mip_category_id,
       val_mip_sub_category_id,
       val_mip_item_id,
       val_mip_interest_category_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_mip_category_item_inserts.bk_hash,
       #l_mms_mip_category_item_inserts.mip_category_item_id,
       #l_mms_mip_category_item_inserts.val_mip_category_id,
       #l_mms_mip_category_item_inserts.val_mip_sub_category_id,
       #l_mms_mip_category_item_inserts.val_mip_item_id,
       #l_mms_mip_category_item_inserts.val_mip_interest_category_id,
       case when l_mms_mip_category_item.l_mms_mip_category_item_id is null then isnull(#l_mms_mip_category_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_mip_category_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_mip_category_item_inserts
  left join p_mms_mip_category_item
    on #l_mms_mip_category_item_inserts.bk_hash = p_mms_mip_category_item.bk_hash
   and p_mms_mip_category_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_mip_category_item
    on p_mms_mip_category_item.bk_hash = l_mms_mip_category_item.bk_hash
   and p_mms_mip_category_item.l_mms_mip_category_item_id = l_mms_mip_category_item.l_mms_mip_category_item_id
 where l_mms_mip_category_item.l_mms_mip_category_item_id is null
    or (l_mms_mip_category_item.l_mms_mip_category_item_id is not null
        and l_mms_mip_category_item.dv_hash <> #l_mms_mip_category_item_inserts.source_hash)

--calculate hash and lookup to current s_mms_mip_category_item
if object_id('tempdb..#s_mms_mip_category_item_inserts') is not null drop table #s_mms_mip_category_item_inserts
create table #s_mms_mip_category_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MIPCategoryItem.bk_hash,
       stage_hash_mms_MIPCategoryItem.MIPCategoryItemID mip_category_item_id,
       stage_hash_mms_MIPCategoryItem.ActiveFlag active_flag,
       stage_hash_mms_MIPCategoryItem.AllowCommentFlag allow_comment_flag,
       stage_hash_mms_MIPCategoryItem.SortOrder sort_order,
       stage_hash_mms_MIPCategoryItem.InsertedDateTime inserted_date_time,
       stage_hash_mms_MIPCategoryItem.UpdatedDateTime updated_date_time,
       stage_hash_mms_MIPCategoryItem.ProspectEnabledFlag prospect_enabled_flag,
       stage_hash_mms_MIPCategoryItem.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MIPCategoryItem.MIPCategoryItemID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MIPCategoryItem.ActiveFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MIPCategoryItem.AllowCommentFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MIPCategoryItem.SortOrder as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MIPCategoryItem.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MIPCategoryItem.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_MIPCategoryItem.ProspectEnabledFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MIPCategoryItem
 where stage_hash_mms_MIPCategoryItem.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_mip_category_item records
set @insert_date_time = getdate()
insert into s_mms_mip_category_item (
       bk_hash,
       mip_category_item_id,
       active_flag,
       allow_comment_flag,
       sort_order,
       inserted_date_time,
       updated_date_time,
       prospect_enabled_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_mip_category_item_inserts.bk_hash,
       #s_mms_mip_category_item_inserts.mip_category_item_id,
       #s_mms_mip_category_item_inserts.active_flag,
       #s_mms_mip_category_item_inserts.allow_comment_flag,
       #s_mms_mip_category_item_inserts.sort_order,
       #s_mms_mip_category_item_inserts.inserted_date_time,
       #s_mms_mip_category_item_inserts.updated_date_time,
       #s_mms_mip_category_item_inserts.prospect_enabled_flag,
       case when s_mms_mip_category_item.s_mms_mip_category_item_id is null then isnull(#s_mms_mip_category_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_mip_category_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_mip_category_item_inserts
  left join p_mms_mip_category_item
    on #s_mms_mip_category_item_inserts.bk_hash = p_mms_mip_category_item.bk_hash
   and p_mms_mip_category_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_mip_category_item
    on p_mms_mip_category_item.bk_hash = s_mms_mip_category_item.bk_hash
   and p_mms_mip_category_item.s_mms_mip_category_item_id = s_mms_mip_category_item.s_mms_mip_category_item_id
 where s_mms_mip_category_item.s_mms_mip_category_item_id is null
    or (s_mms_mip_category_item.s_mms_mip_category_item_id is not null
        and s_mms_mip_category_item.dv_hash <> #s_mms_mip_category_item_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_mip_category_item @current_dv_batch_id

end
