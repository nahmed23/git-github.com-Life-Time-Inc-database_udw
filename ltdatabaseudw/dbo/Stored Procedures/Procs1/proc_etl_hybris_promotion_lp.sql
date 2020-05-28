CREATE PROC [dbo].[proc_etl_hybris_promotion_lp] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_promotionlp

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_promotionlp (
       bk_hash,
       ITEMPK,
       ITEMTYPEPK,
       LANGPK,
       p_name,
       p_messagefired,
       p_messagecouldhavefired,
       p_messageproductnothreshold,
       p_messagethresholdnoproduct,
       p_promotiondescription,
       createdTS,
       modifiedTS,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ITEMPK as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(LANGPK as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ITEMPK,
       ITEMTYPEPK,
       LANGPK,
       p_name,
       p_messagefired,
       p_messagecouldhavefired,
       p_messageproductnothreshold,
       p_messagethresholdnoproduct,
       p_promotiondescription,
       createdTS,
       modifiedTS,
       isnull(cast(stage_hybris_promotionlp.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_promotionlp
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_promotion_lp @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_promotion_lp (
       bk_hash,
       item_pk,
       lang_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_promotionlp.bk_hash,
       stage_hash_hybris_promotionlp.ITEMPK item_pk,
       stage_hash_hybris_promotionlp.LANGPK lang_pk,
       isnull(cast(stage_hash_hybris_promotionlp.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_promotionlp
  left join h_hybris_promotion_lp
    on stage_hash_hybris_promotionlp.bk_hash = h_hybris_promotion_lp.bk_hash
 where h_hybris_promotion_lp_id is null
   and stage_hash_hybris_promotionlp.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_promotion_lp
if object_id('tempdb..#l_hybris_promotion_lp_inserts') is not null drop table #l_hybris_promotion_lp_inserts
create table #l_hybris_promotion_lp_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_promotionlp.bk_hash,
       stage_hash_hybris_promotionlp.ITEMPK item_pk,
       stage_hash_hybris_promotionlp.ITEMTYPEPK item_type_pk,
       stage_hash_hybris_promotionlp.LANGPK lang_pk,
       stage_hash_hybris_promotionlp.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionlp.ITEMPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionlp.ITEMTYPEPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionlp.LANGPK as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_promotionlp
 where stage_hash_hybris_promotionlp.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_promotion_lp records
set @insert_date_time = getdate()
insert into l_hybris_promotion_lp (
       bk_hash,
       item_pk,
       item_type_pk,
       lang_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_promotion_lp_inserts.bk_hash,
       #l_hybris_promotion_lp_inserts.item_pk,
       #l_hybris_promotion_lp_inserts.item_type_pk,
       #l_hybris_promotion_lp_inserts.lang_pk,
       case when l_hybris_promotion_lp.l_hybris_promotion_lp_id is null then isnull(#l_hybris_promotion_lp_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_promotion_lp_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_promotion_lp_inserts
  left join p_hybris_promotion_lp
    on #l_hybris_promotion_lp_inserts.bk_hash = p_hybris_promotion_lp.bk_hash
   and p_hybris_promotion_lp.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_promotion_lp
    on p_hybris_promotion_lp.bk_hash = l_hybris_promotion_lp.bk_hash
   and p_hybris_promotion_lp.l_hybris_promotion_lp_id = l_hybris_promotion_lp.l_hybris_promotion_lp_id
 where l_hybris_promotion_lp.l_hybris_promotion_lp_id is null
    or (l_hybris_promotion_lp.l_hybris_promotion_lp_id is not null
        and l_hybris_promotion_lp.dv_hash <> #l_hybris_promotion_lp_inserts.source_hash)

--calculate hash and lookup to current s_hybris_promotion_lp
if object_id('tempdb..#s_hybris_promotion_lp_inserts') is not null drop table #s_hybris_promotion_lp_inserts
create table #s_hybris_promotion_lp_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_promotionlp.bk_hash,
       stage_hash_hybris_promotionlp.ITEMPK item_pk,
       stage_hash_hybris_promotionlp.LANGPK lang_pk,
       stage_hash_hybris_promotionlp.p_name p_name,
       stage_hash_hybris_promotionlp.p_messagefired p_message_fired,
       stage_hash_hybris_promotionlp.p_messagecouldhavefired p_message_could_have_fired,
       stage_hash_hybris_promotionlp.p_messageproductnothreshold p_message_product_no_threshold,
       stage_hash_hybris_promotionlp.p_messagethresholdnoproduct p_message_threshold_no_product,
       stage_hash_hybris_promotionlp.p_promotiondescription p_promotion_description,
       stage_hash_hybris_promotionlp.createdTS created_ts,
       stage_hash_hybris_promotionlp.modifiedTS modified_ts,
       stage_hash_hybris_promotionlp.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionlp.ITEMPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotionlp.LANGPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionlp.p_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionlp.p_messagefired,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionlp.p_messagecouldhavefired,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionlp.p_messageproductnothreshold,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionlp.p_messagethresholdnoproduct,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotionlp.p_promotiondescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_promotionlp.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_promotionlp.modifiedTS,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_promotionlp
 where stage_hash_hybris_promotionlp.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_promotion_lp records
set @insert_date_time = getdate()
insert into s_hybris_promotion_lp (
       bk_hash,
       item_pk,
       lang_pk,
       p_name,
       p_message_fired,
       p_message_could_have_fired,
       p_message_product_no_threshold,
       p_message_threshold_no_product,
       p_promotion_description,
       created_ts,
       modified_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_promotion_lp_inserts.bk_hash,
       #s_hybris_promotion_lp_inserts.item_pk,
       #s_hybris_promotion_lp_inserts.lang_pk,
       #s_hybris_promotion_lp_inserts.p_name,
       #s_hybris_promotion_lp_inserts.p_message_fired,
       #s_hybris_promotion_lp_inserts.p_message_could_have_fired,
       #s_hybris_promotion_lp_inserts.p_message_product_no_threshold,
       #s_hybris_promotion_lp_inserts.p_message_threshold_no_product,
       #s_hybris_promotion_lp_inserts.p_promotion_description,
       #s_hybris_promotion_lp_inserts.created_ts,
       #s_hybris_promotion_lp_inserts.modified_ts,
       case when s_hybris_promotion_lp.s_hybris_promotion_lp_id is null then isnull(#s_hybris_promotion_lp_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_promotion_lp_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_promotion_lp_inserts
  left join p_hybris_promotion_lp
    on #s_hybris_promotion_lp_inserts.bk_hash = p_hybris_promotion_lp.bk_hash
   and p_hybris_promotion_lp.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_promotion_lp
    on p_hybris_promotion_lp.bk_hash = s_hybris_promotion_lp.bk_hash
   and p_hybris_promotion_lp.s_hybris_promotion_lp_id = s_hybris_promotion_lp.s_hybris_promotion_lp_id
 where s_hybris_promotion_lp.s_hybris_promotion_lp_id is null
    or (s_hybris_promotion_lp.s_hybris_promotion_lp_id is not null
        and s_hybris_promotion_lp.dv_hash <> #s_hybris_promotion_lp_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_promotion_lp @current_dv_batch_id

end
