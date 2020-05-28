CREATE PROC [dbo].[proc_etl_hybris_products_lp] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_productslp

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_productslp (
       bk_hash,
       ITEMPK,
       ITEMTYPEPK,
       LANGPK,
       p_specifications,
       p_size,
       p_description,
       p_offermedia,
       p_localizedasset,
       p_summary,
       p_caption,
       p_localizedpdfasset,
       p_nutritionweight,
       p_flavor,
       p_localizedassetpicture,
       p_manufacturertypedescription,
       p_offerlink,
       p_style,
       p_segment,
       p_name,
       p_restriction,
       p_instruction,
       p_other,
       p_option,
       p_instructions,
       p_package,
       p_type,
       p_version,
       p_resistance,
       p_model,
       p_weightvariant,
       p_scent,
       p_color,
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
       p_specifications,
       p_size,
       p_description,
       p_offermedia,
       p_localizedasset,
       p_summary,
       p_caption,
       p_localizedpdfasset,
       p_nutritionweight,
       p_flavor,
       p_localizedassetpicture,
       p_manufacturertypedescription,
       p_offerlink,
       p_style,
       p_segment,
       p_name,
       p_restriction,
       p_instruction,
       p_other,
       p_option,
       p_instructions,
       p_package,
       p_type,
       p_version,
       p_resistance,
       p_model,
       p_weightvariant,
       p_scent,
       p_color,
       createdTS,
       modifiedTS,
       isnull(cast(stage_hybris_productslp.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_productslp
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_products_lp @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_products_lp (
       bk_hash,
       item_pk,
       lang_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_productslp.bk_hash,
       stage_hash_hybris_productslp.ITEMPK item_pk,
       stage_hash_hybris_productslp.LANGPK lang_pk,
       isnull(cast(stage_hash_hybris_productslp.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_productslp
  left join h_hybris_products_lp
    on stage_hash_hybris_productslp.bk_hash = h_hybris_products_lp.bk_hash
 where h_hybris_products_lp_id is null
   and stage_hash_hybris_productslp.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_products_lp
if object_id('tempdb..#l_hybris_products_lp_inserts') is not null drop table #l_hybris_products_lp_inserts
create table #l_hybris_products_lp_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_productslp.bk_hash,
       stage_hash_hybris_productslp.ITEMPK item_pk,
       stage_hash_hybris_productslp.LANGPK lang_pk,
       isnull(cast(stage_hash_hybris_productslp.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.ITEMPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.LANGPK as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_productslp
 where stage_hash_hybris_productslp.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_products_lp records
set @insert_date_time = getdate()
insert into l_hybris_products_lp (
       bk_hash,
       item_pk,
       lang_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_products_lp_inserts.bk_hash,
       #l_hybris_products_lp_inserts.item_pk,
       #l_hybris_products_lp_inserts.lang_pk,
       case when l_hybris_products_lp.l_hybris_products_lp_id is null then isnull(#l_hybris_products_lp_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_products_lp_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_products_lp_inserts
  left join p_hybris_products_lp
    on #l_hybris_products_lp_inserts.bk_hash = p_hybris_products_lp.bk_hash
   and p_hybris_products_lp.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_products_lp
    on p_hybris_products_lp.bk_hash = l_hybris_products_lp.bk_hash
   and p_hybris_products_lp.l_hybris_products_lp_id = l_hybris_products_lp.l_hybris_products_lp_id
 where l_hybris_products_lp.l_hybris_products_lp_id is null
    or (l_hybris_products_lp.l_hybris_products_lp_id is not null
        and l_hybris_products_lp.dv_hash <> #l_hybris_products_lp_inserts.source_hash)

--calculate hash and lookup to current s_hybris_products_lp
if object_id('tempdb..#s_hybris_products_lp_inserts') is not null drop table #s_hybris_products_lp_inserts
create table #s_hybris_products_lp_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_productslp.bk_hash,
       stage_hash_hybris_productslp.ITEMPK item_pk,
       stage_hash_hybris_productslp.ITEMTYPEPK item_type_pk,
       stage_hash_hybris_productslp.LANGPK lang_pk,
       stage_hash_hybris_productslp.p_specifications p_specifications,
       stage_hash_hybris_productslp.p_size p_size,
       stage_hash_hybris_productslp.p_description p_description,
       stage_hash_hybris_productslp.p_offermedia p_offer_media,
       stage_hash_hybris_productslp.p_localizedasset p_localized_asset,
       stage_hash_hybris_productslp.p_summary p_summary,
       stage_hash_hybris_productslp.p_caption p_caption,
       stage_hash_hybris_productslp.p_localizedpdfasset p_localized_pdf_asset,
       stage_hash_hybris_productslp.p_nutritionweight p_nutrition_weight,
       stage_hash_hybris_productslp.p_flavor p_flavor,
       stage_hash_hybris_productslp.p_localizedassetpicture p_localized_asset_picture,
       stage_hash_hybris_productslp.p_manufacturertypedescription p_manufacturer_type_description,
       stage_hash_hybris_productslp.p_offerlink p_offer_link,
       stage_hash_hybris_productslp.p_style p_style,
       stage_hash_hybris_productslp.p_segment p_segment,
       stage_hash_hybris_productslp.p_name p_name,
       stage_hash_hybris_productslp.p_restriction p_restriction,
       stage_hash_hybris_productslp.p_instruction p_instruction,
       stage_hash_hybris_productslp.p_other p_other,
       stage_hash_hybris_productslp.p_option p_option,
       stage_hash_hybris_productslp.p_instructions p_instructions,
       stage_hash_hybris_productslp.p_package p_package,
       stage_hash_hybris_productslp.p_type p_type,
       stage_hash_hybris_productslp.p_version p_version,
       stage_hash_hybris_productslp.p_resistance p_resistance,
       stage_hash_hybris_productslp.p_model p_model,
       stage_hash_hybris_productslp.p_weightvariant p_weight_variant,
       stage_hash_hybris_productslp.p_scent p_scent,
       stage_hash_hybris_productslp.p_color p_color,
       stage_hash_hybris_productslp.createdTS created_ts,
       stage_hash_hybris_productslp.modifiedTS modified_ts,
       isnull(cast(stage_hash_hybris_productslp.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.ITEMPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.ITEMTYPEPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.LANGPK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.p_specifications as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_size,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.p_offermedia as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.p_localizedasset as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_summary,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_caption,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.p_localizedpdfasset as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_nutritionweight,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_flavor,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.p_localizedassetpicture as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_manufacturertypedescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_offerlink,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_style,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_segment,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_restriction,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_instruction,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_productslp.p_other as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_option,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_instructions,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_package,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_type,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_version,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_resistance,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_model,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_weightvariant,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_scent,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_productslp.p_color,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_productslp.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_productslp.modifiedTS,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_productslp
 where stage_hash_hybris_productslp.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_products_lp records
set @insert_date_time = getdate()
insert into s_hybris_products_lp (
       bk_hash,
       item_pk,
       item_type_pk,
       lang_pk,
       p_specifications,
       p_size,
       p_description,
       p_offer_media,
       p_localized_asset,
       p_summary,
       p_caption,
       p_localized_pdf_asset,
       p_nutrition_weight,
       p_flavor,
       p_localized_asset_picture,
       p_manufacturer_type_description,
       p_offer_link,
       p_style,
       p_segment,
       p_name,
       p_restriction,
       p_instruction,
       p_other,
       p_option,
       p_instructions,
       p_package,
       p_type,
       p_version,
       p_resistance,
       p_model,
       p_weight_variant,
       p_scent,
       p_color,
       created_ts,
       modified_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_products_lp_inserts.bk_hash,
       #s_hybris_products_lp_inserts.item_pk,
       #s_hybris_products_lp_inserts.item_type_pk,
       #s_hybris_products_lp_inserts.lang_pk,
       #s_hybris_products_lp_inserts.p_specifications,
       #s_hybris_products_lp_inserts.p_size,
       #s_hybris_products_lp_inserts.p_description,
       #s_hybris_products_lp_inserts.p_offer_media,
       #s_hybris_products_lp_inserts.p_localized_asset,
       #s_hybris_products_lp_inserts.p_summary,
       #s_hybris_products_lp_inserts.p_caption,
       #s_hybris_products_lp_inserts.p_localized_pdf_asset,
       #s_hybris_products_lp_inserts.p_nutrition_weight,
       #s_hybris_products_lp_inserts.p_flavor,
       #s_hybris_products_lp_inserts.p_localized_asset_picture,
       #s_hybris_products_lp_inserts.p_manufacturer_type_description,
       #s_hybris_products_lp_inserts.p_offer_link,
       #s_hybris_products_lp_inserts.p_style,
       #s_hybris_products_lp_inserts.p_segment,
       #s_hybris_products_lp_inserts.p_name,
       #s_hybris_products_lp_inserts.p_restriction,
       #s_hybris_products_lp_inserts.p_instruction,
       #s_hybris_products_lp_inserts.p_other,
       #s_hybris_products_lp_inserts.p_option,
       #s_hybris_products_lp_inserts.p_instructions,
       #s_hybris_products_lp_inserts.p_package,
       #s_hybris_products_lp_inserts.p_type,
       #s_hybris_products_lp_inserts.p_version,
       #s_hybris_products_lp_inserts.p_resistance,
       #s_hybris_products_lp_inserts.p_model,
       #s_hybris_products_lp_inserts.p_weight_variant,
       #s_hybris_products_lp_inserts.p_scent,
       #s_hybris_products_lp_inserts.p_color,
       #s_hybris_products_lp_inserts.created_ts,
       #s_hybris_products_lp_inserts.modified_ts,
       case when s_hybris_products_lp.s_hybris_products_lp_id is null then isnull(#s_hybris_products_lp_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_products_lp_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_products_lp_inserts
  left join p_hybris_products_lp
    on #s_hybris_products_lp_inserts.bk_hash = p_hybris_products_lp.bk_hash
   and p_hybris_products_lp.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_products_lp
    on p_hybris_products_lp.bk_hash = s_hybris_products_lp.bk_hash
   and p_hybris_products_lp.s_hybris_products_lp_id = s_hybris_products_lp.s_hybris_products_lp_id
 where s_hybris_products_lp.s_hybris_products_lp_id is null
    or (s_hybris_products_lp.s_hybris_products_lp_id is not null
        and s_hybris_products_lp.dv_hash <> #s_hybris_products_lp_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_products_lp @current_dv_batch_id

end
