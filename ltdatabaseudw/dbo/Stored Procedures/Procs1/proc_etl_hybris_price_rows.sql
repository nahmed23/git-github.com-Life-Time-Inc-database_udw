CREATE PROC [dbo].[proc_etl_hybris_price_rows] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_pricerows

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_pricerows (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_product,
       p_pg,
       p_productmatchqualifier,
       p_starttime,
       p_endtime,
       p_user,
       p_ug,
       p_usermatchqualifier,
       p_productid,
       p_catalogversion,
       p_matchvalue,
       p_currency,
       p_minqtd,
       p_net,
       p_price,
       p_unit,
       p_unitfactor,
       p_giveawayprice,
       p_channel,
       p_sequenceid,
       aCLTS,
       propTS,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_product,
       p_pg,
       p_productmatchqualifier,
       p_starttime,
       p_endtime,
       p_user,
       p_ug,
       p_usermatchqualifier,
       p_productid,
       p_catalogversion,
       p_matchvalue,
       p_currency,
       p_minqtd,
       p_net,
       p_price,
       p_unit,
       p_unitfactor,
       p_giveawayprice,
       p_channel,
       p_sequenceid,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_pricerows.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_pricerows
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_price_rows @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_price_rows (
       bk_hash,
       price_rows_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_pricerows.bk_hash,
       stage_hash_hybris_pricerows.[PK] price_rows_pk,
       isnull(cast(stage_hash_hybris_pricerows.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_pricerows
  left join h_hybris_price_rows
    on stage_hash_hybris_pricerows.bk_hash = h_hybris_price_rows.bk_hash
 where h_hybris_price_rows_id is null
   and stage_hash_hybris_pricerows.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_price_rows
if object_id('tempdb..#l_hybris_price_rows_inserts') is not null drop table #l_hybris_price_rows_inserts
create table #l_hybris_price_rows_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_pricerows.bk_hash,
       stage_hash_hybris_pricerows.TypePkString type_pk_string,
       stage_hash_hybris_pricerows.OwnerPkString owner_pk_string,
       stage_hash_hybris_pricerows.[PK] price_rows_pk,
       stage_hash_hybris_pricerows.p_product p_product,
       stage_hash_hybris_pricerows.p_pg p_pg,
       stage_hash_hybris_pricerows.p_productmatchqualifier p_product_match_qualifier,
       stage_hash_hybris_pricerows.p_user p_user,
       stage_hash_hybris_pricerows.p_ug p_ug,
       stage_hash_hybris_pricerows.p_catalogversion p_catalog_version,
       stage_hash_hybris_pricerows.p_currency p_currency,
       stage_hash_hybris_pricerows.p_unit p_unit,
       stage_hash_hybris_pricerows.p_channel p_channel,
       stage_hash_hybris_pricerows.p_sequenceid p_sequence_id,
       stage_hash_hybris_pricerows.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_product as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_pg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_productmatchqualifier as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_user as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_ug as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_catalogversion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_currency as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_unit as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_channel as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_sequenceid as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_pricerows
 where stage_hash_hybris_pricerows.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_price_rows records
set @insert_date_time = getdate()
insert into l_hybris_price_rows (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       price_rows_pk,
       p_product,
       p_pg,
       p_product_match_qualifier,
       p_user,
       p_ug,
       p_catalog_version,
       p_currency,
       p_unit,
       p_channel,
       p_sequence_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_price_rows_inserts.bk_hash,
       #l_hybris_price_rows_inserts.type_pk_string,
       #l_hybris_price_rows_inserts.owner_pk_string,
       #l_hybris_price_rows_inserts.price_rows_pk,
       #l_hybris_price_rows_inserts.p_product,
       #l_hybris_price_rows_inserts.p_pg,
       #l_hybris_price_rows_inserts.p_product_match_qualifier,
       #l_hybris_price_rows_inserts.p_user,
       #l_hybris_price_rows_inserts.p_ug,
       #l_hybris_price_rows_inserts.p_catalog_version,
       #l_hybris_price_rows_inserts.p_currency,
       #l_hybris_price_rows_inserts.p_unit,
       #l_hybris_price_rows_inserts.p_channel,
       #l_hybris_price_rows_inserts.p_sequence_id,
       case when l_hybris_price_rows.l_hybris_price_rows_id is null then isnull(#l_hybris_price_rows_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_price_rows_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_price_rows_inserts
  left join p_hybris_price_rows
    on #l_hybris_price_rows_inserts.bk_hash = p_hybris_price_rows.bk_hash
   and p_hybris_price_rows.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_price_rows
    on p_hybris_price_rows.bk_hash = l_hybris_price_rows.bk_hash
   and p_hybris_price_rows.l_hybris_price_rows_id = l_hybris_price_rows.l_hybris_price_rows_id
 where l_hybris_price_rows.l_hybris_price_rows_id is null
    or (l_hybris_price_rows.l_hybris_price_rows_id is not null
        and l_hybris_price_rows.dv_hash <> #l_hybris_price_rows_inserts.source_hash)

--calculate hash and lookup to current s_hybris_price_rows
if object_id('tempdb..#s_hybris_price_rows_inserts') is not null drop table #s_hybris_price_rows_inserts
create table #s_hybris_price_rows_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_pricerows.bk_hash,
       stage_hash_hybris_pricerows.hjmpTS hjmpts,
       stage_hash_hybris_pricerows.createdTS created_ts,
       stage_hash_hybris_pricerows.modifiedTS modified_ts,
       stage_hash_hybris_pricerows.[PK] price_rows_pk,
       stage_hash_hybris_pricerows.p_starttime p_start_time,
       stage_hash_hybris_pricerows.p_endtime p_end_time,
       stage_hash_hybris_pricerows.p_usermatchqualifier p_user_match_qualifier,
       stage_hash_hybris_pricerows.p_productid p_product_id,
       stage_hash_hybris_pricerows.p_matchvalue p_match_value,
       stage_hash_hybris_pricerows.p_minqtd p_min_qtd,
       stage_hash_hybris_pricerows.p_net p_net,
       stage_hash_hybris_pricerows.p_price p_price,
       stage_hash_hybris_pricerows.p_unitfactor p_unit_factor,
       stage_hash_hybris_pricerows.p_giveawayprice p_give_away_price,
       stage_hash_hybris_pricerows.aCLTS acl_ts,
       stage_hash_hybris_pricerows.propTS prop_ts,
       stage_hash_hybris_pricerows.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_pricerows.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_pricerows.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_pricerows.p_starttime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_pricerows.p_endtime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_usermatchqualifier as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_pricerows.p_productid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_matchvalue as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_minqtd as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_net as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_price as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_unitfactor as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.p_giveawayprice as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pricerows.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_pricerows
 where stage_hash_hybris_pricerows.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_price_rows records
set @insert_date_time = getdate()
insert into s_hybris_price_rows (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       price_rows_pk,
       p_start_time,
       p_end_time,
       p_user_match_qualifier,
       p_product_id,
       p_match_value,
       p_min_qtd,
       p_net,
       p_price,
       p_unit_factor,
       p_give_away_price,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_price_rows_inserts.bk_hash,
       #s_hybris_price_rows_inserts.hjmpts,
       #s_hybris_price_rows_inserts.created_ts,
       #s_hybris_price_rows_inserts.modified_ts,
       #s_hybris_price_rows_inserts.price_rows_pk,
       #s_hybris_price_rows_inserts.p_start_time,
       #s_hybris_price_rows_inserts.p_end_time,
       #s_hybris_price_rows_inserts.p_user_match_qualifier,
       #s_hybris_price_rows_inserts.p_product_id,
       #s_hybris_price_rows_inserts.p_match_value,
       #s_hybris_price_rows_inserts.p_min_qtd,
       #s_hybris_price_rows_inserts.p_net,
       #s_hybris_price_rows_inserts.p_price,
       #s_hybris_price_rows_inserts.p_unit_factor,
       #s_hybris_price_rows_inserts.p_give_away_price,
       #s_hybris_price_rows_inserts.acl_ts,
       #s_hybris_price_rows_inserts.prop_ts,
       case when s_hybris_price_rows.s_hybris_price_rows_id is null then isnull(#s_hybris_price_rows_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_price_rows_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_price_rows_inserts
  left join p_hybris_price_rows
    on #s_hybris_price_rows_inserts.bk_hash = p_hybris_price_rows.bk_hash
   and p_hybris_price_rows.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_price_rows
    on p_hybris_price_rows.bk_hash = s_hybris_price_rows.bk_hash
   and p_hybris_price_rows.s_hybris_price_rows_id = s_hybris_price_rows.s_hybris_price_rows_id
 where s_hybris_price_rows.s_hybris_price_rows_id is null
    or (s_hybris_price_rows.s_hybris_price_rows_id is not null
        and s_hybris_price_rows.dv_hash <> #s_hybris_price_rows_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_price_rows @current_dv_batch_id

end
