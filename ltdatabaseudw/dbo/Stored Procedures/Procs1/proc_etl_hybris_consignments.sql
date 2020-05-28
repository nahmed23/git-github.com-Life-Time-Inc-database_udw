CREATE PROC [dbo].[proc_etl_hybris_consignments] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_consignments

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_consignments (
       bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       p_trackingid,
       p_status,
       p_shippingdate,
       p_nameddeliverydate,
       p_code,
       p_carrier,
       p_warehouse,
       p_shippingaddress,
       p_order,
       p_deliverymode,
       p_deliverypointofservice,
       p_trackingmessage,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       p_trackingid,
       p_status,
       p_shippingdate,
       p_nameddeliverydate,
       p_code,
       p_carrier,
       p_warehouse,
       p_shippingaddress,
       p_order,
       p_deliverymode,
       p_deliverypointofservice,
       p_trackingmessage,
       isnull(cast(stage_hybris_consignments.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_consignments
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_consignments @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_consignments (
       bk_hash,
       consignments_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_consignments.bk_hash,
       stage_hash_hybris_consignments.[PK] consignments_pk,
       isnull(cast(stage_hash_hybris_consignments.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_consignments
  left join h_hybris_consignments
    on stage_hash_hybris_consignments.bk_hash = h_hybris_consignments.bk_hash
 where h_hybris_consignments_id is null
   and stage_hash_hybris_consignments.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_consignments
if object_id('tempdb..#l_hybris_consignments_inserts') is not null drop table #l_hybris_consignments_inserts
create table #l_hybris_consignments_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_consignments.bk_hash,
       stage_hash_hybris_consignments.TypePkString type_pk_string,
       stage_hash_hybris_consignments.[PK] consignments_pk,
       stage_hash_hybris_consignments.OwnerPkString owner_pk_string,
       stage_hash_hybris_consignments.p_status p_status,
       stage_hash_hybris_consignments.p_warehouse p_warehouse,
       stage_hash_hybris_consignments.p_shippingaddress p_shipping_address,
       stage_hash_hybris_consignments.p_order p_order,
       stage_hash_hybris_consignments.p_deliverymode p_delivery_mode,
       stage_hash_hybris_consignments.p_deliverypointofservice p_delivery_point_of_service,
       isnull(cast(stage_hash_hybris_consignments.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.p_status as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.p_warehouse as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.p_shippingaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.p_order as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.p_deliverymode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.p_deliverypointofservice as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_consignments
 where stage_hash_hybris_consignments.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_consignments records
set @insert_date_time = getdate()
insert into l_hybris_consignments (
       bk_hash,
       type_pk_string,
       consignments_pk,
       owner_pk_string,
       p_status,
       p_warehouse,
       p_shipping_address,
       p_order,
       p_delivery_mode,
       p_delivery_point_of_service,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_consignments_inserts.bk_hash,
       #l_hybris_consignments_inserts.type_pk_string,
       #l_hybris_consignments_inserts.consignments_pk,
       #l_hybris_consignments_inserts.owner_pk_string,
       #l_hybris_consignments_inserts.p_status,
       #l_hybris_consignments_inserts.p_warehouse,
       #l_hybris_consignments_inserts.p_shipping_address,
       #l_hybris_consignments_inserts.p_order,
       #l_hybris_consignments_inserts.p_delivery_mode,
       #l_hybris_consignments_inserts.p_delivery_point_of_service,
       case when l_hybris_consignments.l_hybris_consignments_id is null then isnull(#l_hybris_consignments_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_consignments_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_consignments_inserts
  left join p_hybris_consignments
    on #l_hybris_consignments_inserts.bk_hash = p_hybris_consignments.bk_hash
   and p_hybris_consignments.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_consignments
    on p_hybris_consignments.bk_hash = l_hybris_consignments.bk_hash
   and p_hybris_consignments.l_hybris_consignments_id = l_hybris_consignments.l_hybris_consignments_id
 where l_hybris_consignments.l_hybris_consignments_id is null
    or (l_hybris_consignments.l_hybris_consignments_id is not null
        and l_hybris_consignments.dv_hash <> #l_hybris_consignments_inserts.source_hash)

--calculate hash and lookup to current s_hybris_consignments
if object_id('tempdb..#s_hybris_consignments_inserts') is not null drop table #s_hybris_consignments_inserts
create table #s_hybris_consignments_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_consignments.bk_hash,
       stage_hash_hybris_consignments.hjmpTS hjmpts,
       stage_hash_hybris_consignments.[PK] consignments_pk,
       stage_hash_hybris_consignments.createdTS created_ts,
       stage_hash_hybris_consignments.modifiedTS modified_ts,
       stage_hash_hybris_consignments.aCLTS acl_ts,
       stage_hash_hybris_consignments.propTS prop_ts,
       stage_hash_hybris_consignments.p_trackingid p_tracking_id,
       stage_hash_hybris_consignments.p_shippingdate p_shipping_date,
       stage_hash_hybris_consignments.p_nameddeliverydate p_named_delivery_date,
       stage_hash_hybris_consignments.p_code p_code,
       stage_hash_hybris_consignments.p_carrier p_carrier,
       stage_hash_hybris_consignments.p_trackingmessage p_tracking_message,
       isnull(cast(stage_hash_hybris_consignments.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_consignments.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_consignments.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_consignments.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_consignments.p_trackingid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_consignments.p_shippingdate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_consignments.p_nameddeliverydate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_consignments.p_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_consignments.p_carrier,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_consignments.p_trackingmessage,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_consignments
 where stage_hash_hybris_consignments.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_consignments records
set @insert_date_time = getdate()
insert into s_hybris_consignments (
       bk_hash,
       hjmpts,
       consignments_pk,
       created_ts,
       modified_ts,
       acl_ts,
       prop_ts,
       p_tracking_id,
       p_shipping_date,
       p_named_delivery_date,
       p_code,
       p_carrier,
       p_tracking_message,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_consignments_inserts.bk_hash,
       #s_hybris_consignments_inserts.hjmpts,
       #s_hybris_consignments_inserts.consignments_pk,
       #s_hybris_consignments_inserts.created_ts,
       #s_hybris_consignments_inserts.modified_ts,
       #s_hybris_consignments_inserts.acl_ts,
       #s_hybris_consignments_inserts.prop_ts,
       #s_hybris_consignments_inserts.p_tracking_id,
       #s_hybris_consignments_inserts.p_shipping_date,
       #s_hybris_consignments_inserts.p_named_delivery_date,
       #s_hybris_consignments_inserts.p_code,
       #s_hybris_consignments_inserts.p_carrier,
       #s_hybris_consignments_inserts.p_tracking_message,
       case when s_hybris_consignments.s_hybris_consignments_id is null then isnull(#s_hybris_consignments_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_consignments_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_consignments_inserts
  left join p_hybris_consignments
    on #s_hybris_consignments_inserts.bk_hash = p_hybris_consignments.bk_hash
   and p_hybris_consignments.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_consignments
    on p_hybris_consignments.bk_hash = s_hybris_consignments.bk_hash
   and p_hybris_consignments.s_hybris_consignments_id = s_hybris_consignments.s_hybris_consignments_id
 where s_hybris_consignments.s_hybris_consignments_id is null
    or (s_hybris_consignments.s_hybris_consignments_id is not null
        and s_hybris_consignments.dv_hash <> #s_hybris_consignments_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_consignments @current_dv_batch_id

end
