CREATE PROC [dbo].[proc_etl_hybris_fulfillment_partner] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_fulfillmentpartner

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_fulfillmentpartner (
       bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       p_displayname,
       p_code,
       p_ftpto,
       p_exportfileformat,
       p_ftpfrom,
       p_importfileformat,
       p_workdaysupplierid,
       p_inventoryto,
       p_inventoryfileformat,
       p_receivercodeid,
       p_receiverid,
       p_senderqualifier,
       p_receiverqualifier,
       p_senderid,
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
       p_displayname,
       p_code,
       p_ftpto,
       p_exportfileformat,
       p_ftpfrom,
       p_importfileformat,
       p_workdaysupplierid,
       p_inventoryto,
       p_inventoryfileformat,
       p_receivercodeid,
       p_receiverid,
       p_senderqualifier,
       p_receiverqualifier,
       p_senderid,
       isnull(cast(stage_hybris_fulfillmentpartner.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_fulfillmentpartner
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_fulfillment_partner @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_fulfillment_partner (
       bk_hash,
       fulfillment_partner_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_fulfillmentpartner.bk_hash,
       stage_hash_hybris_fulfillmentpartner.[PK] fulfillment_partner_pk,
       isnull(cast(stage_hash_hybris_fulfillmentpartner.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_fulfillmentpartner
  left join h_hybris_fulfillment_partner
    on stage_hash_hybris_fulfillmentpartner.bk_hash = h_hybris_fulfillment_partner.bk_hash
 where h_hybris_fulfillment_partner_id is null
   and stage_hash_hybris_fulfillmentpartner.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_fulfillment_partner
if object_id('tempdb..#l_hybris_fulfillment_partner_inserts') is not null drop table #l_hybris_fulfillment_partner_inserts
create table #l_hybris_fulfillment_partner_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_fulfillmentpartner.bk_hash,
       stage_hash_hybris_fulfillmentpartner.TypePkString type_pk_string,
       stage_hash_hybris_fulfillmentpartner.[PK] fulfillment_partner_pk,
       stage_hash_hybris_fulfillmentpartner.OwnerPkString owner_pk_string,
       stage_hash_hybris_fulfillmentpartner.p_exportfileformat p_export_file_format,
       stage_hash_hybris_fulfillmentpartner.p_importfileformat p_import_file_format,
       stage_hash_hybris_fulfillmentpartner.p_receivercodeid p_receiver_code_id,
       stage_hash_hybris_fulfillmentpartner.p_receiverid p_receiver_id,
       stage_hash_hybris_fulfillmentpartner.p_senderid p_sender_id,
       isnull(cast(stage_hash_hybris_fulfillmentpartner.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_fulfillmentpartner.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_fulfillmentpartner.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_fulfillmentpartner.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_fulfillmentpartner.p_exportfileformat as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_fulfillmentpartner.p_importfileformat as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_receivercodeid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_receiverid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_senderid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_fulfillmentpartner
 where stage_hash_hybris_fulfillmentpartner.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_fulfillment_partner records
set @insert_date_time = getdate()
insert into l_hybris_fulfillment_partner (
       bk_hash,
       type_pk_string,
       fulfillment_partner_pk,
       owner_pk_string,
       p_export_file_format,
       p_import_file_format,
       p_receiver_code_id,
       p_receiver_id,
       p_sender_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_fulfillment_partner_inserts.bk_hash,
       #l_hybris_fulfillment_partner_inserts.type_pk_string,
       #l_hybris_fulfillment_partner_inserts.fulfillment_partner_pk,
       #l_hybris_fulfillment_partner_inserts.owner_pk_string,
       #l_hybris_fulfillment_partner_inserts.p_export_file_format,
       #l_hybris_fulfillment_partner_inserts.p_import_file_format,
       #l_hybris_fulfillment_partner_inserts.p_receiver_code_id,
       #l_hybris_fulfillment_partner_inserts.p_receiver_id,
       #l_hybris_fulfillment_partner_inserts.p_sender_id,
       case when l_hybris_fulfillment_partner.l_hybris_fulfillment_partner_id is null then isnull(#l_hybris_fulfillment_partner_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_fulfillment_partner_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_fulfillment_partner_inserts
  left join p_hybris_fulfillment_partner
    on #l_hybris_fulfillment_partner_inserts.bk_hash = p_hybris_fulfillment_partner.bk_hash
   and p_hybris_fulfillment_partner.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_fulfillment_partner
    on p_hybris_fulfillment_partner.bk_hash = l_hybris_fulfillment_partner.bk_hash
   and p_hybris_fulfillment_partner.l_hybris_fulfillment_partner_id = l_hybris_fulfillment_partner.l_hybris_fulfillment_partner_id
 where l_hybris_fulfillment_partner.l_hybris_fulfillment_partner_id is null
    or (l_hybris_fulfillment_partner.l_hybris_fulfillment_partner_id is not null
        and l_hybris_fulfillment_partner.dv_hash <> #l_hybris_fulfillment_partner_inserts.source_hash)

--calculate hash and lookup to current s_hybris_fulfillment_partner
if object_id('tempdb..#s_hybris_fulfillment_partner_inserts') is not null drop table #s_hybris_fulfillment_partner_inserts
create table #s_hybris_fulfillment_partner_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_fulfillmentpartner.bk_hash,
       stage_hash_hybris_fulfillmentpartner.hjmpTS hjmpts,
       stage_hash_hybris_fulfillmentpartner.[PK] fulfillment_partner_pk,
       stage_hash_hybris_fulfillmentpartner.createdTS created_ts,
       stage_hash_hybris_fulfillmentpartner.modifiedTS modified_ts,
       stage_hash_hybris_fulfillmentpartner.aCLTS acl_ts,
       stage_hash_hybris_fulfillmentpartner.propTS prop_ts,
       stage_hash_hybris_fulfillmentpartner.p_displayname p_display_name,
       stage_hash_hybris_fulfillmentpartner.p_code p_code,
       stage_hash_hybris_fulfillmentpartner.p_ftpto p_ftp_to,
       stage_hash_hybris_fulfillmentpartner.p_ftpfrom p_ftp_from,
       stage_hash_hybris_fulfillmentpartner.p_workdaysupplierid p_work_day_supplier_id,
       stage_hash_hybris_fulfillmentpartner.p_inventoryto p_inventory_to,
       stage_hash_hybris_fulfillmentpartner.p_inventoryfileformat p_inventory_file_format,
       stage_hash_hybris_fulfillmentpartner.p_senderqualifier p_sender_qualifier,
       stage_hash_hybris_fulfillmentpartner.p_receiverqualifier p_receiver_qualifier,
       isnull(cast(stage_hash_hybris_fulfillmentpartner.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_fulfillmentpartner.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_fulfillmentpartner.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_fulfillmentpartner.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_fulfillmentpartner.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_fulfillmentpartner.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_fulfillmentpartner.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_displayname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_ftpto,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_ftpfrom,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_workdaysupplierid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_inventoryto,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_inventoryfileformat,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_senderqualifier,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_fulfillmentpartner.p_receiverqualifier,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_fulfillmentpartner
 where stage_hash_hybris_fulfillmentpartner.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_fulfillment_partner records
set @insert_date_time = getdate()
insert into s_hybris_fulfillment_partner (
       bk_hash,
       hjmpts,
       fulfillment_partner_pk,
       created_ts,
       modified_ts,
       acl_ts,
       prop_ts,
       p_display_name,
       p_code,
       p_ftp_to,
       p_ftp_from,
       p_work_day_supplier_id,
       p_inventory_to,
       p_inventory_file_format,
       p_sender_qualifier,
       p_receiver_qualifier,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_fulfillment_partner_inserts.bk_hash,
       #s_hybris_fulfillment_partner_inserts.hjmpts,
       #s_hybris_fulfillment_partner_inserts.fulfillment_partner_pk,
       #s_hybris_fulfillment_partner_inserts.created_ts,
       #s_hybris_fulfillment_partner_inserts.modified_ts,
       #s_hybris_fulfillment_partner_inserts.acl_ts,
       #s_hybris_fulfillment_partner_inserts.prop_ts,
       #s_hybris_fulfillment_partner_inserts.p_display_name,
       #s_hybris_fulfillment_partner_inserts.p_code,
       #s_hybris_fulfillment_partner_inserts.p_ftp_to,
       #s_hybris_fulfillment_partner_inserts.p_ftp_from,
       #s_hybris_fulfillment_partner_inserts.p_work_day_supplier_id,
       #s_hybris_fulfillment_partner_inserts.p_inventory_to,
       #s_hybris_fulfillment_partner_inserts.p_inventory_file_format,
       #s_hybris_fulfillment_partner_inserts.p_sender_qualifier,
       #s_hybris_fulfillment_partner_inserts.p_receiver_qualifier,
       case when s_hybris_fulfillment_partner.s_hybris_fulfillment_partner_id is null then isnull(#s_hybris_fulfillment_partner_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_fulfillment_partner_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_fulfillment_partner_inserts
  left join p_hybris_fulfillment_partner
    on #s_hybris_fulfillment_partner_inserts.bk_hash = p_hybris_fulfillment_partner.bk_hash
   and p_hybris_fulfillment_partner.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_fulfillment_partner
    on p_hybris_fulfillment_partner.bk_hash = s_hybris_fulfillment_partner.bk_hash
   and p_hybris_fulfillment_partner.s_hybris_fulfillment_partner_id = s_hybris_fulfillment_partner.s_hybris_fulfillment_partner_id
 where s_hybris_fulfillment_partner.s_hybris_fulfillment_partner_id is null
    or (s_hybris_fulfillment_partner.s_hybris_fulfillment_partner_id is not null
        and s_hybris_fulfillment_partner.dv_hash <> #s_hybris_fulfillment_partner_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_fulfillment_partner @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_hybris_fulfillment_partner @current_dv_batch_id

end
