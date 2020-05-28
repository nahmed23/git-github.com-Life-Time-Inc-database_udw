CREATE PROC [dbo].[proc_etl_hybris_return_request] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_returnrequest

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_returnrequest (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_code,
       p_rma,
       p_replacementorder,
       p_currency,
       p_status,
       p_returnlabel,
       p_trackingid,
       p_returnwarehouse,
       p_orderpos,
       p_order,
       p_refunddeliverycost,
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
       p_code,
       p_rma,
       p_replacementorder,
       p_currency,
       p_status,
       p_returnlabel,
       p_trackingid,
       p_returnwarehouse,
       p_orderpos,
       p_order,
       p_refunddeliverycost,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_returnrequest.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_returnrequest
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_return_request @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_return_request (
       bk_hash,
       return_request_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_returnrequest.bk_hash,
       stage_hash_hybris_returnrequest.[PK] return_request_pk,
       isnull(cast(stage_hash_hybris_returnrequest.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_returnrequest
  left join h_hybris_return_request
    on stage_hash_hybris_returnrequest.bk_hash = h_hybris_return_request.bk_hash
 where h_hybris_return_request_id is null
   and stage_hash_hybris_returnrequest.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_return_request
if object_id('tempdb..#l_hybris_return_request_inserts') is not null drop table #l_hybris_return_request_inserts
create table #l_hybris_return_request_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_returnrequest.bk_hash,
       stage_hash_hybris_returnrequest.TypePkString type_pk_string,
       stage_hash_hybris_returnrequest.OwnerPkString owner_pk_string,
       stage_hash_hybris_returnrequest.[PK] return_request_pk,
       stage_hash_hybris_returnrequest.p_trackingid p_tracking_id,
       stage_hash_hybris_returnrequest.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_returnrequest.p_trackingid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_returnrequest
 where stage_hash_hybris_returnrequest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_return_request records
set @insert_date_time = getdate()
insert into l_hybris_return_request (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       return_request_pk,
       p_tracking_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_return_request_inserts.bk_hash,
       #l_hybris_return_request_inserts.type_pk_string,
       #l_hybris_return_request_inserts.owner_pk_string,
       #l_hybris_return_request_inserts.return_request_pk,
       #l_hybris_return_request_inserts.p_tracking_id,
       case when l_hybris_return_request.l_hybris_return_request_id is null then isnull(#l_hybris_return_request_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_return_request_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_return_request_inserts
  left join p_hybris_return_request
    on #l_hybris_return_request_inserts.bk_hash = p_hybris_return_request.bk_hash
   and p_hybris_return_request.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_return_request
    on p_hybris_return_request.bk_hash = l_hybris_return_request.bk_hash
   and p_hybris_return_request.l_hybris_return_request_id = l_hybris_return_request.l_hybris_return_request_id
 where l_hybris_return_request.l_hybris_return_request_id is null
    or (l_hybris_return_request.l_hybris_return_request_id is not null
        and l_hybris_return_request.dv_hash <> #l_hybris_return_request_inserts.source_hash)

--calculate hash and lookup to current s_hybris_return_request
if object_id('tempdb..#s_hybris_return_request_inserts') is not null drop table #s_hybris_return_request_inserts
create table #s_hybris_return_request_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_returnrequest.bk_hash,
       stage_hash_hybris_returnrequest.hjmpTS hjmpts,
       stage_hash_hybris_returnrequest.createdTS created_ts,
       stage_hash_hybris_returnrequest.modifiedTS modified_ts,
       stage_hash_hybris_returnrequest.[PK] return_request_pk,
       stage_hash_hybris_returnrequest.p_code p_code,
       stage_hash_hybris_returnrequest.p_rma p_rma,
       stage_hash_hybris_returnrequest.p_replacementorder p_replacement_order,
       stage_hash_hybris_returnrequest.p_currency p_currency,
       stage_hash_hybris_returnrequest.p_status p_status,
       stage_hash_hybris_returnrequest.p_returnlabel p_return_label,
       stage_hash_hybris_returnrequest.p_returnwarehouse p_return_warehouse,
       stage_hash_hybris_returnrequest.p_orderpos p_order_pos,
       stage_hash_hybris_returnrequest.p_order p_order,
       stage_hash_hybris_returnrequest.p_refunddeliverycost p_refund_delivery_cost,
       stage_hash_hybris_returnrequest.aCLTS acl_ts,
       stage_hash_hybris_returnrequest.propTS prop_ts,
       stage_hash_hybris_returnrequest.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_returnrequest.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_returnrequest.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_returnrequest.p_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_returnrequest.p_rma,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.p_replacementorder as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.p_currency as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.p_status as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.p_returnlabel as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.p_returnwarehouse as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.p_orderpos as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.p_order as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.p_refunddeliverycost as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnrequest.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_returnrequest
 where stage_hash_hybris_returnrequest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_return_request records
set @insert_date_time = getdate()
insert into s_hybris_return_request (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       return_request_pk,
       p_code,
       p_rma,
       p_replacement_order,
       p_currency,
       p_status,
       p_return_label,
       p_return_warehouse,
       p_order_pos,
       p_order,
       p_refund_delivery_cost,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_return_request_inserts.bk_hash,
       #s_hybris_return_request_inserts.hjmpts,
       #s_hybris_return_request_inserts.created_ts,
       #s_hybris_return_request_inserts.modified_ts,
       #s_hybris_return_request_inserts.return_request_pk,
       #s_hybris_return_request_inserts.p_code,
       #s_hybris_return_request_inserts.p_rma,
       #s_hybris_return_request_inserts.p_replacement_order,
       #s_hybris_return_request_inserts.p_currency,
       #s_hybris_return_request_inserts.p_status,
       #s_hybris_return_request_inserts.p_return_label,
       #s_hybris_return_request_inserts.p_return_warehouse,
       #s_hybris_return_request_inserts.p_order_pos,
       #s_hybris_return_request_inserts.p_order,
       #s_hybris_return_request_inserts.p_refund_delivery_cost,
       #s_hybris_return_request_inserts.acl_ts,
       #s_hybris_return_request_inserts.prop_ts,
       case when s_hybris_return_request.s_hybris_return_request_id is null then isnull(#s_hybris_return_request_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_return_request_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_return_request_inserts
  left join p_hybris_return_request
    on #s_hybris_return_request_inserts.bk_hash = p_hybris_return_request.bk_hash
   and p_hybris_return_request.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_return_request
    on p_hybris_return_request.bk_hash = s_hybris_return_request.bk_hash
   and p_hybris_return_request.s_hybris_return_request_id = s_hybris_return_request.s_hybris_return_request_id
 where s_hybris_return_request.s_hybris_return_request_id is null
    or (s_hybris_return_request.s_hybris_return_request_id is not null
        and s_hybris_return_request.dv_hash <> #s_hybris_return_request_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_return_request @current_dv_batch_id

end
