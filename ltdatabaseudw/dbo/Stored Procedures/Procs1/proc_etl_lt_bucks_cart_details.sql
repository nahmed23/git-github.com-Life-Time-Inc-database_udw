CREATE PROC [dbo].[proc_etl_lt_bucks_cart_details] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_lt_bucks_CartDetails

set @insert_date_time = getdate()
insert into dbo.stage_hash_lt_bucks_CartDetails (
       bk_hash,
       cdetail_id,
       cdetail_cart,
       cdetail_poption,
       cdetail_club,
       cdetail_expiration_date,
       cdetail_transactionkey,
       cdetail_package,
       cdetail_deliverydate,
       cdetail_assembly_cart,
       cdetail_campaign_detail,
       cdetail_qtyExpandCart,
       cdetail_reservation,
       cdetail_assigned_member,
       LastModifiedTimestamp,
       cdetail_service_expired,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cdetail_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       cdetail_id,
       cdetail_cart,
       cdetail_poption,
       cdetail_club,
       cdetail_expiration_date,
       cdetail_transactionkey,
       cdetail_package,
       cdetail_deliverydate,
       cdetail_assembly_cart,
       cdetail_campaign_detail,
       cdetail_qtyExpandCart,
       cdetail_reservation,
       cdetail_assigned_member,
       LastModifiedTimestamp,
       cdetail_service_expired,
       isnull(cast(stage_lt_bucks_CartDetails.cdetail_deliverydate as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_lt_bucks_CartDetails
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_lt_bucks_cart_details @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_lt_bucks_cart_details (
       bk_hash,
       cdetail_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_lt_bucks_CartDetails.bk_hash,
       stage_hash_lt_bucks_CartDetails.cdetail_id cdetail_id,
       isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_deliverydate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       5,
       @insert_date_time,
       @user
  from stage_hash_lt_bucks_CartDetails
  left join h_lt_bucks_cart_details
    on stage_hash_lt_bucks_CartDetails.bk_hash = h_lt_bucks_cart_details.bk_hash
 where h_lt_bucks_cart_details_id is null
   and stage_hash_lt_bucks_CartDetails.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_lt_bucks_cart_details
if object_id('tempdb..#l_lt_bucks_cart_details_inserts') is not null drop table #l_lt_bucks_cart_details_inserts
create table #l_lt_bucks_cart_details_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_CartDetails.bk_hash,
       stage_hash_lt_bucks_CartDetails.cdetail_id cdetail_id,
       stage_hash_lt_bucks_CartDetails.cdetail_cart cdetail_cart,
       stage_hash_lt_bucks_CartDetails.cdetail_poption cdetail_poption,
       stage_hash_lt_bucks_CartDetails.cdetail_club cdetail_club,
       stage_hash_lt_bucks_CartDetails.cdetail_transactionkey cdetail_transaction_key,
       stage_hash_lt_bucks_CartDetails.cdetail_package cdetail_package,
       stage_hash_lt_bucks_CartDetails.cdetail_assembly_cart cdetail_assembly_cart,
       stage_hash_lt_bucks_CartDetails.cdetail_qtyExpandCart cdetail_qty_expand_cart,
       isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_deliverydate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_cart as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_poption as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_club as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_transactionkey as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_package as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_assembly_cart as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_qtyExpandCart as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_CartDetails
 where stage_hash_lt_bucks_CartDetails.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_lt_bucks_cart_details records
set @insert_date_time = getdate()
insert into l_lt_bucks_cart_details (
       bk_hash,
       cdetail_id,
       cdetail_cart,
       cdetail_poption,
       cdetail_club,
       cdetail_transaction_key,
       cdetail_package,
       cdetail_assembly_cart,
       cdetail_qty_expand_cart,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_lt_bucks_cart_details_inserts.bk_hash,
       #l_lt_bucks_cart_details_inserts.cdetail_id,
       #l_lt_bucks_cart_details_inserts.cdetail_cart,
       #l_lt_bucks_cart_details_inserts.cdetail_poption,
       #l_lt_bucks_cart_details_inserts.cdetail_club,
       #l_lt_bucks_cart_details_inserts.cdetail_transaction_key,
       #l_lt_bucks_cart_details_inserts.cdetail_package,
       #l_lt_bucks_cart_details_inserts.cdetail_assembly_cart,
       #l_lt_bucks_cart_details_inserts.cdetail_qty_expand_cart,
       case when l_lt_bucks_cart_details.l_lt_bucks_cart_details_id is null then isnull(#l_lt_bucks_cart_details_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #l_lt_bucks_cart_details_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_lt_bucks_cart_details_inserts
  left join p_lt_bucks_cart_details
    on #l_lt_bucks_cart_details_inserts.bk_hash = p_lt_bucks_cart_details.bk_hash
   and p_lt_bucks_cart_details.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_lt_bucks_cart_details
    on p_lt_bucks_cart_details.bk_hash = l_lt_bucks_cart_details.bk_hash
   and p_lt_bucks_cart_details.l_lt_bucks_cart_details_id = l_lt_bucks_cart_details.l_lt_bucks_cart_details_id
 where l_lt_bucks_cart_details.l_lt_bucks_cart_details_id is null
    or (l_lt_bucks_cart_details.l_lt_bucks_cart_details_id is not null
        and l_lt_bucks_cart_details.dv_hash <> #l_lt_bucks_cart_details_inserts.source_hash)

--calculate hash and lookup to current s_lt_bucks_cart_details
if object_id('tempdb..#s_lt_bucks_cart_details_inserts') is not null drop table #s_lt_bucks_cart_details_inserts
create table #s_lt_bucks_cart_details_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_CartDetails.bk_hash,
       stage_hash_lt_bucks_CartDetails.cdetail_id cdetail_id,
       stage_hash_lt_bucks_CartDetails.cdetail_expiration_date cdetail_expiration_date,
       stage_hash_lt_bucks_CartDetails.cdetail_deliverydate cdetail_delivery_date,
       stage_hash_lt_bucks_CartDetails.cdetail_campaign_detail cdetail_campaign_detail,
       stage_hash_lt_bucks_CartDetails.cdetail_reservation cdetail_reservation,
       stage_hash_lt_bucks_CartDetails.cdetail_assigned_member cdetail_assigned_member,
       stage_hash_lt_bucks_CartDetails.LastModifiedTimestamp last_modified_timestamp,
       stage_hash_lt_bucks_CartDetails.cdetail_service_expired cdetail_service_expired,
       isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_deliverydate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_CartDetails.cdetail_expiration_date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_CartDetails.cdetail_deliverydate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_campaign_detail as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_reservation as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_CartDetails.cdetail_assigned_member,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_CartDetails.LastModifiedTimestamp,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_CartDetails.cdetail_service_expired as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_CartDetails
 where stage_hash_lt_bucks_CartDetails.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_lt_bucks_cart_details records
set @insert_date_time = getdate()
insert into s_lt_bucks_cart_details (
       bk_hash,
       cdetail_id,
       cdetail_expiration_date,
       cdetail_delivery_date,
       cdetail_campaign_detail,
       cdetail_reservation,
       cdetail_assigned_member,
       last_modified_timestamp,
       cdetail_service_expired,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_lt_bucks_cart_details_inserts.bk_hash,
       #s_lt_bucks_cart_details_inserts.cdetail_id,
       #s_lt_bucks_cart_details_inserts.cdetail_expiration_date,
       #s_lt_bucks_cart_details_inserts.cdetail_delivery_date,
       #s_lt_bucks_cart_details_inserts.cdetail_campaign_detail,
       #s_lt_bucks_cart_details_inserts.cdetail_reservation,
       #s_lt_bucks_cart_details_inserts.cdetail_assigned_member,
       #s_lt_bucks_cart_details_inserts.last_modified_timestamp,
       #s_lt_bucks_cart_details_inserts.cdetail_service_expired,
       case when s_lt_bucks_cart_details.s_lt_bucks_cart_details_id is null then isnull(#s_lt_bucks_cart_details_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #s_lt_bucks_cart_details_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_lt_bucks_cart_details_inserts
  left join p_lt_bucks_cart_details
    on #s_lt_bucks_cart_details_inserts.bk_hash = p_lt_bucks_cart_details.bk_hash
   and p_lt_bucks_cart_details.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_lt_bucks_cart_details
    on p_lt_bucks_cart_details.bk_hash = s_lt_bucks_cart_details.bk_hash
   and p_lt_bucks_cart_details.s_lt_bucks_cart_details_id = s_lt_bucks_cart_details.s_lt_bucks_cart_details_id
 where s_lt_bucks_cart_details.s_lt_bucks_cart_details_id is null
    or (s_lt_bucks_cart_details.s_lt_bucks_cart_details_id is not null
        and s_lt_bucks_cart_details.dv_hash <> #s_lt_bucks_cart_details_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_lt_bucks_cart_details @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_lt_bucks_cart_details @current_dv_batch_id

end
