CREATE PROC [dbo].[proc_etl_hybris_return_entry] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_returnentry

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_returnentry (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_orderentry,
       p_expectedquantity,
       p_receivedquantity,
       p_reacheddate,
       p_status,
       p_action,
       p_notes,
       p_returnrequestpos,
       p_returnrequest,
       aCLTS,
       propTS,
       p_reason,
       p_amount,
       p_refundeddate,
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
       p_orderentry,
       p_expectedquantity,
       p_receivedquantity,
       p_reacheddate,
       p_status,
       p_action,
       p_notes,
       p_returnrequestpos,
       p_returnrequest,
       aCLTS,
       propTS,
       p_reason,
       p_amount,
       p_refundeddate,
       isnull(cast(stage_hybris_returnentry.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_returnentry
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_return_entry @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_return_entry (
       bk_hash,
       return_entry_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_returnentry.bk_hash,
       stage_hash_hybris_returnentry.[PK] return_entry_pk,
       isnull(cast(stage_hash_hybris_returnentry.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_returnentry
  left join h_hybris_return_entry
    on stage_hash_hybris_returnentry.bk_hash = h_hybris_return_entry.bk_hash
 where h_hybris_return_entry_id is null
   and stage_hash_hybris_returnentry.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_return_entry
if object_id('tempdb..#l_hybris_return_entry_inserts') is not null drop table #l_hybris_return_entry_inserts
create table #l_hybris_return_entry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_returnentry.bk_hash,
       stage_hash_hybris_returnentry.TypePkString type_pk_string,
       stage_hash_hybris_returnentry.OwnerPkString owner_pk_string,
       stage_hash_hybris_returnentry.[PK] return_entry_pk,
       stage_hash_hybris_returnentry.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.[PK] as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_returnentry
 where stage_hash_hybris_returnentry.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_return_entry records
set @insert_date_time = getdate()
insert into l_hybris_return_entry (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       return_entry_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_return_entry_inserts.bk_hash,
       #l_hybris_return_entry_inserts.type_pk_string,
       #l_hybris_return_entry_inserts.owner_pk_string,
       #l_hybris_return_entry_inserts.return_entry_pk,
       case when l_hybris_return_entry.l_hybris_return_entry_id is null then isnull(#l_hybris_return_entry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_return_entry_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_return_entry_inserts
  left join p_hybris_return_entry
    on #l_hybris_return_entry_inserts.bk_hash = p_hybris_return_entry.bk_hash
   and p_hybris_return_entry.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_return_entry
    on p_hybris_return_entry.bk_hash = l_hybris_return_entry.bk_hash
   and p_hybris_return_entry.l_hybris_return_entry_id = l_hybris_return_entry.l_hybris_return_entry_id
 where l_hybris_return_entry.l_hybris_return_entry_id is null
    or (l_hybris_return_entry.l_hybris_return_entry_id is not null
        and l_hybris_return_entry.dv_hash <> #l_hybris_return_entry_inserts.source_hash)

--calculate hash and lookup to current s_hybris_return_entry
if object_id('tempdb..#s_hybris_return_entry_inserts') is not null drop table #s_hybris_return_entry_inserts
create table #s_hybris_return_entry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_returnentry.bk_hash,
       stage_hash_hybris_returnentry.hjmpTS hjmp_ts,
       stage_hash_hybris_returnentry.createdTS created_ts,
       stage_hash_hybris_returnentry.modifiedTS modified_ts,
       stage_hash_hybris_returnentry.[PK] return_entry_pk,
       stage_hash_hybris_returnentry.p_orderentry p_order_entry,
       stage_hash_hybris_returnentry.p_expectedquantity p_expected_quantity,
       stage_hash_hybris_returnentry.p_receivedquantity p_received_quantity,
       stage_hash_hybris_returnentry.p_reacheddate p_reached_date,
       stage_hash_hybris_returnentry.p_status p_status,
       stage_hash_hybris_returnentry.p_action p_action,
       stage_hash_hybris_returnentry.p_notes p_notes,
       stage_hash_hybris_returnentry.p_returnrequestpos p_return_request_pos,
       stage_hash_hybris_returnentry.p_returnrequest p_return_request,
       stage_hash_hybris_returnentry.aCLTS acl_ts,
       stage_hash_hybris_returnentry.propTS prop_ts,
       stage_hash_hybris_returnentry.p_reason p_reason,
       stage_hash_hybris_returnentry.p_amount p_amount,
       stage_hash_hybris_returnentry.p_refundeddate p_refunded_date,
       stage_hash_hybris_returnentry.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_returnentry.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_returnentry.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.p_orderentry as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.p_expectedquantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.p_receivedquantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_returnentry.p_reacheddate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.p_status as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.p_action as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_returnentry.p_notes,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.p_returnrequestpos as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.p_returnrequest as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.p_reason as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_returnentry.p_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_returnentry.p_refundeddate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_returnentry
 where stage_hash_hybris_returnentry.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_return_entry records
set @insert_date_time = getdate()
insert into s_hybris_return_entry (
       bk_hash,
       hjmp_ts,
       created_ts,
       modified_ts,
       return_entry_pk,
       p_order_entry,
       p_expected_quantity,
       p_received_quantity,
       p_reached_date,
       p_status,
       p_action,
       p_notes,
       p_return_request_pos,
       p_return_request,
       acl_ts,
       prop_ts,
       p_reason,
       p_amount,
       p_refunded_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_return_entry_inserts.bk_hash,
       #s_hybris_return_entry_inserts.hjmp_ts,
       #s_hybris_return_entry_inserts.created_ts,
       #s_hybris_return_entry_inserts.modified_ts,
       #s_hybris_return_entry_inserts.return_entry_pk,
       #s_hybris_return_entry_inserts.p_order_entry,
       #s_hybris_return_entry_inserts.p_expected_quantity,
       #s_hybris_return_entry_inserts.p_received_quantity,
       #s_hybris_return_entry_inserts.p_reached_date,
       #s_hybris_return_entry_inserts.p_status,
       #s_hybris_return_entry_inserts.p_action,
       #s_hybris_return_entry_inserts.p_notes,
       #s_hybris_return_entry_inserts.p_return_request_pos,
       #s_hybris_return_entry_inserts.p_return_request,
       #s_hybris_return_entry_inserts.acl_ts,
       #s_hybris_return_entry_inserts.prop_ts,
       #s_hybris_return_entry_inserts.p_reason,
       #s_hybris_return_entry_inserts.p_amount,
       #s_hybris_return_entry_inserts.p_refunded_date,
       case when s_hybris_return_entry.s_hybris_return_entry_id is null then isnull(#s_hybris_return_entry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_return_entry_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_return_entry_inserts
  left join p_hybris_return_entry
    on #s_hybris_return_entry_inserts.bk_hash = p_hybris_return_entry.bk_hash
   and p_hybris_return_entry.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_return_entry
    on p_hybris_return_entry.bk_hash = s_hybris_return_entry.bk_hash
   and p_hybris_return_entry.s_hybris_return_entry_id = s_hybris_return_entry.s_hybris_return_entry_id
 where s_hybris_return_entry.s_hybris_return_entry_id is null
    or (s_hybris_return_entry.s_hybris_return_entry_id is not null
        and s_hybris_return_entry.dv_hash <> #s_hybris_return_entry_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_return_entry @current_dv_batch_id

end
