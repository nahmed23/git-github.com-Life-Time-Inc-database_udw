CREATE PROC [dbo].[proc_etl_hybris_ltf_refund_entry] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_ltfrefundentry

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_ltfrefundentry (
       bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       p_refundeddate,
       p_reason,
       p_amount,
       p_refundnote,
       p_refundstatus,
       p_orderentries,
       p_refundpaytype,
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
       p_refundeddate,
       p_reason,
       p_amount,
       p_refundnote,
       p_refundstatus,
       p_orderentries,
       p_refundpaytype,
       isnull(cast(stage_hybris_ltfrefundentry.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_ltfrefundentry
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_ltf_refund_entry @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_ltf_refund_entry (
       bk_hash,
       ltf_refund_entry_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_ltfrefundentry.bk_hash,
       stage_hash_hybris_ltfrefundentry.[PK] ltf_refund_entry_pk,
       isnull(cast(stage_hash_hybris_ltfrefundentry.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_ltfrefundentry
  left join h_hybris_ltf_refund_entry
    on stage_hash_hybris_ltfrefundentry.bk_hash = h_hybris_ltf_refund_entry.bk_hash
 where h_hybris_ltf_refund_entry_id is null
   and stage_hash_hybris_ltfrefundentry.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_ltf_refund_entry
if object_id('tempdb..#l_hybris_ltf_refund_entry_inserts') is not null drop table #l_hybris_ltf_refund_entry_inserts
create table #l_hybris_ltf_refund_entry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_ltfrefundentry.bk_hash,
       stage_hash_hybris_ltfrefundentry.TypePkString type_pk_string,
       stage_hash_hybris_ltfrefundentry.[PK] ltf_refund_entry_pk,
       stage_hash_hybris_ltfrefundentry.OwnerPkString owner_pk_string,
       stage_hash_hybris_ltfrefundentry.p_reason p_reason,
       stage_hash_hybris_ltfrefundentry.p_refundstatus p_refund_status,
       stage_hash_hybris_ltfrefundentry.p_orderentries p_order_entries,
       stage_hash_hybris_ltfrefundentry.p_refundpaytype p_refund_pay_type,
       isnull(cast(stage_hash_hybris_ltfrefundentry.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.p_reason as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.p_refundstatus as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.p_orderentries as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.p_refundpaytype as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_ltfrefundentry
 where stage_hash_hybris_ltfrefundentry.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_ltf_refund_entry records
set @insert_date_time = getdate()
insert into l_hybris_ltf_refund_entry (
       bk_hash,
       type_pk_string,
       ltf_refund_entry_pk,
       owner_pk_string,
       p_reason,
       p_refund_status,
       p_order_entries,
       p_refund_pay_type,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_ltf_refund_entry_inserts.bk_hash,
       #l_hybris_ltf_refund_entry_inserts.type_pk_string,
       #l_hybris_ltf_refund_entry_inserts.ltf_refund_entry_pk,
       #l_hybris_ltf_refund_entry_inserts.owner_pk_string,
       #l_hybris_ltf_refund_entry_inserts.p_reason,
       #l_hybris_ltf_refund_entry_inserts.p_refund_status,
       #l_hybris_ltf_refund_entry_inserts.p_order_entries,
       #l_hybris_ltf_refund_entry_inserts.p_refund_pay_type,
       case when l_hybris_ltf_refund_entry.l_hybris_ltf_refund_entry_id is null then isnull(#l_hybris_ltf_refund_entry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_ltf_refund_entry_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_ltf_refund_entry_inserts
  left join p_hybris_ltf_refund_entry
    on #l_hybris_ltf_refund_entry_inserts.bk_hash = p_hybris_ltf_refund_entry.bk_hash
   and p_hybris_ltf_refund_entry.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_ltf_refund_entry
    on p_hybris_ltf_refund_entry.bk_hash = l_hybris_ltf_refund_entry.bk_hash
   and p_hybris_ltf_refund_entry.l_hybris_ltf_refund_entry_id = l_hybris_ltf_refund_entry.l_hybris_ltf_refund_entry_id
 where l_hybris_ltf_refund_entry.l_hybris_ltf_refund_entry_id is null
    or (l_hybris_ltf_refund_entry.l_hybris_ltf_refund_entry_id is not null
        and l_hybris_ltf_refund_entry.dv_hash <> #l_hybris_ltf_refund_entry_inserts.source_hash)

--calculate hash and lookup to current s_hybris_ltf_refund_entry
if object_id('tempdb..#s_hybris_ltf_refund_entry_inserts') is not null drop table #s_hybris_ltf_refund_entry_inserts
create table #s_hybris_ltf_refund_entry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_ltfrefundentry.bk_hash,
       stage_hash_hybris_ltfrefundentry.hjmpTS hjmpts,
       stage_hash_hybris_ltfrefundentry.[PK] ltf_refund_entry_pk,
       stage_hash_hybris_ltfrefundentry.createdTS created_ts,
       stage_hash_hybris_ltfrefundentry.modifiedTS modified_ts,
       stage_hash_hybris_ltfrefundentry.aCLTS acl_ts,
       stage_hash_hybris_ltfrefundentry.propTS prop_ts,
       stage_hash_hybris_ltfrefundentry.p_refundeddate p_refunded_date,
       stage_hash_hybris_ltfrefundentry.p_amount p_amount,
       stage_hash_hybris_ltfrefundentry.p_refundnote p_refund_note,
       isnull(cast(stage_hash_hybris_ltfrefundentry.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ltfrefundentry.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ltfrefundentry.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ltfrefundentry.p_refundeddate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ltfrefundentry.p_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ltfrefundentry.p_refundnote,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_ltfrefundentry
 where stage_hash_hybris_ltfrefundentry.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_ltf_refund_entry records
set @insert_date_time = getdate()
insert into s_hybris_ltf_refund_entry (
       bk_hash,
       hjmpts,
       ltf_refund_entry_pk,
       created_ts,
       modified_ts,
       acl_ts,
       prop_ts,
       p_refunded_date,
       p_amount,
       p_refund_note,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_ltf_refund_entry_inserts.bk_hash,
       #s_hybris_ltf_refund_entry_inserts.hjmpts,
       #s_hybris_ltf_refund_entry_inserts.ltf_refund_entry_pk,
       #s_hybris_ltf_refund_entry_inserts.created_ts,
       #s_hybris_ltf_refund_entry_inserts.modified_ts,
       #s_hybris_ltf_refund_entry_inserts.acl_ts,
       #s_hybris_ltf_refund_entry_inserts.prop_ts,
       #s_hybris_ltf_refund_entry_inserts.p_refunded_date,
       #s_hybris_ltf_refund_entry_inserts.p_amount,
       #s_hybris_ltf_refund_entry_inserts.p_refund_note,
       case when s_hybris_ltf_refund_entry.s_hybris_ltf_refund_entry_id is null then isnull(#s_hybris_ltf_refund_entry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_ltf_refund_entry_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_ltf_refund_entry_inserts
  left join p_hybris_ltf_refund_entry
    on #s_hybris_ltf_refund_entry_inserts.bk_hash = p_hybris_ltf_refund_entry.bk_hash
   and p_hybris_ltf_refund_entry.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_ltf_refund_entry
    on p_hybris_ltf_refund_entry.bk_hash = s_hybris_ltf_refund_entry.bk_hash
   and p_hybris_ltf_refund_entry.s_hybris_ltf_refund_entry_id = s_hybris_ltf_refund_entry.s_hybris_ltf_refund_entry_id
 where s_hybris_ltf_refund_entry.s_hybris_ltf_refund_entry_id is null
    or (s_hybris_ltf_refund_entry.s_hybris_ltf_refund_entry_id is not null
        and s_hybris_ltf_refund_entry.dv_hash <> #s_hybris_ltf_refund_entry_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_ltf_refund_entry @current_dv_batch_id

end
