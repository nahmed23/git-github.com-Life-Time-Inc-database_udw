CREATE PROC [dbo].[proc_etl_mdm_golden_record_customer_linkage] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mdm_GoldenRecordCustomerLinkage

set @insert_date_time = getdate()
insert into dbo.stage_hash_mdm_GoldenRecordCustomerLinkage (
       bk_hash,
       LoadDateTime,
       RowNumber,
       SourceCode,
       SourceID,
       EventDateTime,
       EventType,
       CurrentEntityID,
       PreviousEntityID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,LoadDateTime,120),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(RowNumber as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(SourceCode,'z#@$k%&P')+'P%#&z$@k'+isnull(SourceID,'z#@$k%&P')+'P%#&z$@k'+isnull(convert(varchar,EventDateTime,120),'z#@$k%&P')+'P%#&z$@k'+isnull(EventType,'z#@$k%&P'))),2) bk_hash,
       LoadDateTime,
       RowNumber,
       SourceCode,
       SourceID,
       EventDateTime,
       EventType,
       CurrentEntityID,
       PreviousEntityID,
       isnull(cast(stage_mdm_GoldenRecordCustomerLinkage.LoadDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mdm_GoldenRecordCustomerLinkage
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mdm_golden_record_customer_linkage @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mdm_golden_record_customer_linkage (
       bk_hash,
       load_date_time,
       row_number,
       source_code,
       source_id,
       event_date_time,
       event_type,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mdm_GoldenRecordCustomerLinkage.bk_hash,
       stage_hash_mdm_GoldenRecordCustomerLinkage.LoadDateTime load_date_time,
       stage_hash_mdm_GoldenRecordCustomerLinkage.RowNumber row_number,
       stage_hash_mdm_GoldenRecordCustomerLinkage.SourceCode source_code,
       stage_hash_mdm_GoldenRecordCustomerLinkage.SourceID source_id,
       stage_hash_mdm_GoldenRecordCustomerLinkage.EventDateTime event_date_time,
       stage_hash_mdm_GoldenRecordCustomerLinkage.EventType event_type,
       isnull(cast(stage_hash_mdm_GoldenRecordCustomerLinkage.LoadDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       25,
       @insert_date_time,
       @user
  from stage_hash_mdm_GoldenRecordCustomerLinkage
  left join h_mdm_golden_record_customer_linkage
    on stage_hash_mdm_GoldenRecordCustomerLinkage.bk_hash = h_mdm_golden_record_customer_linkage.bk_hash
 where h_mdm_golden_record_customer_linkage_id is null
   and stage_hash_mdm_GoldenRecordCustomerLinkage.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mdm_golden_record_customer_linkage
if object_id('tempdb..#l_mdm_golden_record_customer_linkage_inserts') is not null drop table #l_mdm_golden_record_customer_linkage_inserts
create table #l_mdm_golden_record_customer_linkage_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mdm_GoldenRecordCustomerLinkage.bk_hash,
       stage_hash_mdm_GoldenRecordCustomerLinkage.LoadDateTime load_date_time,
       stage_hash_mdm_GoldenRecordCustomerLinkage.RowNumber row_number,
       stage_hash_mdm_GoldenRecordCustomerLinkage.SourceCode source_code,
       stage_hash_mdm_GoldenRecordCustomerLinkage.SourceID source_id,
       stage_hash_mdm_GoldenRecordCustomerLinkage.EventDateTime event_date_time,
       stage_hash_mdm_GoldenRecordCustomerLinkage.EventType event_type,
       stage_hash_mdm_GoldenRecordCustomerLinkage.CurrentEntityID current_entity_id,
       stage_hash_mdm_GoldenRecordCustomerLinkage.PreviousEntityID previous_entity_id,
       stage_hash_mdm_GoldenRecordCustomerLinkage.LoadDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_mdm_GoldenRecordCustomerLinkage.LoadDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mdm_GoldenRecordCustomerLinkage.RowNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerLinkage.SourceCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerLinkage.SourceID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mdm_GoldenRecordCustomerLinkage.EventDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerLinkage.EventType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mdm_GoldenRecordCustomerLinkage.CurrentEntityID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mdm_GoldenRecordCustomerLinkage.PreviousEntityID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mdm_GoldenRecordCustomerLinkage
 where stage_hash_mdm_GoldenRecordCustomerLinkage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mdm_golden_record_customer_linkage records
set @insert_date_time = getdate()
insert into l_mdm_golden_record_customer_linkage (
       bk_hash,
       load_date_time,
       row_number,
       source_code,
       source_id,
       event_date_time,
       event_type,
       current_entity_id,
       previous_entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mdm_golden_record_customer_linkage_inserts.bk_hash,
       #l_mdm_golden_record_customer_linkage_inserts.load_date_time,
       #l_mdm_golden_record_customer_linkage_inserts.row_number,
       #l_mdm_golden_record_customer_linkage_inserts.source_code,
       #l_mdm_golden_record_customer_linkage_inserts.source_id,
       #l_mdm_golden_record_customer_linkage_inserts.event_date_time,
       #l_mdm_golden_record_customer_linkage_inserts.event_type,
       #l_mdm_golden_record_customer_linkage_inserts.current_entity_id,
       #l_mdm_golden_record_customer_linkage_inserts.previous_entity_id,
       case when l_mdm_golden_record_customer_linkage.l_mdm_golden_record_customer_linkage_id is null then isnull(#l_mdm_golden_record_customer_linkage_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       25,
       #l_mdm_golden_record_customer_linkage_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mdm_golden_record_customer_linkage_inserts
  left join p_mdm_golden_record_customer_linkage
    on #l_mdm_golden_record_customer_linkage_inserts.bk_hash = p_mdm_golden_record_customer_linkage.bk_hash
   and p_mdm_golden_record_customer_linkage.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mdm_golden_record_customer_linkage
    on p_mdm_golden_record_customer_linkage.bk_hash = l_mdm_golden_record_customer_linkage.bk_hash
   and p_mdm_golden_record_customer_linkage.l_mdm_golden_record_customer_linkage_id = l_mdm_golden_record_customer_linkage.l_mdm_golden_record_customer_linkage_id
 where l_mdm_golden_record_customer_linkage.l_mdm_golden_record_customer_linkage_id is null
    or (l_mdm_golden_record_customer_linkage.l_mdm_golden_record_customer_linkage_id is not null
        and l_mdm_golden_record_customer_linkage.dv_hash <> #l_mdm_golden_record_customer_linkage_inserts.source_hash)

--calculate hash and lookup to current s_mdm_golden_record_customer_linkage
if object_id('tempdb..#s_mdm_golden_record_customer_linkage_inserts') is not null drop table #s_mdm_golden_record_customer_linkage_inserts
create table #s_mdm_golden_record_customer_linkage_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mdm_GoldenRecordCustomerLinkage.bk_hash,
       stage_hash_mdm_GoldenRecordCustomerLinkage.LoadDateTime load_date_time,
       stage_hash_mdm_GoldenRecordCustomerLinkage.RowNumber row_number,
       stage_hash_mdm_GoldenRecordCustomerLinkage.SourceCode source_code,
       stage_hash_mdm_GoldenRecordCustomerLinkage.SourceID source_id,
       stage_hash_mdm_GoldenRecordCustomerLinkage.EventDateTime event_date_time,
       stage_hash_mdm_GoldenRecordCustomerLinkage.EventType event_type,
       stage_hash_mdm_GoldenRecordCustomerLinkage.LoadDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_mdm_GoldenRecordCustomerLinkage.LoadDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mdm_GoldenRecordCustomerLinkage.RowNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerLinkage.SourceCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerLinkage.SourceID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mdm_GoldenRecordCustomerLinkage.EventDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerLinkage.EventType,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mdm_GoldenRecordCustomerLinkage
 where stage_hash_mdm_GoldenRecordCustomerLinkage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mdm_golden_record_customer_linkage records
set @insert_date_time = getdate()
insert into s_mdm_golden_record_customer_linkage (
       bk_hash,
       load_date_time,
       row_number,
       source_code,
       source_id,
       event_date_time,
       event_type,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mdm_golden_record_customer_linkage_inserts.bk_hash,
       #s_mdm_golden_record_customer_linkage_inserts.load_date_time,
       #s_mdm_golden_record_customer_linkage_inserts.row_number,
       #s_mdm_golden_record_customer_linkage_inserts.source_code,
       #s_mdm_golden_record_customer_linkage_inserts.source_id,
       #s_mdm_golden_record_customer_linkage_inserts.event_date_time,
       #s_mdm_golden_record_customer_linkage_inserts.event_type,
       case when s_mdm_golden_record_customer_linkage.s_mdm_golden_record_customer_linkage_id is null then isnull(#s_mdm_golden_record_customer_linkage_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       25,
       #s_mdm_golden_record_customer_linkage_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mdm_golden_record_customer_linkage_inserts
  left join p_mdm_golden_record_customer_linkage
    on #s_mdm_golden_record_customer_linkage_inserts.bk_hash = p_mdm_golden_record_customer_linkage.bk_hash
   and p_mdm_golden_record_customer_linkage.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mdm_golden_record_customer_linkage
    on p_mdm_golden_record_customer_linkage.bk_hash = s_mdm_golden_record_customer_linkage.bk_hash
   and p_mdm_golden_record_customer_linkage.s_mdm_golden_record_customer_linkage_id = s_mdm_golden_record_customer_linkage.s_mdm_golden_record_customer_linkage_id
 where s_mdm_golden_record_customer_linkage.s_mdm_golden_record_customer_linkage_id is null
    or (s_mdm_golden_record_customer_linkage.s_mdm_golden_record_customer_linkage_id is not null
        and s_mdm_golden_record_customer_linkage.dv_hash <> #s_mdm_golden_record_customer_linkage_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mdm_golden_record_customer_linkage @current_dv_batch_id

end
