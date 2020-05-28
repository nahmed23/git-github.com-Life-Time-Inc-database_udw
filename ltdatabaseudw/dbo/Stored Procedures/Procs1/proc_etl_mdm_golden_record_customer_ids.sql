CREATE PROC [dbo].[proc_etl_mdm_golden_record_customer_ids] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mdm_GoldenRecordCustomerIDS

set @insert_date_time = getdate()
insert into dbo.stage_hash_mdm_GoldenRecordCustomerIDS (
       bk_hash,
       LoadDateTime,
       EntityID,
       SourceCode,
       SourceID,
       InsertedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(EntityID,'z#@$k%&P')+'P%#&z$@k'+isnull(SourceCode,'z#@$k%&P')+'P%#&z$@k'+isnull(SourceID,'z#@$k%&P'))),2) bk_hash,
       LoadDateTime,
       EntityID,
       SourceCode,
       SourceID,
       InsertedDateTime,
       isnull(cast(stage_mdm_GoldenRecordCustomerIDS.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mdm_GoldenRecordCustomerIDS
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mdm_golden_record_customer_ids @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mdm_golden_record_customer_ids (
       bk_hash,
       entity_id,
       source_code,
       source_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mdm_GoldenRecordCustomerIDS.bk_hash,
       stage_hash_mdm_GoldenRecordCustomerIDS.EntityID entity_id,
       stage_hash_mdm_GoldenRecordCustomerIDS.SourceCode source_code,
       stage_hash_mdm_GoldenRecordCustomerIDS.SourceID source_id,
       isnull(cast(stage_hash_mdm_GoldenRecordCustomerIDS.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       25,
       @insert_date_time,
       @user
  from stage_hash_mdm_GoldenRecordCustomerIDS
  left join h_mdm_golden_record_customer_ids
    on stage_hash_mdm_GoldenRecordCustomerIDS.bk_hash = h_mdm_golden_record_customer_ids.bk_hash
 where h_mdm_golden_record_customer_ids_id is null
   and stage_hash_mdm_GoldenRecordCustomerIDS.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mdm_golden_record_customer_ids
if object_id('tempdb..#l_mdm_golden_record_customer_ids_inserts') is not null drop table #l_mdm_golden_record_customer_ids_inserts
create table #l_mdm_golden_record_customer_ids_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mdm_GoldenRecordCustomerIDS.bk_hash,
       stage_hash_mdm_GoldenRecordCustomerIDS.EntityID entity_id,
       stage_hash_mdm_GoldenRecordCustomerIDS.SourceCode source_code,
       stage_hash_mdm_GoldenRecordCustomerIDS.SourceID source_id,
       isnull(cast(stage_hash_mdm_GoldenRecordCustomerIDS.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerIDS.EntityID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerIDS.SourceCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerIDS.SourceID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mdm_GoldenRecordCustomerIDS
 where stage_hash_mdm_GoldenRecordCustomerIDS.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mdm_golden_record_customer_ids records
set @insert_date_time = getdate()
insert into l_mdm_golden_record_customer_ids (
       bk_hash,
       entity_id,
       source_code,
       source_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mdm_golden_record_customer_ids_inserts.bk_hash,
       #l_mdm_golden_record_customer_ids_inserts.entity_id,
       #l_mdm_golden_record_customer_ids_inserts.source_code,
       #l_mdm_golden_record_customer_ids_inserts.source_id,
       case when l_mdm_golden_record_customer_ids.l_mdm_golden_record_customer_ids_id is null then isnull(#l_mdm_golden_record_customer_ids_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       25,
       #l_mdm_golden_record_customer_ids_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mdm_golden_record_customer_ids_inserts
  left join p_mdm_golden_record_customer_ids
    on #l_mdm_golden_record_customer_ids_inserts.bk_hash = p_mdm_golden_record_customer_ids.bk_hash
   and p_mdm_golden_record_customer_ids.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mdm_golden_record_customer_ids
    on p_mdm_golden_record_customer_ids.bk_hash = l_mdm_golden_record_customer_ids.bk_hash
   and p_mdm_golden_record_customer_ids.l_mdm_golden_record_customer_ids_id = l_mdm_golden_record_customer_ids.l_mdm_golden_record_customer_ids_id
 where l_mdm_golden_record_customer_ids.l_mdm_golden_record_customer_ids_id is null
    or (l_mdm_golden_record_customer_ids.l_mdm_golden_record_customer_ids_id is not null
        and l_mdm_golden_record_customer_ids.dv_hash <> #l_mdm_golden_record_customer_ids_inserts.source_hash)

--calculate hash and lookup to current s_mdm_golden_record_customer_ids
if object_id('tempdb..#s_mdm_golden_record_customer_ids_inserts') is not null drop table #s_mdm_golden_record_customer_ids_inserts
create table #s_mdm_golden_record_customer_ids_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mdm_GoldenRecordCustomerIDS.bk_hash,
       stage_hash_mdm_GoldenRecordCustomerIDS.LoadDateTime load_date_time,
       stage_hash_mdm_GoldenRecordCustomerIDS.EntityID entity_id,
       stage_hash_mdm_GoldenRecordCustomerIDS.SourceCode source_code,
       stage_hash_mdm_GoldenRecordCustomerIDS.SourceID source_id,
       stage_hash_mdm_GoldenRecordCustomerIDS.InsertedDateTime inserted_date_time,
       isnull(cast(stage_hash_mdm_GoldenRecordCustomerIDS.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_mdm_GoldenRecordCustomerIDS.LoadDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerIDS.EntityID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerIDS.SourceCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mdm_GoldenRecordCustomerIDS.SourceID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mdm_GoldenRecordCustomerIDS.InsertedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mdm_GoldenRecordCustomerIDS
 where stage_hash_mdm_GoldenRecordCustomerIDS.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mdm_golden_record_customer_ids records
set @insert_date_time = getdate()
insert into s_mdm_golden_record_customer_ids (
       bk_hash,
       load_date_time,
       entity_id,
       source_code,
       source_id,
       inserted_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mdm_golden_record_customer_ids_inserts.bk_hash,
       #s_mdm_golden_record_customer_ids_inserts.load_date_time,
       #s_mdm_golden_record_customer_ids_inserts.entity_id,
       #s_mdm_golden_record_customer_ids_inserts.source_code,
       #s_mdm_golden_record_customer_ids_inserts.source_id,
       #s_mdm_golden_record_customer_ids_inserts.inserted_date_time,
       case when s_mdm_golden_record_customer_ids.s_mdm_golden_record_customer_ids_id is null then isnull(#s_mdm_golden_record_customer_ids_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       25,
       #s_mdm_golden_record_customer_ids_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mdm_golden_record_customer_ids_inserts
  left join p_mdm_golden_record_customer_ids
    on #s_mdm_golden_record_customer_ids_inserts.bk_hash = p_mdm_golden_record_customer_ids.bk_hash
   and p_mdm_golden_record_customer_ids.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mdm_golden_record_customer_ids
    on p_mdm_golden_record_customer_ids.bk_hash = s_mdm_golden_record_customer_ids.bk_hash
   and p_mdm_golden_record_customer_ids.s_mdm_golden_record_customer_ids_id = s_mdm_golden_record_customer_ids.s_mdm_golden_record_customer_ids_id
 where s_mdm_golden_record_customer_ids.s_mdm_golden_record_customer_ids_id is null
    or (s_mdm_golden_record_customer_ids.s_mdm_golden_record_customer_ids_id is not null
        and s_mdm_golden_record_customer_ids.dv_hash <> #s_mdm_golden_record_customer_ids_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mdm_golden_record_customer_ids @current_dv_batch_id

end
