CREATE PROC [dbo].[proc_etl_exerp_message] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_message

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_message (
       bk_hash,
       id,
       person_id,
       company_id,
       creation_datetime,
       delivery_datetime,
       delivery_method,
       delivered_by_person_id,
       template_id,
       type,
       ref_type,
       ref_id,
       subject,
       from_person_id,
       channel,
       message_category,
       center_id,
       ets,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       person_id,
       company_id,
       creation_datetime,
       delivery_datetime,
       delivery_method,
       delivered_by_person_id,
       template_id,
       type,
       ref_type,
       ref_id,
       subject,
       from_person_id,
       channel,
       message_category,
       center_id,
       ets,
       isnull(cast(stage_exerp_message.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_message
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_message @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_message (
       bk_hash,
       message_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_message.bk_hash,
       stage_hash_exerp_message.id message_id,
       isnull(cast(stage_hash_exerp_message.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_message
  left join h_exerp_message
    on stage_hash_exerp_message.bk_hash = h_exerp_message.bk_hash
 where h_exerp_message_id is null
   and stage_hash_exerp_message.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_message
if object_id('tempdb..#l_exerp_message_inserts') is not null drop table #l_exerp_message_inserts
create table #l_exerp_message_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_message.bk_hash,
       stage_hash_exerp_message.id message_id,
       stage_hash_exerp_message.person_id person_id,
       stage_hash_exerp_message.company_id company_id,
       stage_hash_exerp_message.delivered_by_person_id delivered_by_person_id,
       stage_hash_exerp_message.template_id template_id,
       stage_hash_exerp_message.ref_id ref_id,
       stage_hash_exerp_message.from_person_id from_person_id,
       stage_hash_exerp_message.center_id center_id,
       isnull(cast(stage_hash_exerp_message.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_message.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.delivered_by_person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_message.template_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.ref_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.from_person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_message.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_message
 where stage_hash_exerp_message.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_message records
set @insert_date_time = getdate()
insert into l_exerp_message (
       bk_hash,
       message_id,
       person_id,
       company_id,
       delivered_by_person_id,
       template_id,
       ref_id,
       from_person_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_message_inserts.bk_hash,
       #l_exerp_message_inserts.message_id,
       #l_exerp_message_inserts.person_id,
       #l_exerp_message_inserts.company_id,
       #l_exerp_message_inserts.delivered_by_person_id,
       #l_exerp_message_inserts.template_id,
       #l_exerp_message_inserts.ref_id,
       #l_exerp_message_inserts.from_person_id,
       #l_exerp_message_inserts.center_id,
       case when l_exerp_message.l_exerp_message_id is null then isnull(#l_exerp_message_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_message_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_message_inserts
  left join p_exerp_message
    on #l_exerp_message_inserts.bk_hash = p_exerp_message.bk_hash
   and p_exerp_message.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_message
    on p_exerp_message.bk_hash = l_exerp_message.bk_hash
   and p_exerp_message.l_exerp_message_id = l_exerp_message.l_exerp_message_id
 where l_exerp_message.l_exerp_message_id is null
    or (l_exerp_message.l_exerp_message_id is not null
        and l_exerp_message.dv_hash <> #l_exerp_message_inserts.source_hash)

--calculate hash and lookup to current s_exerp_message
if object_id('tempdb..#s_exerp_message_inserts') is not null drop table #s_exerp_message_inserts
create table #s_exerp_message_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_message.bk_hash,
       stage_hash_exerp_message.id message_id,
       stage_hash_exerp_message.creation_datetime creation_datetime,
       stage_hash_exerp_message.delivery_datetime delivery_datetime,
       stage_hash_exerp_message.delivery_method delivery_method,
       stage_hash_exerp_message.type type,
       stage_hash_exerp_message.ref_type ref_type,
       stage_hash_exerp_message.subject subject,
       stage_hash_exerp_message.channel channel,
       stage_hash_exerp_message.message_category message_category,
       stage_hash_exerp_message.ets ets,
       isnull(cast(stage_hash_exerp_message.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_message.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_message.creation_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_message.delivery_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.delivery_method,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.ref_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.subject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.channel,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_message.message_category,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_message.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_message
 where stage_hash_exerp_message.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_message records
set @insert_date_time = getdate()
insert into s_exerp_message (
       bk_hash,
       message_id,
       creation_datetime,
       delivery_datetime,
       delivery_method,
       type,
       ref_type,
       subject,
       channel,
       message_category,
       ets,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_message_inserts.bk_hash,
       #s_exerp_message_inserts.message_id,
       #s_exerp_message_inserts.creation_datetime,
       #s_exerp_message_inserts.delivery_datetime,
       #s_exerp_message_inserts.delivery_method,
       #s_exerp_message_inserts.type,
       #s_exerp_message_inserts.ref_type,
       #s_exerp_message_inserts.subject,
       #s_exerp_message_inserts.channel,
       #s_exerp_message_inserts.message_category,
       #s_exerp_message_inserts.ets,
       case when s_exerp_message.s_exerp_message_id is null then isnull(#s_exerp_message_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_message_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_message_inserts
  left join p_exerp_message
    on #s_exerp_message_inserts.bk_hash = p_exerp_message.bk_hash
   and p_exerp_message.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_message
    on p_exerp_message.bk_hash = s_exerp_message.bk_hash
   and p_exerp_message.s_exerp_message_id = s_exerp_message.s_exerp_message_id
 where s_exerp_message.s_exerp_message_id is null
    or (s_exerp_message.s_exerp_message_id is not null
        and s_exerp_message.dv_hash <> #s_exerp_message_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_message @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_message @current_dv_batch_id

end
