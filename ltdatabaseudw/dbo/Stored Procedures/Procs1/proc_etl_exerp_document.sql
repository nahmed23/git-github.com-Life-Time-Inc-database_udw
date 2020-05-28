CREATE PROC [dbo].[proc_etl_exerp_document] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_document

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_document (
       bk_hash,
       id,
       person_id,
       company_id,
       creation_datetime,
       type,
       subject,
       details,
       creator_person_id,
       require_signature,
       signatures_signed,
       signatures_missing,
       latest_signed_datetime,
       attached_file_name,
       center_id,
       ets,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       person_id,
       company_id,
       creation_datetime,
       type,
       subject,
       details,
       creator_person_id,
       require_signature,
       signatures_signed,
       signatures_missing,
       latest_signed_datetime,
       attached_file_name,
       center_id,
       ets,
       isnull(cast(stage_exerp_document.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_document
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_document @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_document (
       bk_hash,
       document_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_document.bk_hash,
       stage_hash_exerp_document.id document_id,
       isnull(cast(stage_hash_exerp_document.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_document
  left join h_exerp_document
    on stage_hash_exerp_document.bk_hash = h_exerp_document.bk_hash
 where h_exerp_document_id is null
   and stage_hash_exerp_document.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_document
if object_id('tempdb..#l_exerp_document_inserts') is not null drop table #l_exerp_document_inserts
create table #l_exerp_document_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_document.bk_hash,
       stage_hash_exerp_document.id document_id,
       stage_hash_exerp_document.person_id person_id,
       stage_hash_exerp_document.company_id company_id,
       stage_hash_exerp_document.creator_person_id creator_person_id,
       stage_hash_exerp_document.center_id center_id,
       isnull(cast(stage_hash_exerp_document.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_document.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_document.person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_document.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_document.creator_person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_document.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_document
 where stage_hash_exerp_document.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_document records
set @insert_date_time = getdate()
insert into l_exerp_document (
       bk_hash,
       document_id,
       person_id,
       company_id,
       creator_person_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_document_inserts.bk_hash,
       #l_exerp_document_inserts.document_id,
       #l_exerp_document_inserts.person_id,
       #l_exerp_document_inserts.company_id,
       #l_exerp_document_inserts.creator_person_id,
       #l_exerp_document_inserts.center_id,
       case when l_exerp_document.l_exerp_document_id is null then isnull(#l_exerp_document_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_document_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_document_inserts
  left join p_exerp_document
    on #l_exerp_document_inserts.bk_hash = p_exerp_document.bk_hash
   and p_exerp_document.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_document
    on p_exerp_document.bk_hash = l_exerp_document.bk_hash
   and p_exerp_document.l_exerp_document_id = l_exerp_document.l_exerp_document_id
 where l_exerp_document.l_exerp_document_id is null
    or (l_exerp_document.l_exerp_document_id is not null
        and l_exerp_document.dv_hash <> #l_exerp_document_inserts.source_hash)

--calculate hash and lookup to current s_exerp_document
if object_id('tempdb..#s_exerp_document_inserts') is not null drop table #s_exerp_document_inserts
create table #s_exerp_document_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_document.bk_hash,
       stage_hash_exerp_document.id document_id,
       stage_hash_exerp_document.creation_datetime creation_datetime,
       stage_hash_exerp_document.type type,
       stage_hash_exerp_document.subject subject,
       stage_hash_exerp_document.details details,
       stage_hash_exerp_document.require_signature require_signature,
       stage_hash_exerp_document.signatures_signed signatures_signed,
       stage_hash_exerp_document.signatures_missing signatures_missing,
       stage_hash_exerp_document.latest_signed_datetime latest_signed_datetime,
       stage_hash_exerp_document.attached_file_name attached_file_name,
       stage_hash_exerp_document.ets ets,
       isnull(cast(stage_hash_exerp_document.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_document.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_document.creation_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_document.type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_document.subject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_document.details,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_document.require_signature as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_document.signatures_signed as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_document.signatures_missing as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_document.latest_signed_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_document.attached_file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_document.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_document
 where stage_hash_exerp_document.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_document records
set @insert_date_time = getdate()
insert into s_exerp_document (
       bk_hash,
       document_id,
       creation_datetime,
       type,
       subject,
       details,
       require_signature,
       signatures_signed,
       signatures_missing,
       latest_signed_datetime,
       attached_file_name,
       ets,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_document_inserts.bk_hash,
       #s_exerp_document_inserts.document_id,
       #s_exerp_document_inserts.creation_datetime,
       #s_exerp_document_inserts.type,
       #s_exerp_document_inserts.subject,
       #s_exerp_document_inserts.details,
       #s_exerp_document_inserts.require_signature,
       #s_exerp_document_inserts.signatures_signed,
       #s_exerp_document_inserts.signatures_missing,
       #s_exerp_document_inserts.latest_signed_datetime,
       #s_exerp_document_inserts.attached_file_name,
       #s_exerp_document_inserts.ets,
       case when s_exerp_document.s_exerp_document_id is null then isnull(#s_exerp_document_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_document_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_document_inserts
  left join p_exerp_document
    on #s_exerp_document_inserts.bk_hash = p_exerp_document.bk_hash
   and p_exerp_document.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_document
    on p_exerp_document.bk_hash = s_exerp_document.bk_hash
   and p_exerp_document.s_exerp_document_id = s_exerp_document.s_exerp_document_id
 where s_exerp_document.s_exerp_document_id is null
    or (s_exerp_document.s_exerp_document_id is not null
        and s_exerp_document.dv_hash <> #s_exerp_document_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_document @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_document @current_dv_batch_id

end
