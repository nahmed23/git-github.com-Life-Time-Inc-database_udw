CREATE PROC [dbo].[proc_etl_mms_reimbursement_program_identifier_format] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ReimbursementProgramIdentifierFormat

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ReimbursementProgramIdentifierFormat (
       bk_hash,
       ReimbursementProgramIdentifierFormatID,
       ReimbursementProgramID,
       Description,
       ActiveFlag,
       InsertedDateTime,
       UpdatedDateTime,
       ImageURL,
       ImageDescription,
       SortOrder,
       ValProgramIdentifierValidationClassID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ReimbursementProgramIdentifierFormatID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ReimbursementProgramIdentifierFormatID,
       ReimbursementProgramID,
       Description,
       ActiveFlag,
       InsertedDateTime,
       UpdatedDateTime,
       ImageURL,
       ImageDescription,
       SortOrder,
       ValProgramIdentifierValidationClassID,
       isnull(cast(stage_mms_ReimbursementProgramIdentifierFormat.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_ReimbursementProgramIdentifierFormat
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_reimbursement_program_identifier_format @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_reimbursement_program_identifier_format (
       bk_hash,
       reimbursement_program_identifier_format_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_ReimbursementProgramIdentifierFormat.bk_hash,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.ReimbursementProgramIdentifierFormatID reimbursement_program_identifier_format_id,
       isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormat.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ReimbursementProgramIdentifierFormat
  left join h_mms_reimbursement_program_identifier_format
    on stage_hash_mms_ReimbursementProgramIdentifierFormat.bk_hash = h_mms_reimbursement_program_identifier_format.bk_hash
 where h_mms_reimbursement_program_identifier_format_id is null
   and stage_hash_mms_ReimbursementProgramIdentifierFormat.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_reimbursement_program_identifier_format
if object_id('tempdb..#l_mms_reimbursement_program_identifier_format_inserts') is not null drop table #l_mms_reimbursement_program_identifier_format_inserts
create table #l_mms_reimbursement_program_identifier_format_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ReimbursementProgramIdentifierFormat.bk_hash,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.ReimbursementProgramIdentifierFormatID reimbursement_program_identifier_format_id,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.ValProgramIdentifierValidationClassID val_program_identifier_validation_class_id,
       isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormat.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormat.ReimbursementProgramIdentifierFormatID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormat.ValProgramIdentifierValidationClassID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ReimbursementProgramIdentifierFormat
 where stage_hash_mms_ReimbursementProgramIdentifierFormat.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_reimbursement_program_identifier_format records
set @insert_date_time = getdate()
insert into l_mms_reimbursement_program_identifier_format (
       bk_hash,
       reimbursement_program_identifier_format_id,
       val_program_identifier_validation_class_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_reimbursement_program_identifier_format_inserts.bk_hash,
       #l_mms_reimbursement_program_identifier_format_inserts.reimbursement_program_identifier_format_id,
       #l_mms_reimbursement_program_identifier_format_inserts.val_program_identifier_validation_class_id,
       case when l_mms_reimbursement_program_identifier_format.l_mms_reimbursement_program_identifier_format_id is null then isnull(#l_mms_reimbursement_program_identifier_format_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_reimbursement_program_identifier_format_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_reimbursement_program_identifier_format_inserts
  left join p_mms_reimbursement_program_identifier_format
    on #l_mms_reimbursement_program_identifier_format_inserts.bk_hash = p_mms_reimbursement_program_identifier_format.bk_hash
   and p_mms_reimbursement_program_identifier_format.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_reimbursement_program_identifier_format
    on p_mms_reimbursement_program_identifier_format.bk_hash = l_mms_reimbursement_program_identifier_format.bk_hash
   and p_mms_reimbursement_program_identifier_format.l_mms_reimbursement_program_identifier_format_id = l_mms_reimbursement_program_identifier_format.l_mms_reimbursement_program_identifier_format_id
 where l_mms_reimbursement_program_identifier_format.l_mms_reimbursement_program_identifier_format_id is null
    or (l_mms_reimbursement_program_identifier_format.l_mms_reimbursement_program_identifier_format_id is not null
        and l_mms_reimbursement_program_identifier_format.dv_hash <> #l_mms_reimbursement_program_identifier_format_inserts.source_hash)

--calculate hash and lookup to current s_mms_reimbursement_program_identifier_format
if object_id('tempdb..#s_mms_reimbursement_program_identifier_format_inserts') is not null drop table #s_mms_reimbursement_program_identifier_format_inserts
create table #s_mms_reimbursement_program_identifier_format_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ReimbursementProgramIdentifierFormat.bk_hash,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.ReimbursementProgramIdentifierFormatID reimbursement_program_identifier_format_id,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.ReimbursementProgramID reimbursement_program_id,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.Description description,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.ActiveFlag active_flag,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.InsertedDateTime inserted_date_time,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.UpdatedDateTime updated_date_time,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.ImageURL image_url,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.ImageDescription image_description,
       stage_hash_mms_ReimbursementProgramIdentifierFormat.SortOrder sort_order,
       isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormat.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormat.ReimbursementProgramIdentifierFormatID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormat.ReimbursementProgramID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ReimbursementProgramIdentifierFormat.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormat.ActiveFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ReimbursementProgramIdentifierFormat.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ReimbursementProgramIdentifierFormat.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ReimbursementProgramIdentifierFormat.ImageURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ReimbursementProgramIdentifierFormat.ImageDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormat.SortOrder as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ReimbursementProgramIdentifierFormat
 where stage_hash_mms_ReimbursementProgramIdentifierFormat.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_reimbursement_program_identifier_format records
set @insert_date_time = getdate()
insert into s_mms_reimbursement_program_identifier_format (
       bk_hash,
       reimbursement_program_identifier_format_id,
       reimbursement_program_id,
       description,
       active_flag,
       inserted_date_time,
       updated_date_time,
       image_url,
       image_description,
       sort_order,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_reimbursement_program_identifier_format_inserts.bk_hash,
       #s_mms_reimbursement_program_identifier_format_inserts.reimbursement_program_identifier_format_id,
       #s_mms_reimbursement_program_identifier_format_inserts.reimbursement_program_id,
       #s_mms_reimbursement_program_identifier_format_inserts.description,
       #s_mms_reimbursement_program_identifier_format_inserts.active_flag,
       #s_mms_reimbursement_program_identifier_format_inserts.inserted_date_time,
       #s_mms_reimbursement_program_identifier_format_inserts.updated_date_time,
       #s_mms_reimbursement_program_identifier_format_inserts.image_url,
       #s_mms_reimbursement_program_identifier_format_inserts.image_description,
       #s_mms_reimbursement_program_identifier_format_inserts.sort_order,
       case when s_mms_reimbursement_program_identifier_format.s_mms_reimbursement_program_identifier_format_id is null then isnull(#s_mms_reimbursement_program_identifier_format_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_reimbursement_program_identifier_format_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_reimbursement_program_identifier_format_inserts
  left join p_mms_reimbursement_program_identifier_format
    on #s_mms_reimbursement_program_identifier_format_inserts.bk_hash = p_mms_reimbursement_program_identifier_format.bk_hash
   and p_mms_reimbursement_program_identifier_format.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_reimbursement_program_identifier_format
    on p_mms_reimbursement_program_identifier_format.bk_hash = s_mms_reimbursement_program_identifier_format.bk_hash
   and p_mms_reimbursement_program_identifier_format.s_mms_reimbursement_program_identifier_format_id = s_mms_reimbursement_program_identifier_format.s_mms_reimbursement_program_identifier_format_id
 where s_mms_reimbursement_program_identifier_format.s_mms_reimbursement_program_identifier_format_id is null
    or (s_mms_reimbursement_program_identifier_format.s_mms_reimbursement_program_identifier_format_id is not null
        and s_mms_reimbursement_program_identifier_format.dv_hash <> #s_mms_reimbursement_program_identifier_format_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_reimbursement_program_identifier_format @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_reimbursement_program_identifier_format @current_dv_batch_id

end
