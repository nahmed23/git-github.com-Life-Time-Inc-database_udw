CREATE PROC [dbo].[proc_etl_mms_reimbursement_program_identifier_format_part] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ReimbursementProgramIdentifierFormatPart

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ReimbursementProgramIdentifierFormatPart (
       bk_hash,
       ReimbursementProgramIdentifierFormatPartID,
       ReimbursementProgramIdentifierFormatID,
       FieldName,
       FieldSize,
       FieldValidationRule,
       FieldValidationErrorMessage,
       FieldSequence,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ReimbursementProgramIdentifierFormatPartID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ReimbursementProgramIdentifierFormatPartID,
       ReimbursementProgramIdentifierFormatID,
       FieldName,
       FieldSize,
       FieldValidationRule,
       FieldValidationErrorMessage,
       FieldSequence,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_ReimbursementProgramIdentifierFormatPart.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_ReimbursementProgramIdentifierFormatPart
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_reimbursement_program_identifier_format_part @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_reimbursement_program_identifier_format_part (
       bk_hash,
       reimbursement_program_identifier_format_part_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_ReimbursementProgramIdentifierFormatPart.bk_hash,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.ReimbursementProgramIdentifierFormatPartID reimbursement_program_identifier_format_part_id,
       isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ReimbursementProgramIdentifierFormatPart
  left join h_mms_reimbursement_program_identifier_format_part
    on stage_hash_mms_ReimbursementProgramIdentifierFormatPart.bk_hash = h_mms_reimbursement_program_identifier_format_part.bk_hash
 where h_mms_reimbursement_program_identifier_format_part_id is null
   and stage_hash_mms_ReimbursementProgramIdentifierFormatPart.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_reimbursement_program_identifier_format_part
if object_id('tempdb..#l_mms_reimbursement_program_identifier_format_part_inserts') is not null drop table #l_mms_reimbursement_program_identifier_format_part_inserts
create table #l_mms_reimbursement_program_identifier_format_part_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ReimbursementProgramIdentifierFormatPart.bk_hash,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.ReimbursementProgramIdentifierFormatPartID reimbursement_program_identifier_format_part_id,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.ReimbursementProgramIdentifierFormatID reimbursement_program_identifier_format_id,
       isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.ReimbursementProgramIdentifierFormatPartID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.ReimbursementProgramIdentifierFormatID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ReimbursementProgramIdentifierFormatPart
 where stage_hash_mms_ReimbursementProgramIdentifierFormatPart.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_reimbursement_program_identifier_format_part records
set @insert_date_time = getdate()
insert into l_mms_reimbursement_program_identifier_format_part (
       bk_hash,
       reimbursement_program_identifier_format_part_id,
       reimbursement_program_identifier_format_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_reimbursement_program_identifier_format_part_inserts.bk_hash,
       #l_mms_reimbursement_program_identifier_format_part_inserts.reimbursement_program_identifier_format_part_id,
       #l_mms_reimbursement_program_identifier_format_part_inserts.reimbursement_program_identifier_format_id,
       case when l_mms_reimbursement_program_identifier_format_part.l_mms_reimbursement_program_identifier_format_part_id is null then isnull(#l_mms_reimbursement_program_identifier_format_part_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_reimbursement_program_identifier_format_part_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_reimbursement_program_identifier_format_part_inserts
  left join p_mms_reimbursement_program_identifier_format_part
    on #l_mms_reimbursement_program_identifier_format_part_inserts.bk_hash = p_mms_reimbursement_program_identifier_format_part.bk_hash
   and p_mms_reimbursement_program_identifier_format_part.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_reimbursement_program_identifier_format_part
    on p_mms_reimbursement_program_identifier_format_part.bk_hash = l_mms_reimbursement_program_identifier_format_part.bk_hash
   and p_mms_reimbursement_program_identifier_format_part.l_mms_reimbursement_program_identifier_format_part_id = l_mms_reimbursement_program_identifier_format_part.l_mms_reimbursement_program_identifier_format_part_id
 where l_mms_reimbursement_program_identifier_format_part.l_mms_reimbursement_program_identifier_format_part_id is null
    or (l_mms_reimbursement_program_identifier_format_part.l_mms_reimbursement_program_identifier_format_part_id is not null
        and l_mms_reimbursement_program_identifier_format_part.dv_hash <> #l_mms_reimbursement_program_identifier_format_part_inserts.source_hash)

--calculate hash and lookup to current s_mms_reimbursement_program_identifier_format_part
if object_id('tempdb..#s_mms_reimbursement_program_identifier_format_part_inserts') is not null drop table #s_mms_reimbursement_program_identifier_format_part_inserts
create table #s_mms_reimbursement_program_identifier_format_part_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ReimbursementProgramIdentifierFormatPart.bk_hash,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.ReimbursementProgramIdentifierFormatPartID reimbursement_program_identifier_format_part_id,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.FieldName field_name,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.FieldSize field_size,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.FieldValidationRule field_validation_rule,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.FieldValidationErrorMessage field_validation_error_message,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.FieldSequence field_sequence,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.InsertedDateTime inserted_date_time,
       stage_hash_mms_ReimbursementProgramIdentifierFormatPart.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.ReimbursementProgramIdentifierFormatPartID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.FieldName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.FieldSize as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.FieldValidationRule,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.FieldValidationErrorMessage,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ReimbursementProgramIdentifierFormatPart.FieldSequence as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ReimbursementProgramIdentifierFormatPart.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ReimbursementProgramIdentifierFormatPart.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ReimbursementProgramIdentifierFormatPart
 where stage_hash_mms_ReimbursementProgramIdentifierFormatPart.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_reimbursement_program_identifier_format_part records
set @insert_date_time = getdate()
insert into s_mms_reimbursement_program_identifier_format_part (
       bk_hash,
       reimbursement_program_identifier_format_part_id,
       field_name,
       field_size,
       field_validation_rule,
       field_validation_error_message,
       field_sequence,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_reimbursement_program_identifier_format_part_inserts.bk_hash,
       #s_mms_reimbursement_program_identifier_format_part_inserts.reimbursement_program_identifier_format_part_id,
       #s_mms_reimbursement_program_identifier_format_part_inserts.field_name,
       #s_mms_reimbursement_program_identifier_format_part_inserts.field_size,
       #s_mms_reimbursement_program_identifier_format_part_inserts.field_validation_rule,
       #s_mms_reimbursement_program_identifier_format_part_inserts.field_validation_error_message,
       #s_mms_reimbursement_program_identifier_format_part_inserts.field_sequence,
       #s_mms_reimbursement_program_identifier_format_part_inserts.inserted_date_time,
       #s_mms_reimbursement_program_identifier_format_part_inserts.updated_date_time,
       case when s_mms_reimbursement_program_identifier_format_part.s_mms_reimbursement_program_identifier_format_part_id is null then isnull(#s_mms_reimbursement_program_identifier_format_part_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_reimbursement_program_identifier_format_part_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_reimbursement_program_identifier_format_part_inserts
  left join p_mms_reimbursement_program_identifier_format_part
    on #s_mms_reimbursement_program_identifier_format_part_inserts.bk_hash = p_mms_reimbursement_program_identifier_format_part.bk_hash
   and p_mms_reimbursement_program_identifier_format_part.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_reimbursement_program_identifier_format_part
    on p_mms_reimbursement_program_identifier_format_part.bk_hash = s_mms_reimbursement_program_identifier_format_part.bk_hash
   and p_mms_reimbursement_program_identifier_format_part.s_mms_reimbursement_program_identifier_format_part_id = s_mms_reimbursement_program_identifier_format_part.s_mms_reimbursement_program_identifier_format_part_id
 where s_mms_reimbursement_program_identifier_format_part.s_mms_reimbursement_program_identifier_format_part_id is null
    or (s_mms_reimbursement_program_identifier_format_part.s_mms_reimbursement_program_identifier_format_part_id is not null
        and s_mms_reimbursement_program_identifier_format_part.dv_hash <> #s_mms_reimbursement_program_identifier_format_part_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_reimbursement_program_identifier_format_part @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_reimbursement_program_identifier_format_part @current_dv_batch_id

end
