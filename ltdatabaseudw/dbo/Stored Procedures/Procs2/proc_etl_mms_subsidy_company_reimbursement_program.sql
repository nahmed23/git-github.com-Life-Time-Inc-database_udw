﻿CREATE PROC [dbo].[proc_etl_mms_subsidy_company_reimbursement_program] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_SubsidyCompanyReimbursementProgram

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_SubsidyCompanyReimbursementProgram (
       bk_hash,
       SubsidyCompanyReimbursementProgramID,
       SubsidyCompanyID,
       ReimbursementProgramID,
       Description,
       SendQualificationDataFlag,
       LTFCalcFlag,
       BatchNumber,
       MaximumReimbursement,
       EffectiveFromDateTime,
       EffectiveThruDateTime,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(SubsidyCompanyReimbursementProgramID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       SubsidyCompanyReimbursementProgramID,
       SubsidyCompanyID,
       ReimbursementProgramID,
       Description,
       SendQualificationDataFlag,
       LTFCalcFlag,
       BatchNumber,
       MaximumReimbursement,
       EffectiveFromDateTime,
       EffectiveThruDateTime,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_SubsidyCompanyReimbursementProgram.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_SubsidyCompanyReimbursementProgram
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_subsidy_company_reimbursement_program @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_subsidy_company_reimbursement_program (
       bk_hash,
       subsidy_company_reimbursement_program_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_SubsidyCompanyReimbursementProgram.bk_hash,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.SubsidyCompanyReimbursementProgramID subsidy_company_reimbursement_program_id,
       isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_SubsidyCompanyReimbursementProgram
  left join h_mms_subsidy_company_reimbursement_program
    on stage_hash_mms_SubsidyCompanyReimbursementProgram.bk_hash = h_mms_subsidy_company_reimbursement_program.bk_hash
 where h_mms_subsidy_company_reimbursement_program_id is null
   and stage_hash_mms_SubsidyCompanyReimbursementProgram.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_subsidy_company_reimbursement_program
if object_id('tempdb..#l_mms_subsidy_company_reimbursement_program_inserts') is not null drop table #l_mms_subsidy_company_reimbursement_program_inserts
create table #l_mms_subsidy_company_reimbursement_program_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_SubsidyCompanyReimbursementProgram.bk_hash,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.SubsidyCompanyReimbursementProgramID subsidy_company_reimbursement_program_id,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.SubsidyCompanyID subsidy_company_id,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.ReimbursementProgramID reimbursement_program_id,
       isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.SubsidyCompanyReimbursementProgramID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.SubsidyCompanyID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.ReimbursementProgramID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_SubsidyCompanyReimbursementProgram
 where stage_hash_mms_SubsidyCompanyReimbursementProgram.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_subsidy_company_reimbursement_program records
set @insert_date_time = getdate()
insert into l_mms_subsidy_company_reimbursement_program (
       bk_hash,
       subsidy_company_reimbursement_program_id,
       subsidy_company_id,
       reimbursement_program_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_subsidy_company_reimbursement_program_inserts.bk_hash,
       #l_mms_subsidy_company_reimbursement_program_inserts.subsidy_company_reimbursement_program_id,
       #l_mms_subsidy_company_reimbursement_program_inserts.subsidy_company_id,
       #l_mms_subsidy_company_reimbursement_program_inserts.reimbursement_program_id,
       case when l_mms_subsidy_company_reimbursement_program.l_mms_subsidy_company_reimbursement_program_id is null then isnull(#l_mms_subsidy_company_reimbursement_program_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_subsidy_company_reimbursement_program_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_subsidy_company_reimbursement_program_inserts
  left join p_mms_subsidy_company_reimbursement_program
    on #l_mms_subsidy_company_reimbursement_program_inserts.bk_hash = p_mms_subsidy_company_reimbursement_program.bk_hash
   and p_mms_subsidy_company_reimbursement_program.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_subsidy_company_reimbursement_program
    on p_mms_subsidy_company_reimbursement_program.bk_hash = l_mms_subsidy_company_reimbursement_program.bk_hash
   and p_mms_subsidy_company_reimbursement_program.l_mms_subsidy_company_reimbursement_program_id = l_mms_subsidy_company_reimbursement_program.l_mms_subsidy_company_reimbursement_program_id
 where l_mms_subsidy_company_reimbursement_program.l_mms_subsidy_company_reimbursement_program_id is null
    or (l_mms_subsidy_company_reimbursement_program.l_mms_subsidy_company_reimbursement_program_id is not null
        and l_mms_subsidy_company_reimbursement_program.dv_hash <> #l_mms_subsidy_company_reimbursement_program_inserts.source_hash)

--calculate hash and lookup to current s_mms_subsidy_company_reimbursement_program
if object_id('tempdb..#s_mms_subsidy_company_reimbursement_program_inserts') is not null drop table #s_mms_subsidy_company_reimbursement_program_inserts
create table #s_mms_subsidy_company_reimbursement_program_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_SubsidyCompanyReimbursementProgram.bk_hash,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.SubsidyCompanyReimbursementProgramID subsidy_company_reimbursement_program_id,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.Description description,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.SendQualificationDataFlag send_qualification_data_flag,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.LTFCalcFlag ltf_calc_flag,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.BatchNumber batch_number,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.MaximumReimbursement maximum_reimbursement,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.EffectiveFromDateTime effective_from_date_time,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.EffectiveThruDateTime effective_thru_date_time,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.InsertedDateTime inserted_date_time,
       stage_hash_mms_SubsidyCompanyReimbursementProgram.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.SubsidyCompanyReimbursementProgramID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_SubsidyCompanyReimbursementProgram.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.SendQualificationDataFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.LTFCalcFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.BatchNumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_SubsidyCompanyReimbursementProgram.MaximumReimbursement as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SubsidyCompanyReimbursementProgram.EffectiveFromDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SubsidyCompanyReimbursementProgram.EffectiveThruDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SubsidyCompanyReimbursementProgram.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SubsidyCompanyReimbursementProgram.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_SubsidyCompanyReimbursementProgram
 where stage_hash_mms_SubsidyCompanyReimbursementProgram.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_subsidy_company_reimbursement_program records
set @insert_date_time = getdate()
insert into s_mms_subsidy_company_reimbursement_program (
       bk_hash,
       subsidy_company_reimbursement_program_id,
       description,
       send_qualification_data_flag,
       ltf_calc_flag,
       batch_number,
       maximum_reimbursement,
       effective_from_date_time,
       effective_thru_date_time,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_subsidy_company_reimbursement_program_inserts.bk_hash,
       #s_mms_subsidy_company_reimbursement_program_inserts.subsidy_company_reimbursement_program_id,
       #s_mms_subsidy_company_reimbursement_program_inserts.description,
       #s_mms_subsidy_company_reimbursement_program_inserts.send_qualification_data_flag,
       #s_mms_subsidy_company_reimbursement_program_inserts.ltf_calc_flag,
       #s_mms_subsidy_company_reimbursement_program_inserts.batch_number,
       #s_mms_subsidy_company_reimbursement_program_inserts.maximum_reimbursement,
       #s_mms_subsidy_company_reimbursement_program_inserts.effective_from_date_time,
       #s_mms_subsidy_company_reimbursement_program_inserts.effective_thru_date_time,
       #s_mms_subsidy_company_reimbursement_program_inserts.inserted_date_time,
       #s_mms_subsidy_company_reimbursement_program_inserts.updated_date_time,
       case when s_mms_subsidy_company_reimbursement_program.s_mms_subsidy_company_reimbursement_program_id is null then isnull(#s_mms_subsidy_company_reimbursement_program_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_subsidy_company_reimbursement_program_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_subsidy_company_reimbursement_program_inserts
  left join p_mms_subsidy_company_reimbursement_program
    on #s_mms_subsidy_company_reimbursement_program_inserts.bk_hash = p_mms_subsidy_company_reimbursement_program.bk_hash
   and p_mms_subsidy_company_reimbursement_program.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_subsidy_company_reimbursement_program
    on p_mms_subsidy_company_reimbursement_program.bk_hash = s_mms_subsidy_company_reimbursement_program.bk_hash
   and p_mms_subsidy_company_reimbursement_program.s_mms_subsidy_company_reimbursement_program_id = s_mms_subsidy_company_reimbursement_program.s_mms_subsidy_company_reimbursement_program_id
 where s_mms_subsidy_company_reimbursement_program.s_mms_subsidy_company_reimbursement_program_id is null
    or (s_mms_subsidy_company_reimbursement_program.s_mms_subsidy_company_reimbursement_program_id is not null
        and s_mms_subsidy_company_reimbursement_program.dv_hash <> #s_mms_subsidy_company_reimbursement_program_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_subsidy_company_reimbursement_program @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_subsidy_company_reimbursement_program @current_dv_batch_id

end
