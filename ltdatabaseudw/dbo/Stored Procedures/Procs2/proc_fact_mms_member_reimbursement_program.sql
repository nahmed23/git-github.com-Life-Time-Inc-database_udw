CREATE PROC [dbo].[proc_fact_mms_member_reimbursement_program] @dv_batch_id [varchar](500) AS
Begin
set xact_abort on
set nocount on





if object_id('tempdb..#identifiers') is not null drop table #identifiers
create table dbo.#identifiers with(distribution=hash(fact_mms_member_reimbursement_key), location=user_db, heap) as
select id_part.member_reimbursement_id,
       id_part.fact_mms_member_reimbursement_key,
       id_format.reimbursement_program_id,
       id_format.dim_mms_reimbursement_program_key,
       max(case when id_format_part.field_sequence = 1 then id_part.part_value else null end) identifier_field1_value,
       max(case when id_format_part.field_sequence = 1 then id_format_part.field_name_dim_description_key else null end) identifier_field1_name_dim_description_key,
       max(case when id_format_part.field_sequence = 2 then id_part.part_value else null end) identifier_field2_value,
       max(case when id_format_part.field_sequence = 2 then id_format_part.field_name_dim_description_key else null end) identifier_field2_name_dim_description_key,
       max(case when id_format_part.field_sequence = 3 then id_part.part_value else null end) identifier_field3_value,
       max(case when id_format_part.field_sequence = 3 then id_format_part.field_name_dim_description_key else null end) identifier_field3_name_dim_description_key
from d_mms_member_reimbursement_program_identifier_part id_part
join d_mms_reimbursement_program_identifier_format_part id_format_part
  on id_part.reimbursement_program_identifier_format_part_bk_hash = id_format_part.reimbursement_program_identifier_format_part_bk_hash
join d_mms_reimbursement_program_identifier_format id_format
  on id_format_part.reimbursement_program_identifier_format_bk_hash = id_format.reimbursement_program_identifier_format_bk_hash
group by id_part.member_reimbursement_id,
       id_part.fact_mms_member_reimbursement_key,
       id_format.reimbursement_program_id,
       id_format.dim_mms_reimbursement_program_key

if object_id('tempdb..#fact_mms_member_reimbursement') is not null drop table #fact_mms_member_reimbursement
create table dbo.#fact_mms_member_reimbursement with(distribution=hash(fact_mms_member_reimbursement_key)) as
select d_mms_member_reimbursement.fact_mms_member_reimbursement_key,
       d_mms_member_reimbursement.member_reimbursement_id,
       d_mms_member_reimbursement.dim_mms_member_key,
       d_mms_member_reimbursement.dim_mms_reimbursement_program_key,
       d_mms_member_reimbursement.enrollment_dim_date_key,
       d_mms_member_reimbursement.enrollment_date,
       d_mms_member_reimbursement.termination_dim_date_key,
       d_mms_member_reimbursement.termination_date,
       d_mms_member.dim_mms_membership_key,
       dim_mms_reimbursement_program.dim_mms_company_key, -----------IN LTFDW being fetched from vDimReimbursementProgramActive lookup basxed on: ReimbursementProgramID 
       isnull(#identifiers.identifier_field1_name_dim_description_key,'-998') identifier_field1_name_dim_description_key,
       isnull(#identifiers.identifier_field2_name_dim_description_key,'-998') identifier_field2_name_dim_description_key,
       isnull(#identifiers.identifier_field3_name_dim_description_key,'-998') identifier_field3_name_dim_description_key,
       isnull(#identifiers.identifier_field1_value,'') identifier_field1_value,
       isnull(#identifiers.identifier_field2_value,'') identifier_field2_value,
       isnull(#identifiers.identifier_field3_value,'') identifier_field3_value,
       d_mms_member_reimbursement.p_mms_member_reimbursement_id,
       d_mms_member_reimbursement.dv_load_date_time,
       d_mms_member_reimbursement.dv_load_end_date_time,
       d_mms_member_reimbursement.dv_batch_id
     from d_mms_member_reimbursement
     left join #identifiers
       on d_mms_member_reimbursement.bk_hash = #identifiers.fact_mms_member_reimbursement_key
      and d_mms_member_reimbursement.dim_mms_reimbursement_program_key = #identifiers.dim_mms_reimbursement_program_key
     join d_mms_member
       on d_mms_member_reimbursement.dim_mms_member_key = d_mms_member.dim_mms_member_key
     join dim_mms_reimbursement_program
       on d_mms_member_reimbursement.dim_mms_reimbursement_program_key = dim_mms_reimbursement_program.dim_mms_reimbursement_program_key



--~~~~~~~~~~~~~~~~~~END OF STEP 2: Requried Fields from different participating fields have been created as #TEMP tables~~~~~~~~~~~~~~~~~~~~


--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 3:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    -----------STEP 3: INSERT INTO DIM TABLE: By Joining the temp STEP 2's #temp tables, forming the main Dim table record-----------
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- Delete and re-insert
-- do as a single transaction
-- delete records from the fact table that exist
-- insert records from records from current and missing batches

    truncate table dbo.fact_mms_member_reimbursement_program

 
    insert into fact_mms_member_reimbursement_program
    (fact_mms_member_reimbursement_program_key,
     member_reimbursement_id,
     dim_mms_member_key,
     dim_mms_reimbursement_program_key,
     enrollment_dim_date_key,
     enrollment_date,
     termination_dim_date_key,
     termination_date,
     dim_mms_membership_key, 
     dim_mms_company_key,
     identifier_field1_name_dim_description_key,
     identifier_field2_name_dim_description_key,
     identifier_field3_name_dim_description_key,
     identifier_field1_value,
     identifier_field2_value,
     identifier_field3_value,
     p_mms_member_reimbursement_id,
      dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user)
 
 Select fact_mms_member_reimbursement_key,
        member_reimbursement_id,
        dim_mms_member_key,
        dim_mms_reimbursement_program_key,
        enrollment_dim_date_key,
        enrollment_date,
        case when fact_mms_member_reimbursement_key in ('-999','-998','-997') then null else isnull(termination_dim_date_key,99991231) end,
        case when fact_mms_member_reimbursement_key in ('-999','-998','-997') then null else isnull(termination_date,'9999-12-31 00:00:00.000') end,
        dim_mms_membership_key,
        dim_mms_company_key, -----------IN LTFDW being fetched from vDimReimbursementProgramActive lookup basxed on: ReimbursementProgramID 
        identifier_field1_name_dim_description_key,
        identifier_field2_name_dim_description_key,
        identifier_field3_name_dim_description_key,
        identifier_field1_value,
        identifier_field2_value,
        identifier_field3_value,
        p_mms_member_reimbursement_id,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id,
        getdate(),
        suser_sname()     
   from #fact_mms_member_reimbursement


end
