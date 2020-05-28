CREATE PROC [dbo].[proc_d_qtest_jira] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_qtest_jira)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_qtest_jira_insert') is not null drop table #p_qtest_jira_insert
create table dbo.#p_qtest_jira_insert with(distribution=hash(bk_hash), location=user_db) as
select p_qtest_jira.p_qtest_jira_id,
       p_qtest_jira.bk_hash
  from dbo.p_qtest_jira
 where p_qtest_jira.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_qtest_jira.dv_batch_id > @max_dv_batch_id
        or p_qtest_jira.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_qtest_jira.bk_hash,
       p_qtest_jira.bk_hash dim_qtest_jira_key,
       p_qtest_jira.test_case_id test_case_id,
       p_qtest_jira.project_id project_id,
       p_qtest_jira.release_id release_id,
       p_qtest_jira.requirement_id requirement_id,
       s_qtest_jira.project_automation project_automation,
       s_qtest_jira.project_date_format project_date_format,
       s_qtest_jira.project_description project_description,
       s_qtest_jira.project_name project_name,
       s_qtest_jira.project_sample project_sample,
       s_qtest_jira.project_start_date project_start_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.project_start_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.project_start_date,1,10) as date),112)    end project_start_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.project_start_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.project_start_date, 12, 6),':',''),114) end project_start_dim_time_key,
       l_qtest_jira.project_status_id project_status_id,
       l_qtest_jira.project_template_id project_template_id,
       s_qtest_jira.project_uuid project_uuid,
       s_qtest_jira.project_x_explorer_access_level project_x_explorer_access_level,
       s_qtest_jira.release_created_date release_created_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.release_created_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.release_created_date,1,10) as date),112)    end release_created_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.release_created_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.release_created_date, 12, 6),':',''),114) end release_created_dim_time_key,
       s_qtest_jira.release_end_date release_end_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.release_end_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.release_end_date,1,10) as date),112)    end release_end_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.release_end_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.release_end_date, 12, 6),':',''),114) end release_end_dim_time_key,
       s_qtest_jira.release_last_modified_date release_last_modified_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.release_last_modified_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.release_last_modified_date,1,10) as date),112)    end release_last_modified_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.release_last_modified_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.release_last_modified_date, 12, 6),':',''),114) end release_last_modified_dim_time_key,
       s_qtest_jira.release_name release_name,
       s_qtest_jira.release_order release_order,
       l_qtest_jira.release_pid release_pid,
       l_qtest_jira.release_project_id release_project_id,
       s_qtest_jira.release_properties_end_date release_properties_end_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.release_properties_end_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.release_properties_end_date,1,10) as date),112)    end release_properties_end_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.release_properties_end_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.release_properties_end_date, 12, 6),':',''),114) end release_properties_end_dim_time_key,
       s_qtest_jira.release_properties_start_date release_properties_start_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.release_properties_start_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.release_properties_start_date,1,10) as date),112)    end release_properties_start_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.release_properties_start_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.release_properties_start_date, 12, 6),':',''),114) end release_properties_start_dim_time_key,
       s_qtest_jira.release_properties_status release_properties_status,
       s_qtest_jira.release_start_date release_start_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.release_start_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.release_start_date,1,10) as date),112)    end release_start_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.release_start_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.release_start_date, 12, 6),':',''),114) end release_start_dim_time_key,
       s_qtest_jira.requirement_created_date requirement_created_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.requirement_created_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.requirement_created_date,1,10) as date),112)    end requirement_created_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.requirement_created_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.requirement_created_date, 12, 6),':',''),114) end requirement_created_dim_time_key,
       l_qtest_jira.requirement_jira_defect_id requirement_jira_defect_id,
       s_qtest_jira.requirement_jira_issue requirement_jira_issue,
       s_qtest_jira.requirement_jira_priority requirement_jira_priority,
       s_qtest_jira.requirement_jira_status requirement_jira_status,
       s_qtest_jira.requirement_jira_story_point requirement_jira_story_point,
       s_qtest_jira.requirement_last_modified_date requirement_last_modified_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.requirement_last_modified_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.requirement_last_modified_date,1,10) as date),112)    end requirement_last_modified_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.requirement_last_modified_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.requirement_last_modified_date, 12, 6),':',''),114) end requirement_last_modified_dim_time_key,
       s_qtest_jira.requirement_name requirement_name,
       l_qtest_jira.requirement_parent_id requirement_parent_id,
       l_qtest_jira.requirement_pid requirement_pid,
       l_qtest_jira.requirement_project_id requirement_project_id,
       s_qtest_jira.test_case_created_date test_case_created_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.test_case_created_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.test_case_created_date,1,10) as date),112)    end test_case_created_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.test_case_created_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.test_case_created_date, 12, 6),':',''),114) end test_case_created_dim_time_key,
       l_qtest_jira.test_case_creator_id test_case_creator_id,
       s_qtest_jira.test_case_description test_case_description,
       s_qtest_jira.test_case_last_modified_date test_case_last_modified_date,
       case when p_qtest_jira.bk_hash in('-997', '-998', '-999') then p_qtest_jira.bk_hash
           when s_qtest_jira.test_case_last_modified_date is null then '-998'
        else convert(varchar,cast(substring(s_qtest_jira.test_case_last_modified_date,1,10) as date),112)    end test_case_last_modified_dim_date_key,
       case when p_qtest_jira.bk_hash in ('-997','-998','-999') then p_qtest_jira.bk_hash
       when s_qtest_jira.test_case_last_modified_date is null then '-998'
       else '1' + convert(varchar,replace(substring(s_qtest_jira.test_case_last_modified_date, 12, 6),':',''),114) end test_case_last_modified_dim_time_key,
       s_qtest_jira.test_case_name test_case_name,
       s_qtest_jira.test_case_order test_case_order,
       l_qtest_jira.test_case_parent_id test_case_parent_id,
       l_qtest_jira.test_case_pid test_case_pid,
       l_qtest_jira.test_case_project_id test_case_project_id,
       s_qtest_jira.test_case_properties_assigned_to_field_value_name test_case_properties_assigned_to_field_value_name,
       s_qtest_jira.test_case_properties_automation_content_field_value test_case_properties_automation_content_field_value,
       s_qtest_jira.test_case_properties_automation_field_value_name test_case_properties_automation_field_value_name,
       s_qtest_jira.test_case_properties_candidate_for_automation_field_value_name test_case_properties_candidate_for_automation_field_value_name,
       s_qtest_jira.test_case_properties_description_field_value test_case_properties_description_field_value,
       s_qtest_jira.test_case_properties_platform_field_value_name test_case_properties_platform_field_value_name,
       s_qtest_jira.test_case_properties_precondition_field_value test_case_properties_precondition_field_value,
       s_qtest_jira.test_case_properties_shared_field_value test_case_properties_shared_field_value,
       s_qtest_jira.test_case_properties_status_field_value_name test_case_properties_status_field_value_name,
       s_qtest_jira.test_case_properties_test_data_field_value test_case_properties_test_data_field_value,
       s_qtest_jira.test_case_properties_test_tier_field_value_name test_case_properties_test_tier_field_value_name,
       s_qtest_jira.test_case_properties_type_field_value_name test_case_properties_type_field_value_name,
       s_qtest_jira.test_case_properties_zephyr_issue_key_field_value test_case_properties_zephyr_issue_key_field_value,
       s_qtest_jira.test_case_properties_zephyr_test_steps_field_value test_case_properties_zephyr_test_steps_field_value,
       l_qtest_jira.test_case_test_case_version_id test_case_test_case_version_id,
       s_qtest_jira.test_case_version test_case_version,
       isnull(h_qtest_jira.dv_deleted,0) dv_deleted,
       p_qtest_jira.p_qtest_jira_id,
       p_qtest_jira.dv_batch_id,
       p_qtest_jira.dv_load_date_time,
       p_qtest_jira.dv_load_end_date_time
  from dbo.h_qtest_jira
  join dbo.p_qtest_jira
    on h_qtest_jira.bk_hash = p_qtest_jira.bk_hash
  join #p_qtest_jira_insert
    on p_qtest_jira.bk_hash = #p_qtest_jira_insert.bk_hash
   and p_qtest_jira.p_qtest_jira_id = #p_qtest_jira_insert.p_qtest_jira_id
  join dbo.l_qtest_jira
    on p_qtest_jira.bk_hash = l_qtest_jira.bk_hash
   and p_qtest_jira.l_qtest_jira_id = l_qtest_jira.l_qtest_jira_id
  join dbo.s_qtest_jira
    on p_qtest_jira.bk_hash = s_qtest_jira.bk_hash
   and p_qtest_jira.s_qtest_jira_id = s_qtest_jira.s_qtest_jira_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_qtest_jira
   where d_qtest_jira.bk_hash in (select bk_hash from #p_qtest_jira_insert)

  insert dbo.d_qtest_jira(
             bk_hash,
             dim_qtest_jira_key,
             test_case_id,
             project_id,
             release_id,
             requirement_id,
             project_automation,
             project_date_format,
             project_description,
             project_name,
             project_sample,
             project_start_date,
             project_start_dim_date_key,
             project_start_dim_time_key,
             project_status_id,
             project_template_id,
             project_uuid,
             project_x_explorer_access_level,
             release_created_date,
             release_created_dim_date_key,
             release_created_dim_time_key,
             release_end_date,
             release_end_dim_date_key,
             release_end_dim_time_key,
             release_last_modified_date,
             release_last_modified_dim_date_key,
             release_last_modified_dim_time_key,
             release_name,
             release_order,
             release_pid,
             release_project_id,
             release_properties_end_date,
             release_properties_end_dim_date_key,
             release_properties_end_dim_time_key,
             release_properties_start_date,
             release_properties_start_dim_date_key,
             release_properties_start_dim_time_key,
             release_properties_status,
             release_start_date,
             release_start_dim_date_key,
             release_start_dim_time_key,
             requirement_created_date,
             requirement_created_dim_date_key,
             requirement_created_dim_time_key,
             requirement_jira_defect_id,
             requirement_jira_issue,
             requirement_jira_priority,
             requirement_jira_status,
             requirement_jira_story_point,
             requirement_last_modified_date,
             requirement_last_modified_dim_date_key,
             requirement_last_modified_dim_time_key,
             requirement_name,
             requirement_parent_id,
             requirement_pid,
             requirement_project_id,
             test_case_created_date,
             test_case_created_dim_date_key,
             test_case_created_dim_time_key,
             test_case_creator_id,
             test_case_description,
             test_case_last_modified_date,
             test_case_last_modified_dim_date_key,
             test_case_last_modified_dim_time_key,
             test_case_name,
             test_case_order,
             test_case_parent_id,
             test_case_pid,
             test_case_project_id,
             test_case_properties_assigned_to_field_value_name,
             test_case_properties_automation_content_field_value,
             test_case_properties_automation_field_value_name,
             test_case_properties_candidate_for_automation_field_value_name,
             test_case_properties_description_field_value,
             test_case_properties_platform_field_value_name,
             test_case_properties_precondition_field_value,
             test_case_properties_shared_field_value,
             test_case_properties_status_field_value_name,
             test_case_properties_test_data_field_value,
             test_case_properties_test_tier_field_value_name,
             test_case_properties_type_field_value_name,
             test_case_properties_zephyr_issue_key_field_value,
             test_case_properties_zephyr_test_steps_field_value,
             test_case_test_case_version_id,
             test_case_version,
             deleted_flag,
             p_qtest_jira_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_qtest_jira_key,
         test_case_id,
         project_id,
         release_id,
         requirement_id,
         project_automation,
         project_date_format,
         project_description,
         project_name,
         project_sample,
         project_start_date,
         project_start_dim_date_key,
         project_start_dim_time_key,
         project_status_id,
         project_template_id,
         project_uuid,
         project_x_explorer_access_level,
         release_created_date,
         release_created_dim_date_key,
         release_created_dim_time_key,
         release_end_date,
         release_end_dim_date_key,
         release_end_dim_time_key,
         release_last_modified_date,
         release_last_modified_dim_date_key,
         release_last_modified_dim_time_key,
         release_name,
         release_order,
         release_pid,
         release_project_id,
         release_properties_end_date,
         release_properties_end_dim_date_key,
         release_properties_end_dim_time_key,
         release_properties_start_date,
         release_properties_start_dim_date_key,
         release_properties_start_dim_time_key,
         release_properties_status,
         release_start_date,
         release_start_dim_date_key,
         release_start_dim_time_key,
         requirement_created_date,
         requirement_created_dim_date_key,
         requirement_created_dim_time_key,
         requirement_jira_defect_id,
         requirement_jira_issue,
         requirement_jira_priority,
         requirement_jira_status,
         requirement_jira_story_point,
         requirement_last_modified_date,
         requirement_last_modified_dim_date_key,
         requirement_last_modified_dim_time_key,
         requirement_name,
         requirement_parent_id,
         requirement_pid,
         requirement_project_id,
         test_case_created_date,
         test_case_created_dim_date_key,
         test_case_created_dim_time_key,
         test_case_creator_id,
         test_case_description,
         test_case_last_modified_date,
         test_case_last_modified_dim_date_key,
         test_case_last_modified_dim_time_key,
         test_case_name,
         test_case_order,
         test_case_parent_id,
         test_case_pid,
         test_case_project_id,
         test_case_properties_assigned_to_field_value_name,
         test_case_properties_automation_content_field_value,
         test_case_properties_automation_field_value_name,
         test_case_properties_candidate_for_automation_field_value_name,
         test_case_properties_description_field_value,
         test_case_properties_platform_field_value_name,
         test_case_properties_precondition_field_value,
         test_case_properties_shared_field_value,
         test_case_properties_status_field_value_name,
         test_case_properties_test_data_field_value,
         test_case_properties_test_tier_field_value_name,
         test_case_properties_type_field_value_name,
         test_case_properties_zephyr_issue_key_field_value,
         test_case_properties_zephyr_test_steps_field_value,
         test_case_test_case_version_id,
         test_case_version,
         dv_deleted,
         p_qtest_jira_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_qtest_jira)
--Done!
end
