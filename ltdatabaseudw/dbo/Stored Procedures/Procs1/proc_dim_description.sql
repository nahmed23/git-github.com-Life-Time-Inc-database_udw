CREATE PROC [dbo].[proc_dim_description] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

/*
Notes:
  The dim_description_key column is set up a bit different in the dv_d_etl_map record since this will be populated with the source bk_hash plus the source object name
    There are many source tables for this and most are r_ tables, but need a p_ reference to indicate this is the _key column so referencing a non-existant p_ table
  The source_object and source_bk_hash columns are the business key columns and are set up a bit different in the dv_d_etl_map record since there are many sources, but we just need a reference to any business key
    We need to identify these as the bk columns so we are referencing any business key with at least two columns regardless of source or actual data type

  Remember to add dv_job_dependency records whenever new description tables are added here
*/

/* Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.*/
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @dv_batch_id as current_dv_batch_id
  from dbo.dim_description

/*
/* Use the following code to generate a new section in the code below*/
/* FYI - The union alls are about twice as fast as separate inserts so dynamic SQL would be slower */
select 'select ''' + table_name + ''' + ''_'' + bk_hash dim_description_key,' + char(13)
     + '       ''' + table_name + ''' source_object,' + char(13)
     + '       bk_hash source_bk_hash,' + char(13)
     + '       ' + case when (select count(*) from information_schema.columns where columns.table_name = tables.table_name and columns.column_name = 'abbreviation') = 1 then 'abbreviation' else 'null' end + ' abbreviated_description,' + char(13)
     + '       description,' + char(13)
     + '       dv_load_date_time,' + char(13)
     + '       dv_load_end_date_time,' + char(13)
     + '       dv_batch_id' + char(13)
     + '  from ' + table_name + char(13)
     + '  join #dv_batch_id' + char(13)
     + '    on ' + table_name + '.dv_batch_id > #dv_batch_id.max_dv_batch_id' + char(13)
     + '    or ' + table_name + '.dv_batch_id = #dv_batch_id.current_dv_batch_id' + char(13)
     + ' where ' + table_name + '.dv_load_end_date_time = ''Dec 31, 9999''' + char(13)
     + 'union all'
  from information_schema.tables
 where table_name in (select table_name from information_schema.tables where table_name like 'r[_]mms[_]%')
 order by table_name
*/

/* Create #dim_description to hold all of the new and changed descriptions*/
if object_id('tempdb..#dim_description') is not null drop table #dim_description
create table dbo.#dim_description with(distribution=round_robin, location=user_db, heap) as
select dim_description_key,
       source_object,
       source_bk_hash,
       abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from dim_description
 where 1=2

/* Insert the new and changed descriptions from mms into #dim_description*/
insert #dim_description(dim_description_key,source_object,source_bk_hash,abbreviated_description,description,dv_load_date_time,dv_load_end_date_time,dv_batch_id)
select 'r_mms_val_address_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_address_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_address_type
  join #dv_batch_id
    on r_mms_val_address_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_address_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_address_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_assessment_day' + '_' + bk_hash dim_description_key,
       'r_mms_val_assessment_day' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_assessment_day
  join #dv_batch_id
    on r_mms_val_assessment_day.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_assessment_day.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_assessment_day.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_business_area' + '_' + bk_hash dim_description_key,
       'r_mms_val_business_area' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_business_area
  join #dv_batch_id
    on r_mms_val_business_area.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_business_area.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_business_area.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_check_in_group' + '_' + bk_hash dim_description_key,
       'r_mms_val_check_in_group' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_check_in_group
  join #dv_batch_id
    on r_mms_val_check_in_group.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_check_in_group.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_check_in_group.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_child_center_usage_exception' + '_' + bk_hash dim_description_key,
       'r_mms_val_child_center_usage_exception' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_child_center_usage_exception
  join #dv_batch_id
    on r_mms_val_child_center_usage_exception.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_child_center_usage_exception.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_child_center_usage_exception.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_club_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_club_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_club_type
  join #dv_batch_id
    on r_mms_val_club_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_club_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_club_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_commissionable' + '_' + bk_hash dim_description_key,
       'r_mms_val_commissionable' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_commissionable
  join #dv_batch_id
    on r_mms_val_commissionable.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_commissionable.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_commissionable.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_communication_preference' + '_' + bk_hash dim_description_key,
       'r_mms_val_communication_preference' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_communication_preference
  join #dv_batch_id
    on r_mms_val_communication_preference.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_communication_preference.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_communication_preference.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_communication_preference_source' + '_' + bk_hash dim_description_key,
       'r_mms_val_communication_preference_source' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_communication_preference_source
  join #dv_batch_id
    on r_mms_val_communication_preference_source.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_communication_preference_source.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_communication_preference_source.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_country' + '_' + bk_hash dim_description_key,
       'r_mms_val_country' source_object,
       bk_hash source_bk_hash,
       abbreviation abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_country
  join #dv_batch_id
    on r_mms_val_country.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_country.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_country.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_credit_card_batch_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_credit_card_batch_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_credit_card_batch_status
  join #dv_batch_id
    on r_mms_val_credit_card_batch_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_credit_card_batch_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_credit_card_batch_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_currency_code' + '_' + bk_hash dim_description_key,
       'r_mms_val_currency_code' source_object,
       bk_hash source_bk_hash,
       currency_code abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_currency_code
  join #dv_batch_id
    on r_mms_val_currency_code.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_currency_code.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_currency_code.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_cw_region' + '_' + bk_hash dim_description_key,
       'r_mms_val_cw_region' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_cw_region
  join #dv_batch_id
    on r_mms_val_cw_region.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_cw_region.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_cw_region.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_discount_application_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_discount_application_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_discount_application_type
  join #dv_batch_id
    on r_mms_val_discount_application_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_discount_application_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_discount_application_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_discount_combine_rule' + '_' + bk_hash dim_description_key,
       'r_mms_val_discount_combine_rule' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_discount_combine_rule
  join #dv_batch_id
    on r_mms_val_discount_combine_rule.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_discount_combine_rule.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_discount_combine_rule.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_discount_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_discount_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_discount_type
  join #dv_batch_id
    on r_mms_val_discount_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_discount_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_discount_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_drawer_audit_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_drawer_audit_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_drawer_audit_type
  join #dv_batch_id
    on r_mms_val_drawer_audit_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_drawer_audit_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_drawer_audit_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_drawer_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_drawer_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_drawer_status
  join #dv_batch_id
    on r_mms_val_drawer_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_drawer_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_drawer_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_eft_option' + '_' + bk_hash dim_description_key,
       'r_mms_val_eft_option' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_eft_option
  join #dv_batch_id
    on r_mms_val_eft_option.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_eft_option.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_eft_option.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_eft_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_eft_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_eft_status
  join #dv_batch_id
    on r_mms_val_eft_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_eft_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_eft_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_employee_role' + '_' + bk_hash dim_description_key,
       'r_mms_val_employee_role' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_employee_role
  join #dv_batch_id
    on r_mms_val_employee_role.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_employee_role.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_employee_role.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_enrollment_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_enrollment_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_enrollment_type
  join #dv_batch_id
    on r_mms_val_enrollment_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_enrollment_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_enrollment_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_flex_reason' + '_' + bk_hash dim_description_key,
       'r_mms_val_flex_reason' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_flex_reason
  join #dv_batch_id
    on r_mms_val_flex_reason.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_flex_reason.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_flex_reason.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_gl_group' + '_' + bk_hash dim_description_key,
       'r_mms_val_gl_group' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_gl_group
  join #dv_batch_id
    on r_mms_val_gl_group.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_gl_group.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_gl_group.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_ig_profit_center' + '_' + bk_hash dim_description_key,
       'r_mms_val_ig_profit_center' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_ig_profit_center
  join #dv_batch_id
    on r_mms_val_ig_profit_center.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_ig_profit_center.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_ig_profit_center.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_member_activity_region' + '_' + bk_hash dim_description_key,
       'r_mms_val_member_activity_region' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_member_activity_region
  join #dv_batch_id
    on r_mms_val_member_activity_region.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_member_activity_region.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_member_activity_region.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_member_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_member_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_member_type
  join #dv_batch_id
    on r_mms_val_member_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_member_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_member_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_membership_message_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_membership_message_type' source_object,
       bk_hash source_bk_hash,
       abbreviation abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_membership_message_type
  join #dv_batch_id
    on r_mms_val_membership_message_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_membership_message_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_membership_message_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_membership_modification_request_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_membership_modification_request_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_membership_modification_request_status
  join #dv_batch_id
    on r_mms_val_membership_modification_request_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_membership_modification_request_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_membership_modification_request_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_membership_modification_request_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_membership_modification_request_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_membership_modification_request_type
  join #dv_batch_id
    on r_mms_val_membership_modification_request_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_membership_modification_request_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_membership_modification_request_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_membership_source' + '_' + bk_hash dim_description_key,
       'r_mms_val_membership_source' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_membership_source
  join #dv_batch_id
    on r_mms_val_membership_source.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_membership_source.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_membership_source.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_membership_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_membership_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_membership_status
  join #dv_batch_id
    on r_mms_val_membership_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_membership_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_membership_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_membership_type_attribute' + '_' + bk_hash dim_description_key,
       'r_mms_val_membership_type_attribute' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_membership_type_attribute
  join #dv_batch_id
    on r_mms_val_membership_type_attribute.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_membership_type_attribute.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_membership_type_attribute.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_membership_type_family_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_membership_type_family_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_membership_type_family_status
  join #dv_batch_id
    on r_mms_val_membership_type_family_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_membership_type_family_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_membership_type_family_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_membership_type_group' + '_' + bk_hash dim_description_key,
       'r_mms_val_membership_type_group' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_membership_type_group
  join #dv_batch_id
    on r_mms_val_membership_type_group.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_membership_type_group.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_membership_type_group.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_membership_upgrade_date_range' + '_' + bk_hash dim_description_key,
       'r_mms_val_membership_upgrade_date_range' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_membership_upgrade_date_range
  join #dv_batch_id
    on r_mms_val_membership_upgrade_date_range.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_membership_upgrade_date_range.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_membership_upgrade_date_range.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_message_severity' + '_' + bk_hash dim_description_key,
       'r_mms_val_message_severity' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_message_severity
  join #dv_batch_id
    on r_mms_val_message_severity.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_message_severity.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_message_severity.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_message_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_message_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_message_status
  join #dv_batch_id
    on r_mms_val_message_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_message_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_message_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_mip_category' + '_' + bk_hash dim_description_key,
       'r_mms_val_mip_category' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_mip_category
  join #dv_batch_id
    on r_mms_val_mip_category.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_mip_category.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_mip_category.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_mip_interest_category' + '_' + bk_hash dim_description_key,
       'r_mms_val_mip_interest_category' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_mip_interest_category
  join #dv_batch_id
    on r_mms_val_mip_interest_category.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_mip_interest_category.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_mip_interest_category.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_mip_item' + '_' + bk_hash dim_description_key,
       'r_mms_val_mip_item' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_mip_item
  join #dv_batch_id
    on r_mms_val_mip_item.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_mip_item.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_mip_item.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_name_prefix' + '_' + bk_hash dim_description_key,
       'r_mms_val_name_prefix' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_name_prefix
  join #dv_batch_id
    on r_mms_val_name_prefix.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_name_prefix.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_name_prefix.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_name_suffix' + '_' + bk_hash dim_description_key,
       'r_mms_val_name_suffix' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_name_suffix
  join #dv_batch_id
    on r_mms_val_name_suffix.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_name_suffix.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_name_suffix.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_package_adjustment_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_package_adjustment_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_package_adjustment_type
  join #dv_batch_id
    on r_mms_val_package_adjustment_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_package_adjustment_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_package_adjustment_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_package_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_package_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_package_status
  join #dv_batch_id
    on r_mms_val_package_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_package_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_package_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_payment_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_payment_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_payment_type
  join #dv_batch_id
    on r_mms_val_payment_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_payment_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_payment_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_phone_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_phone_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_phone_type
  join #dv_batch_id
    on r_mms_val_phone_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_phone_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_phone_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_pre_sale' + '_' + bk_hash dim_description_key,
       'r_mms_val_pre_sale' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_pre_sale
  join #dv_batch_id
    on r_mms_val_pre_sale.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_pre_sale.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_pre_sale.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_pricing_method' + '_' + bk_hash dim_description_key,
       'r_mms_val_pricing_method' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_pricing_method
  join #dv_batch_id
    on r_mms_val_pricing_method.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_pricing_method.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_pricing_method.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_pricing_rule' + '_' + bk_hash dim_description_key,
       'r_mms_val_pricing_rule' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_pricing_rule
  join #dv_batch_id
    on r_mms_val_pricing_rule.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_pricing_rule.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_pricing_rule.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_product_sales_channel' + '_' + bk_hash dim_description_key,
       'r_mms_val_product_sales_channel' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       'LT E-Commerce - ' + description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_product_sales_channel
  join #dv_batch_id
    on r_mms_val_product_sales_channel.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_product_sales_channel.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_product_sales_channel.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_product_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_product_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_product_status
  join #dv_batch_id
    on r_mms_val_product_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_product_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_product_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_product_tier_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_product_tier_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_product_tier_type
  join #dv_batch_id
    on r_mms_val_product_tier_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_product_tier_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_product_tier_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_program_identifier_validation_class' + '_' + bk_hash dim_description_key,
       'r_mms_val_program_identifier_validation_class' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_program_identifier_validation_class
  join #dv_batch_id
    on r_mms_val_program_identifier_validation_class.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_program_identifier_validation_class.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_program_identifier_validation_class.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_pt_credit_card_transaction_code' + '_' + bk_hash dim_description_key,
       'r_mms_val_pt_credit_card_transaction_code' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_pt_credit_card_transaction_code
  join #dv_batch_id
    on r_mms_val_pt_credit_card_transaction_code.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_pt_credit_card_transaction_code.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_pt_credit_card_transaction_code.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_pt_credit_card_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_pt_credit_card_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_pt_credit_card_type
  join #dv_batch_id
    on r_mms_val_pt_credit_card_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_pt_credit_card_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_pt_credit_card_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_pt_rcl_area' + '_' + bk_hash dim_description_key,
       'r_mms_val_pt_rcl_area' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_pt_rcl_area
  join #dv_batch_id
    on r_mms_val_pt_rcl_area.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_pt_rcl_area.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_pt_rcl_area.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_qualified_sales_promotion_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_qualified_sales_promotion_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_qualified_sales_promotion_type
  join #dv_batch_id
    on r_mms_val_qualified_sales_promotion_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_qualified_sales_promotion_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_qualified_sales_promotion_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_recurrent_product_source' + '_' + bk_hash dim_description_key,
       'r_mms_val_recurrent_product_source' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_recurrent_product_source
  join #dv_batch_id
    on r_mms_val_recurrent_product_source.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_recurrent_product_source.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_recurrent_product_source.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_recurrent_product_termination_reason' + '_' + bk_hash dim_description_key,
       'r_mms_val_recurrent_product_termination_reason' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_recurrent_product_termination_reason
  join #dv_batch_id
    on r_mms_val_recurrent_product_termination_reason.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_recurrent_product_termination_reason.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_recurrent_product_termination_reason.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_recurrent_product_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_recurrent_product_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_recurrent_product_type
  join #dv_batch_id
    on r_mms_val_recurrent_product_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_recurrent_product_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_recurrent_product_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_region' + '_' + bk_hash dim_description_key,
       'r_mms_val_region' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_region
  join #dv_batch_id
    on r_mms_val_region.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_region.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_region.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_reimbursement_program_processing_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_reimbursement_program_processing_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_reimbursement_program_processing_type
  join #dv_batch_id
    on r_mms_val_reimbursement_program_processing_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_reimbursement_program_processing_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_reimbursement_program_processing_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_reimbursement_program_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_reimbursement_program_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_reimbursement_program_type
  join #dv_batch_id
    on r_mms_val_reimbursement_program_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_reimbursement_program_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_reimbursement_program_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_reimbursement_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_reimbursement_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_reimbursement_type
  join #dv_batch_id
    on r_mms_val_reimbursement_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_reimbursement_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_reimbursement_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_reimbursement_usage_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_reimbursement_usage_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_reimbursement_usage_type
  join #dv_batch_id
    on r_mms_val_reimbursement_usage_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_reimbursement_usage_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_reimbursement_usage_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_restricted_group' + '_' + bk_hash dim_description_key,
       'r_mms_val_restricted_group' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_restricted_group
  join #dv_batch_id
    on r_mms_val_restricted_group.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_restricted_group.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_restricted_group.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_revenue_reporting_category' + '_' + bk_hash dim_description_key,
       'r_mms_val_revenue_reporting_category' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_revenue_reporting_category
  join #dv_batch_id
    on r_mms_val_revenue_reporting_category.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_revenue_reporting_category.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_revenue_reporting_category.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_sales_area' + '_' + bk_hash dim_description_key,
       'r_mms_val_sales_area' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_sales_area
  join #dv_batch_id
    on r_mms_val_sales_area.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_sales_area.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_sales_area.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_sales_promotion_attribute' + '_' + bk_hash dim_description_key,
       'r_mms_val_sales_promotion_attribute' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_sales_promotion_attribute
  join #dv_batch_id
    on r_mms_val_sales_promotion_attribute.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_sales_promotion_attribute.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_sales_promotion_attribute.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_sales_promotion_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_sales_promotion_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_sales_promotion_type
  join #dv_batch_id
    on r_mms_val_sales_promotion_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_sales_promotion_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_sales_promotion_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_sales_reporting_category' + '_' + bk_hash dim_description_key,
       'r_mms_val_sales_reporting_category' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_sales_reporting_category
  join #dv_batch_id
    on r_mms_val_sales_reporting_category.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_sales_reporting_category.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_sales_reporting_category.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_state' + '_' + bk_hash dim_description_key,
       'r_mms_val_state' source_object,
       bk_hash source_bk_hash,
       abbreviation abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_state
  join #dv_batch_id
    on r_mms_val_state.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_state.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_state.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_statement_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_statement_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_statement_type
  join #dv_batch_id
    on r_mms_val_statement_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_statement_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_statement_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_termination_reason' + '_' + bk_hash dim_description_key,
       'r_mms_val_termination_reason' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_termination_reason
  join #dv_batch_id
    on r_mms_val_termination_reason.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_termination_reason.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_termination_reason.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_termination_reason_club_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_termination_reason_club_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_termination_reason_club_type
  join #dv_batch_id
    on r_mms_val_termination_reason_club_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_termination_reason_club_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_termination_reason_club_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_time_zone' + '_' + bk_hash dim_description_key,
       'r_mms_val_time_zone' source_object,
       bk_hash source_bk_hash,
       abbreviation abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_time_zone
  join #dv_batch_id
    on r_mms_val_time_zone.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_time_zone.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_time_zone.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_tran_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_tran_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_tran_type
  join #dv_batch_id
    on r_mms_val_tran_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_tran_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_tran_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_unit_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_unit_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_unit_type
  join #dv_batch_id
    on r_mms_val_unit_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_unit_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_unit_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_web_order_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_web_order_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_web_order_status
  join #dv_batch_id
    on r_mms_val_web_order_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_web_order_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_web_order_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
select 'r_mms_val_welcome_kit_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_welcome_kit_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_welcome_kit_type
  join #dv_batch_id
    on r_mms_val_welcome_kit_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_welcome_kit_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_welcome_kit_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
union all
/* Insert the new and changed descriptions from UDW-6559 Update dim_description with val-activityarea data*/
select 'r_mms_val_activity_area' + '_' + bk_hash dim_description_key,
       'r_mms_val_activity_area' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_activity_area
  join #dv_batch_id
    on r_mms_val_activity_area.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_activity_area.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_activity_area.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
 union all
 select 'r_mms_val_communication_preference_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_communication_preference_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_communication_preference_status
  join #dv_batch_id
    on r_mms_val_communication_preference_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_communication_preference_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_communication_preference_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
  union all
 select 'r_mms_val_card_level' + '_' + bk_hash dim_description_key,
       'r_mms_val_card_level' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_card_level
  join #dv_batch_id
    on r_mms_val_card_level.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_card_level.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_card_level.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
 union all
 select 'r_mms_val_payment_status' + '_' + bk_hash dim_description_key,
       'r_mms_val_payment_status' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_payment_status
  join #dv_batch_id
    on r_mms_val_payment_status.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_payment_status.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_payment_status.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
 union all
 /*/*-Added in DW_2019_09_04 for UDW-10202*/*/
 select 'r_mms_val_member_attribute_type' + '_' + bk_hash dim_description_key,
       'r_mms_val_member_attribute_type' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_member_attribute_type
  join #dv_batch_id
    on r_mms_val_member_attribute_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_member_attribute_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_member_attribute_type.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')
 union all
 /*/*-Added in DW_2020_02_26 for UDW-11564 MMS ValMembershipModificationRequest*/*/
 select 'r_mms_val_membership_modification_request' + '_' + bk_hash dim_description_key,
       'r_mms_val_membership_modification_request' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_mms_val_membership_modification_request
  join #dv_batch_id
    on r_mms_val_membership_modification_request.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_membership_modification_request.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_mms_val_membership_modification_request.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')


/* Insert the new and changed descriptions from lt_bucks into #dim_description*/
insert #dim_description(dim_description_key,source_object,source_bk_hash,abbreviated_description,description,dv_load_date_time,dv_load_end_date_time,dv_batch_id)
select 'r_lt_bucks_transaction_types' + '_' + bk_hash dim_description_key,
       'r_lt_bucks_transaction_types' source_object,
       bk_hash source_bk_hash,
       null abbreviated_description,
       ttype_desc description,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id
  from r_lt_bucks_transaction_types
  join #dv_batch_id
    on r_lt_bucks_transaction_types.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_lt_bucks_transaction_types.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where r_lt_bucks_transaction_types.dv_load_end_date_time = 'Dec 31, 9999' and bk_hash not in ('-999','-998','-997')



/*/*/*/*----------------------------------inserts from non-reference tables-----------------------------------------*/*/*/*/
 insert #dim_description(dim_description_key,source_object,source_bk_hash,abbreviated_description,description,dv_load_date_time,dv_load_end_date_time,dv_batch_id)
 select 's_mms_reimbursement_program_identifier_format_part.reimbursement_program_identifier_format_part_id_'+s_mms_reimbursement_program_identifier_format_part.bk_hash dim_description_key,
        's_mms_reimbursement_program_identifier_format_part' source_object,
        s_mms_reimbursement_program_identifier_format_part.bk_hash source_bk_hash,
        null abbreviated_description,
        field_name description,
        p_mms_reimbursement_program_identifier_format_part.dv_load_date_time,
        p_mms_reimbursement_program_identifier_format_part.dv_load_end_date_time,
        p_mms_reimbursement_program_identifier_format_part.dv_batch_id
  from s_mms_reimbursement_program_identifier_format_part
  join p_mms_reimbursement_program_identifier_format_part
    on s_mms_reimbursement_program_identifier_format_part.s_mms_reimbursement_program_identifier_format_part_id = p_mms_reimbursement_program_identifier_format_part.s_mms_reimbursement_program_identifier_format_part_id
  join #dv_batch_id
    on p_mms_reimbursement_program_identifier_format_part.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or p_mms_reimbursement_program_identifier_format_part.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where p_mms_reimbursement_program_identifier_format_part.dv_load_end_date_time = 'Dec 31, 9999'
   and p_mms_reimbursement_program_identifier_format_part.bk_hash  not in  ('-997','-998','-999')

 union all

 select 's_mms_department.department_id_'+s_mms_department.bk_hash dim_description_key,
        's_mms_department' source_object,
        s_mms_department.bk_hash source_bk_hash,
        null abbreviated_description,
        s_mms_department.description description,
        p_mms_department.dv_load_date_time,
        p_mms_department.dv_load_end_date_time,
        p_mms_department.dv_batch_id
  from s_mms_department
  join p_mms_department
    on s_mms_department.s_mms_department_id = p_mms_department.s_mms_department_id
  join #dv_batch_id
    on p_mms_department.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or p_mms_department.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where p_mms_department.dv_load_end_date_time = 'Dec 31, 9999'
   and p_mms_department.bk_hash  not in  ('-997','-998','-999')

    union all

  /*/*ig_ig_dimension_tender_dimension data for dim_description*/*/
  /*- UDW-7556 Altered d_ig_ig_dimension_tender_dimension*/
 select 'd_ig_ig_dimension_tender_dimension' + '_' + dim_cafe_payment_type_key dim_description_key,
        'd_ig_ig_dimension_tender_dimension' source_object,
        dim_cafe_payment_type_key source_bk_hash,
        payment_class abbreviated_description,
        payment_type description,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id
  from d_ig_ig_dimension_tender_dimension
  join #dv_batch_id
    on d_ig_ig_dimension_tender_dimension.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or d_ig_ig_dimension_tender_dimension.dv_batch_id = #dv_batch_id.current_dv_batch_id
  where d_ig_ig_dimension_tender_dimension.dv_load_end_date_time = 'Dec 31, 9999'
   and d_ig_ig_dimension_tender_dimension.bk_hash  not in  ('-997','-998','-999')

    union all

  /*/*ig_ig_dimension_tender_dimension data for dim_description*/*/
  /*- UDW-7556 Altered d_ig_ig_dimension_tender_dimension*/
 select option_dim_description_key,
        'd_magento_eav_attribute_option_value' source_object,
        bk_hash source_bk_hash,
        null abbreviated_description,
        option_value description,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id
  from d_magento_eav_attribute_option_value
  join #dv_batch_id
    on d_magento_eav_attribute_option_value.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or d_magento_eav_attribute_option_value.dv_batch_id = #dv_batch_id.current_dv_batch_id
  where d_magento_eav_attribute_option_value.bk_hash  not in  ('-997','-998','-999')
  
   union all

/*/*-UDW-10555 inserting hard coded value for AUTOMATED TRIGGER,Life Time Health Program,Loyalty Program,MMS-----*/*/
select 'mms_sales_channel_special_employee_-2','r_mms_val_product_sales_channel', '-2','NULL','AUTOMATED TRIGGER',getdate(),'9999-12-31 00:00:00.000','20191014120635' union
select 'mms_sales_channel_special_employee_-4','r_mms_val_product_sales_channel', '-4','NULL','Life Time Health Program',getdate(),'9999-12-31 00:00:00.000','20191014120635' union
select 'mms_sales_channel_special_employee_-5','r_mms_val_product_sales_channel', '-5','NULL','Loyalty Program',getdate(),'9999-12-31 00:00:00.000','20191014120635' union
select 'mms_sales_channel_mms_default','r_mms_val_product_sales_channel', 'default','NULL','MMS',getdate(),'9999-12-31 00:00:00.000','20191014120635' 
  

/* Delete and re-insert*/
/* Do as a single transaction*/
/*   Delete records from the dim table that exist*/
/*   Insert records from current and missing batches*/

begin tran
  delete dbo.dim_description
   where dim_description_key in (select dim_description_key from #dim_description)

  insert dbo.dim_description(
               dim_description_key,
               source_object,
               source_bk_hash,
               abbreviated_description,
               description,
               dv_load_date_time,
               dv_load_end_date_time,
               dv_batch_id,
               dv_inserted_date_time,
               dv_insert_user)
  select #dim_description.dim_description_key,
         #dim_description.source_object,
         #dim_description.source_bk_hash,
         #dim_description.abbreviated_description,
         #dim_description.description,
         #dim_description.dv_load_date_time,
         #dim_description.dv_load_end_date_time,
         #dim_description.dv_batch_id,
         getdate(),
         suser_sname()
    from #dim_description
commit tran

end
