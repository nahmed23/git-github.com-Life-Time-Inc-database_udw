CREATE PROC [dbo].[proc_d_crmcloudsync_phone_call] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_phone_call)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_phone_call_insert') is not null drop table #p_crmcloudsync_phone_call_insert
create table dbo.#p_crmcloudsync_phone_call_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_phone_call.p_crmcloudsync_phone_call_id,
       p_crmcloudsync_phone_call.bk_hash
  from dbo.p_crmcloudsync_phone_call
 where p_crmcloudsync_phone_call.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_phone_call.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_phone_call.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_phone_call.bk_hash,
       p_crmcloudsync_phone_call.activity_id activity_id,
       s_crmcloudsync_phone_call.activity_additional_params activity_additional_params,
       s_crmcloudsync_phone_call.activity_type_code activity_type_code,
       s_crmcloudsync_phone_call.activity_type_code_name activity_type_code_name,
       s_crmcloudsync_phone_call.actual_duration_minutes actual_duration_minutes,
       case when p_crmcloudsync_phone_call.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash
           when s_crmcloudsync_phone_call.actual_end is null then '-998'
        else convert(varchar, s_crmcloudsync_phone_call.actual_end, 112) end actual_end_dim_date_key,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then p_crmcloudsync_phone_call.bk_hash
       when s_crmcloudsync_phone_call.actual_end is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_phone_call.actual_end,114), 1, 5),':','') end  actual_end_dim_time_key,
       case when p_crmcloudsync_phone_call.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash
           when s_crmcloudsync_phone_call.actual_start is null then '-998'
        else convert(varchar, s_crmcloudsync_phone_call.actual_start, 112) end actual_start_dim_date_key,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then p_crmcloudsync_phone_call.bk_hash
       when s_crmcloudsync_phone_call.actual_start is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_phone_call.actual_start,114), 1, 5),':','') end actual_start_dim_time_key,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then 'N'       when s_crmcloudsync_phone_call.is_billed = 1 then 'Y'       else 'N'   end billed_flag,
       s_crmcloudsync_phone_call.created_by_name created_by_name,
       s_crmcloudsync_phone_call.created_by_yomi_name created_by_yomi_name,
        case when p_crmcloudsync_phone_call.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash 
           when s_crmcloudsync_phone_call.created_on_behalf_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(s_crmcloudsync_phone_call.created_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end created_on_behalf_by_dim_crmcloudsync_system_user_key,
       s_crmcloudsync_phone_call.created_on_behalf_by_name created_on_behalf_by_name,
       s_crmcloudsync_phone_call.created_on_behalf_by_yomi_name created_on_behalf_by_yomi_name,
       case when p_crmcloudsync_phone_call.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash
           when s_crmcloudsync_phone_call.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_phone_call.created_on, 112) end created_on_dim_date_key,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then p_crmcloudsync_phone_call.bk_hash
       when s_crmcloudsync_phone_call.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_phone_call.created_on,114), 1, 5),':','') end created_on_dim_time_key,
       s_crmcloudsync_phone_call.description description,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash
              when s_crmcloudsync_phone_call.ltf_club_id is null then '-998' 
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(s_crmcloudsync_phone_call.ltf_club_id as varchar(36)),'z#@$k%&P'))),2) end dim_crmcloudsync_ltf_club_key,
       s_crmcloudsync_phone_call.direction_code direction_code,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then 'N'       when s_crmcloudsync_phone_call.direction_code = 1 then 'Y'       else 'N'   end direction_code_flag,
       s_crmcloudsync_phone_call.direction_code_name direction_code_name,
       s_crmcloudsync_phone_call.exchange_rate exchange_rate,
       s_crmcloudsync_phone_call.import_sequence_number import_sequence_number,
       s_crmcloudsync_phone_call.insert_user insert_user,
       s_crmcloudsync_phone_call.inserted_date_time inserted_date_time,
       s_crmcloudsync_phone_call.is_billed_name is_billed_name,
       s_crmcloudsync_phone_call.is_regular_activity_name is_regular_activity_name,
       s_crmcloudsync_phone_call.is_workflow_created_name is_workflow_created_name,
       s_crmcloudsync_phone_call.left_voice_mail left_voice_mail,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then 'N'       when s_crmcloudsync_phone_call.left_voice_mail = 1 then 'Y'       else 'N'   end left_voice_mail_flag,
       s_crmcloudsync_phone_call.left_voice_mail_name left_voice_mail_name,
       s_crmcloudsync_phone_call.ltf_call_sub_type ltf_call_sub_type,
       s_crmcloudsync_phone_call.ltf_call_sub_type_name ltf_call_sub_type_name,
       s_crmcloudsync_phone_call.ltf_call_type ltf_call_type,
       s_crmcloudsync_phone_call.ltf_call_type_name ltf_call_type_name,
       s_crmcloudsync_phone_call.ltf_caller_name ltf_caller_name,
       s_crmcloudsync_phone_call.ltf_club ltf_club,
       s_crmcloudsync_phone_call.ltf_club_id_name ltf_club_id_name,
       s_crmcloudsync_phone_call.ltf_club_name ltf_club_name,
       case when p_crmcloudsync_phone_call.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash
           when s_crmcloudsync_phone_call.ltf_most_recent_casl is null then '-998'
        else convert(varchar, s_crmcloudsync_phone_call.ltf_most_recent_casl, 112) end ltf_most_recent_casl_dim_date_key,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then p_crmcloudsync_phone_call.bk_hash
       when s_crmcloudsync_phone_call.ltf_most_recent_casl is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_phone_call.ltf_most_recent_casl,114), 1, 5),':','') end ltf_most_recent_casl_dim_time_key,
       s_crmcloudsync_phone_call.ltf_program ltf_program,
       s_crmcloudsync_phone_call.ltf_program_name ltf_program_name,
       s_crmcloudsync_phone_call.ltf_wrap_up_code ltf_wrap_up_code,
       s_crmcloudsync_phone_call.ltf_wrap_up_code_name ltf_wrap_up_code_name,
        case when p_crmcloudsync_phone_call.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash 
           when s_crmcloudsync_phone_call.modified_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(s_crmcloudsync_phone_call.modified_by as varchar(36)),'z#@$k%&P'))),2) end modified_by_dim_crmcloudsync_system_user_key,
       s_crmcloudsync_phone_call.modified_by_name modified_by_name,
       s_crmcloudsync_phone_call.modified_by_yomi_name modified_by_yomi_name,
        case when p_crmcloudsync_phone_call.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash 
           when s_crmcloudsync_phone_call.modified_on_behalf_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(s_crmcloudsync_phone_call.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end modified_on_behalf_by_dim_crmcloudsync_system_user_key,
       s_crmcloudsync_phone_call.modified_on_behalf_by_name modified_on_behalf_by_name,
       s_crmcloudsync_phone_call.modified_on_behalf_by_yomi_name modified_on_behalf_by_yomi_name,
       case when p_crmcloudsync_phone_call.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash
           when s_crmcloudsync_phone_call.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_phone_call.created_on, 112) end modified_on_dim_date_key,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then p_crmcloudsync_phone_call.bk_hash
       when s_crmcloudsync_phone_call.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_phone_call.modified_on,114), 1, 5),':','') end modified_on_dim_time_key,
       s_crmcloudsync_phone_call.new_callid new_callid,
       case when p_crmcloudsync_phone_call.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash
           when s_crmcloudsync_phone_call.overridden_created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_phone_call.overridden_created_on, 112) end overridden_on_dim_date_key,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then p_crmcloudsync_phone_call.bk_hash
       when s_crmcloudsync_phone_call.overridden_created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_phone_call.overridden_created_on,114), 1, 5),':','') end overridden_on_dim_time_key,
       s_crmcloudsync_phone_call.owner_id owner_id,
       s_crmcloudsync_phone_call.owner_id_name owner_id_name,
       s_crmcloudsync_phone_call.owner_id_type owner_id_type,
       s_crmcloudsync_phone_call.owner_id_yomi_name owner_id_yomi_name,
       s_crmcloudsync_phone_call.owning_business_unit owning_business_unit,
       s_crmcloudsync_phone_call.owning_team owning_team,
       s_crmcloudsync_phone_call.owning_user owning_user,
       s_crmcloudsync_phone_call.[from] phone_call_from,
       s_crmcloudsync_phone_call.[to] phone_call_to,
       s_crmcloudsync_phone_call.phone_number phone_number,
       l_crmcloudsync_phone_call.priority_code priority_code,
       s_crmcloudsync_phone_call.priority_code_name priority_code_name,
       s_crmcloudsync_phone_call.process_id process_id,
       s_crmcloudsync_phone_call.regarding_object_id regarding_object_id,
       s_crmcloudsync_phone_call.regarding_object_id_name regarding_object_id_name,
       s_crmcloudsync_phone_call.regarding_object_id_yomi_name regarding_object_id_yomi_name,
       s_crmcloudsync_phone_call.regarding_object_type_code regarding_object_type_code,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then 'N'       when s_crmcloudsync_phone_call.is_regular_activity = 1 then 'Y'       else 'N'   end regular_activity_flag,
       s_crmcloudsync_phone_call.scheduled_duration_minutes scheduled_duration_minutes,
       s_crmcloudsync_phone_call.scheduled_end scheduled_end,
       case when p_crmcloudsync_phone_call.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash
           when s_crmcloudsync_phone_call.scheduled_end is null then '-998'
        else convert(varchar, s_crmcloudsync_phone_call.scheduled_end, 112) end scheduled_end_dim_date_key,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then p_crmcloudsync_phone_call.bk_hash
       when s_crmcloudsync_phone_call.scheduled_end is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_phone_call.scheduled_end,114), 1, 5),':','') end scheduled_end_dim_time_key,
       s_crmcloudsync_phone_call.scheduled_start scheduled_start,
       case when p_crmcloudsync_phone_call.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_phone_call.bk_hash
           when s_crmcloudsync_phone_call.scheduled_start is null then '-998'
        else convert(varchar, s_crmcloudsync_phone_call.scheduled_start, 112) end  scheduled_start_dim_date_key,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then p_crmcloudsync_phone_call.bk_hash
       when s_crmcloudsync_phone_call.scheduled_start is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_phone_call.scheduled_start,114), 1, 5),':','') end  scheduled_start_dim_time_key,
       s_crmcloudsync_phone_call.service_id service_id,
       s_crmcloudsync_phone_call.stage_id stage_id,
       l_crmcloudsync_phone_call.state_code state_code,
       s_crmcloudsync_phone_call.state_code_name state_code_name,
       s_crmcloudsync_phone_call.status_code status_code,
       s_crmcloudsync_phone_call.status_code_name status_code_name,
       s_crmcloudsync_phone_call.sub_category sub_category,
       s_crmcloudsync_phone_call.subject subject,
       s_crmcloudsync_phone_call.time_zone_rule_version_number time_zone_rule_version_number,
       s_crmcloudsync_phone_call.transaction_currency_id transaction_currency_id,
       s_crmcloudsync_phone_call.transaction_currency_id_name transaction_currency_id_name,
       s_crmcloudsync_phone_call.traversed_path traversed_path,
       s_crmcloudsync_phone_call.update_user update_user,
       s_crmcloudsync_phone_call.updated_date_time updated_date_time,
       s_crmcloudsync_phone_call.utc_conversion_time_zone_code utc_conversion_time_zone_code,
       l_crmcloudsync_phone_call.version_number version_number,
       case when p_crmcloudsync_phone_call.bk_hash in ('-997','-998','-999') then 'N'       when s_crmcloudsync_phone_call.is_workflow_created = 1 then 'Y'       else 'N'   end workflow_created_flag,
       isnull(h_crmcloudsync_phone_call.dv_deleted,0) dv_deleted,
       p_crmcloudsync_phone_call.p_crmcloudsync_phone_call_id,
       p_crmcloudsync_phone_call.dv_batch_id,
       p_crmcloudsync_phone_call.dv_load_date_time,
       p_crmcloudsync_phone_call.dv_load_end_date_time
  from dbo.h_crmcloudsync_phone_call
  join dbo.p_crmcloudsync_phone_call
    on h_crmcloudsync_phone_call.bk_hash = p_crmcloudsync_phone_call.bk_hash
  join #p_crmcloudsync_phone_call_insert
    on p_crmcloudsync_phone_call.bk_hash = #p_crmcloudsync_phone_call_insert.bk_hash
   and p_crmcloudsync_phone_call.p_crmcloudsync_phone_call_id = #p_crmcloudsync_phone_call_insert.p_crmcloudsync_phone_call_id
  join dbo.l_crmcloudsync_phone_call
    on p_crmcloudsync_phone_call.bk_hash = l_crmcloudsync_phone_call.bk_hash
   and p_crmcloudsync_phone_call.l_crmcloudsync_phone_call_id = l_crmcloudsync_phone_call.l_crmcloudsync_phone_call_id
  join dbo.s_crmcloudsync_phone_call
    on p_crmcloudsync_phone_call.bk_hash = s_crmcloudsync_phone_call.bk_hash
   and p_crmcloudsync_phone_call.s_crmcloudsync_phone_call_id = s_crmcloudsync_phone_call.s_crmcloudsync_phone_call_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_phone_call
   where d_crmcloudsync_phone_call.bk_hash in (select bk_hash from #p_crmcloudsync_phone_call_insert)

  insert dbo.d_crmcloudsync_phone_call(
             bk_hash,
             activity_id,
             activity_additional_params,
             activity_type_code,
             activity_type_code_name,
             actual_duration_minutes,
             actual_end_dim_date_key,
             actual_end_dim_time_key,
             actual_start_dim_date_key,
             actual_start_dim_time_key,
             billed_flag,
             created_by_name,
             created_by_yomi_name,
             created_on_behalf_by_dim_crmcloudsync_system_user_key,
             created_on_behalf_by_name,
             created_on_behalf_by_yomi_name,
             created_on_dim_date_key,
             created_on_dim_time_key,
             description,
             dim_crmcloudsync_ltf_club_key,
             direction_code,
             direction_code_flag,
             direction_code_name,
             exchange_rate,
             import_sequence_number,
             insert_user,
             inserted_date_time,
             is_billed_name,
             is_regular_activity_name,
             is_workflow_created_name,
             left_voice_mail,
             left_voice_mail_flag,
             left_voice_mail_name,
             ltf_call_sub_type,
             ltf_call_sub_type_name,
             ltf_call_type,
             ltf_call_type_name,
             ltf_caller_name,
             ltf_club,
             ltf_club_id_name,
             ltf_club_name,
             ltf_most_recent_casl_dim_date_key,
             ltf_most_recent_casl_dim_time_key,
             ltf_program,
             ltf_program_name,
             ltf_wrap_up_code,
             ltf_wrap_up_code_name,
             modified_by_dim_crmcloudsync_system_user_key,
             modified_by_name,
             modified_by_yomi_name,
             modified_on_behalf_by_dim_crmcloudsync_system_user_key,
             modified_on_behalf_by_name,
             modified_on_behalf_by_yomi_name,
             modified_on_dim_date_key,
             modified_on_dim_time_key,
             new_callid,
             overridden_on_dim_date_key,
             overridden_on_dim_time_key,
             owner_id,
             owner_id_name,
             owner_id_type,
             owner_id_yomi_name,
             owning_business_unit,
             owning_team,
             owning_user,
             phone_call_from,
             phone_call_to,
             phone_number,
             priority_code,
             priority_code_name,
             process_id,
             regarding_object_id,
             regarding_object_id_name,
             regarding_object_id_yomi_name,
             regarding_object_type_code,
             regular_activity_flag,
             scheduled_duration_minutes,
             scheduled_end,
             scheduled_end_dim_date_key,
             scheduled_end_dim_time_key,
             scheduled_start,
             scheduled_start_dim_date_key,
             scheduled_start_dim_time_key,
             service_id,
             stage_id,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             sub_category,
             subject,
             time_zone_rule_version_number,
             transaction_currency_id,
             transaction_currency_id_name,
             traversed_path,
             update_user,
             updated_date_time,
             utc_conversion_time_zone_code,
             version_number,
             workflow_created_flag,
             deleted_flag,
             p_crmcloudsync_phone_call_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         activity_id,
         activity_additional_params,
         activity_type_code,
         activity_type_code_name,
         actual_duration_minutes,
         actual_end_dim_date_key,
         actual_end_dim_time_key,
         actual_start_dim_date_key,
         actual_start_dim_time_key,
         billed_flag,
         created_by_name,
         created_by_yomi_name,
         created_on_behalf_by_dim_crmcloudsync_system_user_key,
         created_on_behalf_by_name,
         created_on_behalf_by_yomi_name,
         created_on_dim_date_key,
         created_on_dim_time_key,
         description,
         dim_crmcloudsync_ltf_club_key,
         direction_code,
         direction_code_flag,
         direction_code_name,
         exchange_rate,
         import_sequence_number,
         insert_user,
         inserted_date_time,
         is_billed_name,
         is_regular_activity_name,
         is_workflow_created_name,
         left_voice_mail,
         left_voice_mail_flag,
         left_voice_mail_name,
         ltf_call_sub_type,
         ltf_call_sub_type_name,
         ltf_call_type,
         ltf_call_type_name,
         ltf_caller_name,
         ltf_club,
         ltf_club_id_name,
         ltf_club_name,
         ltf_most_recent_casl_dim_date_key,
         ltf_most_recent_casl_dim_time_key,
         ltf_program,
         ltf_program_name,
         ltf_wrap_up_code,
         ltf_wrap_up_code_name,
         modified_by_dim_crmcloudsync_system_user_key,
         modified_by_name,
         modified_by_yomi_name,
         modified_on_behalf_by_dim_crmcloudsync_system_user_key,
         modified_on_behalf_by_name,
         modified_on_behalf_by_yomi_name,
         modified_on_dim_date_key,
         modified_on_dim_time_key,
         new_callid,
         overridden_on_dim_date_key,
         overridden_on_dim_time_key,
         owner_id,
         owner_id_name,
         owner_id_type,
         owner_id_yomi_name,
         owning_business_unit,
         owning_team,
         owning_user,
         phone_call_from,
         phone_call_to,
         phone_number,
         priority_code,
         priority_code_name,
         process_id,
         regarding_object_id,
         regarding_object_id_name,
         regarding_object_id_yomi_name,
         regarding_object_type_code,
         regular_activity_flag,
         scheduled_duration_minutes,
         scheduled_end,
         scheduled_end_dim_date_key,
         scheduled_end_dim_time_key,
         scheduled_start,
         scheduled_start_dim_date_key,
         scheduled_start_dim_time_key,
         service_id,
         stage_id,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         sub_category,
         subject,
         time_zone_rule_version_number,
         transaction_currency_id,
         transaction_currency_id_name,
         traversed_path,
         update_user,
         updated_date_time,
         utc_conversion_time_zone_code,
         version_number,
         workflow_created_flag,
         dv_deleted,
         p_crmcloudsync_phone_call_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_phone_call)
--Done!
end
