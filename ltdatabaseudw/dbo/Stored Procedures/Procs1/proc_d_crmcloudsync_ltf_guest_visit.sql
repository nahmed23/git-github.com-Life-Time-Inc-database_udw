CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_guest_visit] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_guest_visit)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_guest_visit_insert') is not null drop table #p_crmcloudsync_ltf_guest_visit_insert
create table dbo.#p_crmcloudsync_ltf_guest_visit_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_guest_visit.p_crmcloudsync_ltf_guest_visit_id,
       p_crmcloudsync_ltf_guest_visit.bk_hash
  from dbo.p_crmcloudsync_ltf_guest_visit
 where p_crmcloudsync_ltf_guest_visit.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_guest_visit.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_guest_visit.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_guest_visit.bk_hash,
       p_crmcloudsync_ltf_guest_visit.bk_hash dim_crm_ltf_guest_visit_key,
       p_crmcloudsync_ltf_guest_visit.activity_id activity_id,
       isnull(s_crmcloudsync_ltf_guest_visit.activity_type_code,'') activity_type_code,
       isnull(s_crmcloudsync_ltf_guest_visit.activity_type_code_name,'') activity_type_code_name,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.created_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.created_by as varchar(36)),'z#@$k%&P'))),2) end created_by_dim_crm_system_user_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when s_crmcloudsync_ltf_guest_visit.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_guest_visit.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
       when s_crmcloudsync_ltf_guest_visit.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_guest_visit.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_guest_visit.created_on created_on,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.created_on_behalf_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.created_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end created_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_guest_visit.description,'') description,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.ltf_lead_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.ltf_lead_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_lead_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.ltf_campaign_instance is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.ltf_campaign_instance as varchar(36)),'z#@$k%&P'))),2) end dim_crm_ltf_campaign_instance_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.ltf_club_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.ltf_club_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_ltf_club_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.owner_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.owner_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_owner_key,
       isnull(s_crmcloudsync_ltf_guest_visit.insert_user,'') insert_user,
       s_crmcloudsync_ltf_guest_visit.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when s_crmcloudsync_ltf_guest_visit.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_guest_visit.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
       when s_crmcloudsync_ltf_guest_visit.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_guest_visit.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       s_crmcloudsync_ltf_guest_visit.instance_type_code instance_type_code,
       isnull(s_crmcloudsync_ltf_guest_visit.instance_type_code_name,'') instance_type_code_name,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_address_1_city,'') ltf_address_1_city,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_address_1_county,'') ltf_address_1_county,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_address_1_line_1,'') ltf_address_1_line_1,
       s_crmcloudsync_ltf_guest_visit.ltf_address_1_postal_code ltf_address_1_postal_code,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_address_1_state_or_province,'') ltf_address_1_state_or_province,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_agreement_signature,'') ltf_agreement_signature,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.ltf_appointment_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.ltf_appointment_id as varchar(36)),'z#@$k%&P'))),2) end ltf_appointment_dim_crm_activity_key,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_appointment_id_name,'') ltf_appointment_id_name,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.ltf_assigned_mea is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.ltf_assigned_mea as varchar(36)),'z#@$k%&P'))),2) end ltf_assigned_mea_dim_crm_account_manager_key,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_assigned_mea_name,'') ltf_assigned_mea_name,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_campaign_instance_name,'') ltf_campaign_instance_name,
       s_crmcloudsync_ltf_guest_visit.ltf_club_close_to ltf_club_close_to,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_club_close_to_name,'') ltf_club_close_to_name,
       s_crmcloudsync_ltf_guest_visit.ltf_date_of_birth ltf_date_of_birth,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when s_crmcloudsync_ltf_guest_visit.ltf_date_of_birth is null then '-998'
       	 when  convert(varchar, s_crmcloudsync_ltf_guest_visit.ltf_date_of_birth, 112) > '20991231' then '99991231'
       		   when convert(varchar, s_crmcloudsync_ltf_guest_visit.ltf_date_of_birth , 112)< '19000101' then '19000101'  
        else convert(varchar, s_crmcloudsync_ltf_guest_visit.ltf_date_of_birth, 112)    end ltf_date_of_birth_dim_date_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
       when s_crmcloudsync_ltf_guest_visit.ltf_date_of_birth is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_guest_visit.ltf_date_of_birth,114), 1, 5),':','') end ltf_date_of_birth_dim_time_key,
       s_crmcloudsync_ltf_guest_visit.ltf_deduct_guest_priv ltf_deduct_guest_priv,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_email_address_1,'') ltf_email_address_1,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_employer,'') ltf_employer,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_first_name,'') ltf_first_name,
       s_crmcloudsync_ltf_guest_visit.ltf_gender ltf_gender,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_gender_name,'') ltf_gender_name,
       s_crmcloudsync_ltf_guest_visit.ltf_guest_type ltf_guest_type,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_guest_type_name,'') ltf_guest_type_name,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_interests,'') ltf_interests,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_last_name,'') ltf_last_name,
       s_crmcloudsync_ltf_guest_visit.ltf_line_of_business ltf_line_of_business,
       s_crmcloudsync_ltf_guest_visit.ltf_line_of_business_name ltf_line_of_business_name,
       s_crmcloudsync_ltf_guest_visit.ltf_matching_contact_count ltf_matching_contact_count,
       s_crmcloudsync_ltf_guest_visit.ltf_matching_lead_count ltf_matching_lead_count,
       s_crmcloudsync_ltf_guest_visit.ltf_membership_interest ltf_membership_interest,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_membership_interest_name,'') ltf_membership_interest_name,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_middle_name,'') ltf_middle_name,
       s_crmcloudsync_ltf_guest_visit.ltf_mobile_phone ltf_mobile_phone,
       s_crmcloudsync_ltf_guest_visit.ltf_online ltf_online,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_online_name,'') ltf_online_name,
       s_crmcloudsync_ltf_guest_visit.ltf_out_of_area ltf_out_of_area,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_out_of_area_name,'') ltf_out_of_area_name,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.ltf_party_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.ltf_party_id as varchar(36)),'z#@$k%&P'))),2) end ltf_party_dim_crm_ltf_party_key,
       s_crmcloudsync_ltf_guest_visit.ltf_qr_code_used ltf_qr_code_used,
       s_crmcloudsync_ltf_guest_visit.ltf_referral_source ltf_referral_source,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_referral_source_name,'') ltf_referral_source_name,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.ltf_referred_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.ltf_referred_by as varchar(36)),'z#@$k%&P'))),2) end ltf_referred_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_referred_by_name,'') ltf_referred_by_name,
       l_crmcloudsync_ltf_guest_visit.ltf_request_id ltf_request_id,
       s_crmcloudsync_ltf_guest_visit.ltf_same_day ltf_same_day,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_same_day_name,'') ltf_same_day_name,
       isnull(s_crmcloudsync_ltf_guest_visit.ltf_source,'') ltf_source,
       s_crmcloudsync_ltf_guest_visit.ltf_telephone1 ltf_telephone1,
       s_crmcloudsync_ltf_guest_visit.ltf_telephone2 ltf_telephone2,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.modified_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.modified_by as varchar(36)),'z#@$k%&P'))),2) end modified_by_dim_crm_system_user_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when s_crmcloudsync_ltf_guest_visit.modified_on is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_guest_visit.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
       when s_crmcloudsync_ltf_guest_visit.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_guest_visit.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_guest_visit.modified_on modified_on,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.modified_on_behalf_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end modified_on_behalf_by_dim_crm_system_user_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.new_club_name is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.new_club_name as varchar(36)),'z#@$k%&P'))),2) end new_club_name_dim_crm_ltf_club_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when s_crmcloudsync_ltf_guest_visit.overridden_created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_guest_visit.overridden_created_on, 112)    end overridden_created_dim_date_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
       when s_crmcloudsync_ltf_guest_visit.overridden_created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_guest_visit.overridden_created_on,114), 1, 5),':','') end overridden_created_dim_time_key,
       s_crmcloudsync_ltf_guest_visit.overridden_created_on overridden_created_on,
       isnull(s_crmcloudsync_ltf_guest_visit.owner_id_name,'') owner_id_name,
       isnull(s_crmcloudsync_ltf_guest_visit.owner_id_type,'') owner_id_type,
       l_crmcloudsync_ltf_guest_visit.owning_business_unit owning_business_unit,
       l_crmcloudsync_ltf_guest_visit.owning_team owning_team,
       l_crmcloudsync_ltf_guest_visit.owning_user owning_user,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash      
       	when l_crmcloudsync_ltf_guest_visit.ltf_referring_member_id is null then '-998'   
       	when isnumeric(l_crmcloudsync_ltf_guest_visit.ltf_referring_member_id) = 0  then '-999' 
       	     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_crmcloudsync_ltf_guest_visit.ltf_referring_member_id as int) as varchar(500)),'z#@$k%&P'))),2) end referring_dim_mms_member_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when l_crmcloudsync_ltf_guest_visit.regarding_object_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_guest_visit.regarding_object_id as varchar(36)),'z#@$k%&P'))),2) end regarding_object_dim_crm_system_user_key,
       l_crmcloudsync_ltf_guest_visit.regarding_object_id regarding_object_id,
       isnull(s_crmcloudsync_ltf_guest_visit.regarding_object_id_name,'') regarding_object_id_name,
       isnull(s_crmcloudsync_ltf_guest_visit.regarding_object_type_code,'') regarding_object_type_code,
       s_crmcloudsync_ltf_guest_visit.state_code state_code,
       isnull(s_crmcloudsync_ltf_guest_visit.state_code_name,'') state_code_name,
       s_crmcloudsync_ltf_guest_visit.status_code status_code,
       isnull(s_crmcloudsync_ltf_guest_visit.status_code_name,'') status_code_name,
       s_crmcloudsync_ltf_guest_visit.subject subject,
       s_crmcloudsync_ltf_guest_visit.update_user update_user,
       s_crmcloudsync_ltf_guest_visit.updated_date_time updated_date_time,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
           when s_crmcloudsync_ltf_guest_visit.updated_date_time is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_guest_visit.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_ltf_guest_visit.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_guest_visit.bk_hash
       when s_crmcloudsync_ltf_guest_visit.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_guest_visit.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_crmcloudsync_ltf_guest_visit.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_guest_visit.p_crmcloudsync_ltf_guest_visit_id,
       p_crmcloudsync_ltf_guest_visit.dv_batch_id,
       p_crmcloudsync_ltf_guest_visit.dv_load_date_time,
       p_crmcloudsync_ltf_guest_visit.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_guest_visit
  join dbo.p_crmcloudsync_ltf_guest_visit
    on h_crmcloudsync_ltf_guest_visit.bk_hash = p_crmcloudsync_ltf_guest_visit.bk_hash
  join #p_crmcloudsync_ltf_guest_visit_insert
    on p_crmcloudsync_ltf_guest_visit.bk_hash = #p_crmcloudsync_ltf_guest_visit_insert.bk_hash
   and p_crmcloudsync_ltf_guest_visit.p_crmcloudsync_ltf_guest_visit_id = #p_crmcloudsync_ltf_guest_visit_insert.p_crmcloudsync_ltf_guest_visit_id
  join dbo.l_crmcloudsync_ltf_guest_visit
    on p_crmcloudsync_ltf_guest_visit.bk_hash = l_crmcloudsync_ltf_guest_visit.bk_hash
   and p_crmcloudsync_ltf_guest_visit.l_crmcloudsync_ltf_guest_visit_id = l_crmcloudsync_ltf_guest_visit.l_crmcloudsync_ltf_guest_visit_id
  join dbo.s_crmcloudsync_ltf_guest_visit
    on p_crmcloudsync_ltf_guest_visit.bk_hash = s_crmcloudsync_ltf_guest_visit.bk_hash
   and p_crmcloudsync_ltf_guest_visit.s_crmcloudsync_ltf_guest_visit_id = s_crmcloudsync_ltf_guest_visit.s_crmcloudsync_ltf_guest_visit_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_guest_visit
   where d_crmcloudsync_ltf_guest_visit.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_guest_visit_insert)

  insert dbo.d_crmcloudsync_ltf_guest_visit(
             bk_hash,
             dim_crm_ltf_guest_visit_key,
             activity_id,
             activity_type_code,
             activity_type_code_name,
             created_by_dim_crm_system_user_key,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             created_on_behalf_by_dim_crm_system_user_key,
             description,
             dim_crm_lead_key,
             dim_crm_ltf_campaign_instance_key,
             dim_crm_ltf_club_key,
             dim_crm_owner_key,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             instance_type_code,
             instance_type_code_name,
             ltf_address_1_city,
             ltf_address_1_county,
             ltf_address_1_line_1,
             ltf_address_1_postal_code,
             ltf_address_1_state_or_province,
             ltf_agreement_signature,
             ltf_appointment_dim_crm_activity_key,
             ltf_appointment_id_name,
             ltf_assigned_mea_dim_crm_account_manager_key,
             ltf_assigned_mea_name,
             ltf_campaign_instance_name,
             ltf_club_close_to,
             ltf_club_close_to_name,
             ltf_date_of_birth,
             ltf_date_of_birth_dim_date_key,
             ltf_date_of_birth_dim_time_key,
             ltf_deduct_guest_priv,
             ltf_email_address_1,
             ltf_employer,
             ltf_first_name,
             ltf_gender,
             ltf_gender_name,
             ltf_guest_type,
             ltf_guest_type_name,
             ltf_interests,
             ltf_last_name,
             ltf_line_of_business,
             ltf_line_of_business_name,
             ltf_matching_contact_count,
             ltf_matching_lead_count,
             ltf_membership_interest,
             ltf_membership_interest_name,
             ltf_middle_name,
             ltf_mobile_phone,
             ltf_online,
             ltf_online_name,
             ltf_out_of_area,
             ltf_out_of_area_name,
             ltf_party_dim_crm_ltf_party_key,
             ltf_qr_code_used,
             ltf_referral_source,
             ltf_referral_source_name,
             ltf_referred_by_dim_crm_system_user_key,
             ltf_referred_by_name,
             ltf_request_id,
             ltf_same_day,
             ltf_same_day_name,
             ltf_source,
             ltf_telephone1,
             ltf_telephone2,
             modified_by_dim_crm_system_user_key,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             modified_on_behalf_by_dim_crm_system_user_key,
             new_club_name_dim_crm_ltf_club_key,
             overridden_created_dim_date_key,
             overridden_created_dim_time_key,
             overridden_created_on,
             owner_id_name,
             owner_id_type,
             owning_business_unit,
             owning_team,
             owning_user,
             referring_dim_mms_member_key,
             regarding_object_dim_crm_system_user_key,
             regarding_object_id,
             regarding_object_id_name,
             regarding_object_type_code,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             subject,
             update_user,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_crmcloudsync_ltf_guest_visit_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_guest_visit_key,
         activity_id,
         activity_type_code,
         activity_type_code_name,
         created_by_dim_crm_system_user_key,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         created_on_behalf_by_dim_crm_system_user_key,
         description,
         dim_crm_lead_key,
         dim_crm_ltf_campaign_instance_key,
         dim_crm_ltf_club_key,
         dim_crm_owner_key,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         instance_type_code,
         instance_type_code_name,
         ltf_address_1_city,
         ltf_address_1_county,
         ltf_address_1_line_1,
         ltf_address_1_postal_code,
         ltf_address_1_state_or_province,
         ltf_agreement_signature,
         ltf_appointment_dim_crm_activity_key,
         ltf_appointment_id_name,
         ltf_assigned_mea_dim_crm_account_manager_key,
         ltf_assigned_mea_name,
         ltf_campaign_instance_name,
         ltf_club_close_to,
         ltf_club_close_to_name,
         ltf_date_of_birth,
         ltf_date_of_birth_dim_date_key,
         ltf_date_of_birth_dim_time_key,
         ltf_deduct_guest_priv,
         ltf_email_address_1,
         ltf_employer,
         ltf_first_name,
         ltf_gender,
         ltf_gender_name,
         ltf_guest_type,
         ltf_guest_type_name,
         ltf_interests,
         ltf_last_name,
         ltf_line_of_business,
         ltf_line_of_business_name,
         ltf_matching_contact_count,
         ltf_matching_lead_count,
         ltf_membership_interest,
         ltf_membership_interest_name,
         ltf_middle_name,
         ltf_mobile_phone,
         ltf_online,
         ltf_online_name,
         ltf_out_of_area,
         ltf_out_of_area_name,
         ltf_party_dim_crm_ltf_party_key,
         ltf_qr_code_used,
         ltf_referral_source,
         ltf_referral_source_name,
         ltf_referred_by_dim_crm_system_user_key,
         ltf_referred_by_name,
         ltf_request_id,
         ltf_same_day,
         ltf_same_day_name,
         ltf_source,
         ltf_telephone1,
         ltf_telephone2,
         modified_by_dim_crm_system_user_key,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         modified_on_behalf_by_dim_crm_system_user_key,
         new_club_name_dim_crm_ltf_club_key,
         overridden_created_dim_date_key,
         overridden_created_dim_time_key,
         overridden_created_on,
         owner_id_name,
         owner_id_type,
         owning_business_unit,
         owning_team,
         owning_user,
         referring_dim_mms_member_key,
         regarding_object_dim_crm_system_user_key,
         regarding_object_id,
         regarding_object_id_name,
         regarding_object_type_code,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         subject,
         update_user,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_crmcloudsync_ltf_guest_visit_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_guest_visit)
--Done!
end
