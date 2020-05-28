CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_outreach_member_summary] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_outreach_member_summary)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_outreach_member_summary_insert') is not null drop table #p_crmcloudsync_ltf_outreach_member_summary_insert
create table dbo.#p_crmcloudsync_ltf_outreach_member_summary_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_outreach_member_summary.p_crmcloudsync_ltf_outreach_member_summary_id,
       p_crmcloudsync_ltf_outreach_member_summary.bk_hash
  from dbo.p_crmcloudsync_ltf_outreach_member_summary
 where p_crmcloudsync_ltf_outreach_member_summary.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_outreach_member_summary.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_outreach_member_summary.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_outreach_member_summary.bk_hash,
       p_crmcloudsync_ltf_outreach_member_summary.bk_hash dim_crm_ltf_outreach_member_summary_key,
       p_crmcloudsync_ltf_outreach_member_summary.ltf_outreach_member_summary_id ltf_outreach_member_summary_id,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when s_crmcloudsync_ltf_outreach_member_summary.ltf_activation_date is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_activation_date, 112)    end activation_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
       when s_crmcloudsync_ltf_outreach_member_summary.ltf_activation_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_activation_date,114), 1, 5),':','') end activation_dim_time_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when s_crmcloudsync_ltf_outreach_member_summary.ltf_claim_expiration is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_claim_expiration, 112)    end claim_expiration_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
       when s_crmcloudsync_ltf_outreach_member_summary.ltf_claim_expiration is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_claim_expiration,114), 1, 5),':','') end claim_expiration_dim_time_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.ltf_program_cycle_reference is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.ltf_program_cycle_reference as varchar(36)),'z#@$k%&P'))),2) end dim_crm_ltf_program_cycle_reference_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.ltf_subscription_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.ltf_subscription_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_ltf_subscription_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.owner_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.owner_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_owner_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.ltf_product is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.ltf_product as varchar(36)),'z#@$k%&P'))),2) end dim_crm_product_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when s_crmcloudsync_ltf_outreach_member_summary.ltf_initial_contact_date_cycle is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_initial_contact_date_cycle, 112)    end initial_contact_cycle_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
       when s_crmcloudsync_ltf_outreach_member_summary.ltf_initial_contact_date_cycle is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_initial_contact_date_cycle,114), 1, 5),':','') end initial_contact_cycle_dim_time_key,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.insert_user,'') insert_user,
       s_crmcloudsync_ltf_outreach_member_summary.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when s_crmcloudsync_ltf_outreach_member_summary.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_outreach_member_summary.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
       when s_crmcloudsync_ltf_outreach_member_summary.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when s_crmcloudsync_ltf_outreach_member_summary.ltf_last_attempt_date is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_last_attempt_date, 112)    end last_attempt_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
       when s_crmcloudsync_ltf_outreach_member_summary.ltf_last_attempt_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_last_attempt_date,114), 1, 5),':','') end last_attempt_dim_time_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when s_crmcloudsync_ltf_outreach_member_summary.ltf_last_contact_date is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_last_contact_date, 112)    end last_contact_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
       when s_crmcloudsync_ltf_outreach_member_summary.ltf_last_contact_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_last_contact_date,114), 1, 5),':','') end last_contact_dim_time_key,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_activation_date ltf_activation_date,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_claim_expiration ltf_claim_expiration,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.ltf_claimed_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.ltf_claimed_by as varchar(36)),'z#@$k%&P'))),2) end ltf_claimed_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_claimed_by_name,'') ltf_claimed_by_name,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.ltf_contact is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.ltf_contact as varchar(36)),'z#@$k%&P'))),2) end ltf_contact_dim_crm_contact_key,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_contact_name,'') ltf_contact_name,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_description,'') ltf_description,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.ltf_enrolled_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.ltf_enrolled_by as varchar(36)),'z#@$k%&P'))),2) end ltf_enrolled_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_enrolled_by_name,'') ltf_enrolled_by_name,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.ltf_initial_contact_by_cycle is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.ltf_initial_contact_by_cycle as varchar(36)),'z#@$k%&P'))),2) end ltf_initial_contact_by_cycle_dim_crm_contact_key,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_initial_contact_by_cycle_name,'') ltf_initial_contact_by_cycle_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_initial_contact_date_cycle ltf_initial_contact_date_cycle,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_initial_contact_type_cycle ltf_initial_contact_type_cycle,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_initial_contact_type_cycle_name,'') ltf_initial_contact_type_cycle_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_intercept_attempts_cycle ltf_intercept_attempts_cycle,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_intercept_contacts_cycle ltf_intercept_contacts_cycle,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.ltf_last_attempt_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.ltf_last_attempt_by as varchar(36)),'z#@$k%&P'))),2) end ltf_last_attempt_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_last_attempt_by_name,'') ltf_last_attempt_by_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_last_attempt_date ltf_last_attempt_date,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_last_attempt_type ltf_last_attempt_type,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_last_attempt_type_name,'') ltf_last_attempt_type_name,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.ltf_last_contact_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.ltf_last_contact_by as varchar(36)),'z#@$k%&P'))),2) end ltf_last_contact_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_last_contact_by_name,'') ltf_last_contact_by_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_last_contact_date ltf_last_contact_date,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_last_contact_type ltf_last_contact_type,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_last_contact_type_name,'') ltf_last_contact_type_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_ltf_employee ltf_ltf_employee,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_ltf_employee_name,'') ltf_ltf_employee_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_lthealth ltf_lthealth,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_lthealth_name,'') ltf_lthealth_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_meeting_attempts_cycle ltf_meeting_attempts_cycle,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_meeting_contacts_cycle ltf_meeting_contacts_cycle,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_member_number,'') ltf_member_number,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_membership_product,'') ltf_membership_product,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_next_anniversary ltf_next_anniversary,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_outreach_rank ltf_outreach_rank,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_phone_attempts_cycle ltf_phone_attempts_cycle,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_phone_contacts_cycle ltf_phone_contacts_cycle,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_product_name,'') ltf_product_name,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_program_cycle_reference_name,'') ltf_program_cycle_reference_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_risk_score ltf_risk_score,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_role ltf_role,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_role_name,'') ltf_role_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_segment ltf_segment,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_segment_name,'') ltf_segment_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_star_value ltf_star_value,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subscription_id_name,'') ltf_subscription_id_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_1 ltf_subsegment_1,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_1_name,'') ltf_subsegment_1_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_10 ltf_subsegment_10,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_10_name,'') ltf_subsegment_10_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_11 ltf_subsegment_11,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_11_name,'') ltf_subsegment_11_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_12 ltf_subsegment_12,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_12_name,'') ltf_subsegment_12_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_13 ltf_subsegment_13,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_13_name,'') ltf_subsegment_13_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_14 ltf_subsegment_14,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_14_name,'') ltf_subsegment_14_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_15 ltf_subsegment_15,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_15_name,'') ltf_subsegment_15_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_16 ltf_subsegment_16,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_16_name,'') ltf_subsegment_16_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_17 ltf_subsegment_17,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_17_name,'') ltf_subsegment_17_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_18 ltf_subsegment_18,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_18_name,'') ltf_subsegment_18_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_19 ltf_subsegment_19,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_19_name,'') ltf_subsegment_19_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_2 ltf_subsegment_2,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_2_name,'') ltf_subsegment_2_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_20 ltf_subsegment_20,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_20_name,'') ltf_subsegment_20_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_21 ltf_subsegment_21,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_21_name,'') ltf_subsegment_21_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_22 ltf_subsegment_22,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_22_name,'') ltf_subsegment_22_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_23 ltf_subsegment_23,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_23_name,'') ltf_subsegment_23_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_24 ltf_subsegment_24,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_24_name,'') ltf_subsegment_24_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_25 ltf_subsegment_25,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_25_name,'') ltf_subsegment_25_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_3 ltf_subsegment_3,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_3_name,'') ltf_subsegment_3_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_4 ltf_subsegment_4,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_4_name,'') ltf_subsegment_4_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_5 ltf_subsegment_5,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_5_name,'') ltf_subsegment_5_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_6 ltf_subsegment_6,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_6_name,'') ltf_subsegment_6_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_7 ltf_subsegment_7,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_7_name,'') ltf_subsegment_7_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_8 ltf_subsegment_8,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_8_name,'') ltf_subsegment_8_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_9 ltf_subsegment_9,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_subsegment_9_name,'') ltf_subsegment_9_name,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_talking_points,'') ltf_talking_points,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_targeted ltf_targeted,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.ltf_targeted_name,'') ltf_targeted_name,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_total_attempts_cycle ltf_total_attempts_cycle,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_total_contacts_cycle ltf_total_contacts_cycle,
       s_crmcloudsync_ltf_outreach_member_summary.ltf_years_of_membership ltf_years_of_membership,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.modified_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.modified_by as varchar(36)),'z#@$k%&P'))),2) end modified_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.modified_by_name,'') modified_by_name,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when s_crmcloudsync_ltf_outreach_member_summary.modified_on is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
       when s_crmcloudsync_ltf_outreach_member_summary.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_outreach_member_summary.modified_on modified_on,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.modified_on_behalf_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end modified_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.modified_on_behalf_by_name,'') modified_on_behalf_by_name,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when s_crmcloudsync_ltf_outreach_member_summary.ltf_next_anniversary is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_next_anniversary, 112)    end next_anniversary_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
       when s_crmcloudsync_ltf_outreach_member_summary.ltf_next_anniversary is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.ltf_next_anniversary,114), 1, 5),':','') end next_anniversary_dim_time_key,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.owner_id_name,'') owner_id_name,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.owner_id_type,'') owner_id_type,
       l_crmcloudsync_ltf_outreach_member_summary.owning_business_unit owning_business_unit,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when l_crmcloudsync_ltf_outreach_member_summary.ltf_claimed_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_outreach_member_summary.ltf_claimed_by as varchar(36)),'z#@$k%&P'))),2) end owning_user_dim_crm_system_user_key,
       s_crmcloudsync_ltf_outreach_member_summary.state_code state_code,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.state_code_name,'') state_code_name,
       s_crmcloudsync_ltf_outreach_member_summary.status_code status_code,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.status_code_name,'') status_code_name,
       case when s_crmcloudsync_ltf_outreach_member_summary.ltf_targeted = 1  then 'Y' else 'N' end targeted_flag,
       s_crmcloudsync_ltf_outreach_member_summary.time_zone_rule_version_number time_zone_rule_version_number,
       isnull(s_crmcloudsync_ltf_outreach_member_summary.update_user,'') update_user,
       s_crmcloudsync_ltf_outreach_member_summary.updated_date_time updated_date_time,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
           when s_crmcloudsync_ltf_outreach_member_summary.updated_date_time is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_ltf_outreach_member_summary.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_outreach_member_summary.bk_hash
       when s_crmcloudsync_ltf_outreach_member_summary.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_outreach_member_summary.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_crmcloudsync_ltf_outreach_member_summary.version_number version_number,
       isnull(h_crmcloudsync_ltf_outreach_member_summary.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_outreach_member_summary.p_crmcloudsync_ltf_outreach_member_summary_id,
       p_crmcloudsync_ltf_outreach_member_summary.dv_batch_id,
       p_crmcloudsync_ltf_outreach_member_summary.dv_load_date_time,
       p_crmcloudsync_ltf_outreach_member_summary.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_outreach_member_summary
  join dbo.p_crmcloudsync_ltf_outreach_member_summary
    on h_crmcloudsync_ltf_outreach_member_summary.bk_hash = p_crmcloudsync_ltf_outreach_member_summary.bk_hash
  join #p_crmcloudsync_ltf_outreach_member_summary_insert
    on p_crmcloudsync_ltf_outreach_member_summary.bk_hash = #p_crmcloudsync_ltf_outreach_member_summary_insert.bk_hash
   and p_crmcloudsync_ltf_outreach_member_summary.p_crmcloudsync_ltf_outreach_member_summary_id = #p_crmcloudsync_ltf_outreach_member_summary_insert.p_crmcloudsync_ltf_outreach_member_summary_id
  join dbo.l_crmcloudsync_ltf_outreach_member_summary
    on p_crmcloudsync_ltf_outreach_member_summary.bk_hash = l_crmcloudsync_ltf_outreach_member_summary.bk_hash
   and p_crmcloudsync_ltf_outreach_member_summary.l_crmcloudsync_ltf_outreach_member_summary_id = l_crmcloudsync_ltf_outreach_member_summary.l_crmcloudsync_ltf_outreach_member_summary_id
  join dbo.s_crmcloudsync_ltf_outreach_member_summary
    on p_crmcloudsync_ltf_outreach_member_summary.bk_hash = s_crmcloudsync_ltf_outreach_member_summary.bk_hash
   and p_crmcloudsync_ltf_outreach_member_summary.s_crmcloudsync_ltf_outreach_member_summary_id = s_crmcloudsync_ltf_outreach_member_summary.s_crmcloudsync_ltf_outreach_member_summary_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_outreach_member_summary
   where d_crmcloudsync_ltf_outreach_member_summary.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_outreach_member_summary_insert)

  insert dbo.d_crmcloudsync_ltf_outreach_member_summary(
             bk_hash,
             dim_crm_ltf_outreach_member_summary_key,
             ltf_outreach_member_summary_id,
             activation_dim_date_key,
             activation_dim_time_key,
             claim_expiration_dim_date_key,
             claim_expiration_dim_time_key,
             dim_crm_ltf_program_cycle_reference_key,
             dim_crm_ltf_subscription_key,
             dim_crm_owner_key,
             dim_crm_product_key,
             initial_contact_cycle_dim_date_key,
             initial_contact_cycle_dim_time_key,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             last_attempt_dim_date_key,
             last_attempt_dim_time_key,
             last_contact_dim_date_key,
             last_contact_dim_time_key,
             ltf_activation_date,
             ltf_claim_expiration,
             ltf_claimed_by_dim_crm_system_user_key,
             ltf_claimed_by_name,
             ltf_contact_dim_crm_contact_key,
             ltf_contact_name,
             ltf_description,
             ltf_enrolled_by_dim_crm_system_user_key,
             ltf_enrolled_by_name,
             ltf_initial_contact_by_cycle_dim_crm_contact_key,
             ltf_initial_contact_by_cycle_name,
             ltf_initial_contact_date_cycle,
             ltf_initial_contact_type_cycle,
             ltf_initial_contact_type_cycle_name,
             ltf_intercept_attempts_cycle,
             ltf_intercept_contacts_cycle,
             ltf_last_attempt_by_dim_crm_system_user_key,
             ltf_last_attempt_by_name,
             ltf_last_attempt_date,
             ltf_last_attempt_type,
             ltf_last_attempt_type_name,
             ltf_last_contact_by_dim_crm_system_user_key,
             ltf_last_contact_by_name,
             ltf_last_contact_date,
             ltf_last_contact_type,
             ltf_last_contact_type_name,
             ltf_ltf_employee,
             ltf_ltf_employee_name,
             ltf_lthealth,
             ltf_lthealth_name,
             ltf_meeting_attempts_cycle,
             ltf_meeting_contacts_cycle,
             ltf_member_number,
             ltf_membership_product,
             ltf_next_anniversary,
             ltf_outreach_rank,
             ltf_phone_attempts_cycle,
             ltf_phone_contacts_cycle,
             ltf_product_name,
             ltf_program_cycle_reference_name,
             ltf_risk_score,
             ltf_role,
             ltf_role_name,
             ltf_segment,
             ltf_segment_name,
             ltf_star_value,
             ltf_subscription_id_name,
             ltf_subsegment_1,
             ltf_subsegment_1_name,
             ltf_subsegment_10,
             ltf_subsegment_10_name,
             ltf_subsegment_11,
             ltf_subsegment_11_name,
             ltf_subsegment_12,
             ltf_subsegment_12_name,
             ltf_subsegment_13,
             ltf_subsegment_13_name,
             ltf_subsegment_14,
             ltf_subsegment_14_name,
             ltf_subsegment_15,
             ltf_subsegment_15_name,
             ltf_subsegment_16,
             ltf_subsegment_16_name,
             ltf_subsegment_17,
             ltf_subsegment_17_name,
             ltf_subsegment_18,
             ltf_subsegment_18_name,
             ltf_subsegment_19,
             ltf_subsegment_19_name,
             ltf_subsegment_2,
             ltf_subsegment_2_name,
             ltf_subsegment_20,
             ltf_subsegment_20_name,
             ltf_subsegment_21,
             ltf_subsegment_21_name,
             ltf_subsegment_22,
             ltf_subsegment_22_name,
             ltf_subsegment_23,
             ltf_subsegment_23_name,
             ltf_subsegment_24,
             ltf_subsegment_24_name,
             ltf_subsegment_25,
             ltf_subsegment_25_name,
             ltf_subsegment_3,
             ltf_subsegment_3_name,
             ltf_subsegment_4,
             ltf_subsegment_4_name,
             ltf_subsegment_5,
             ltf_subsegment_5_name,
             ltf_subsegment_6,
             ltf_subsegment_6_name,
             ltf_subsegment_7,
             ltf_subsegment_7_name,
             ltf_subsegment_8,
             ltf_subsegment_8_name,
             ltf_subsegment_9,
             ltf_subsegment_9_name,
             ltf_talking_points,
             ltf_targeted,
             ltf_targeted_name,
             ltf_total_attempts_cycle,
             ltf_total_contacts_cycle,
             ltf_years_of_membership,
             modified_by_dim_crm_system_user_key,
             modified_by_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             modified_on_behalf_by_dim_crm_system_user_key,
             modified_on_behalf_by_name,
             next_anniversary_dim_date_key,
             next_anniversary_dim_time_key,
             owner_id_name,
             owner_id_type,
             owning_business_unit,
             owning_user_dim_crm_system_user_key,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             targeted_flag,
             time_zone_rule_version_number,
             update_user,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             version_number,
             deleted_flag,
             p_crmcloudsync_ltf_outreach_member_summary_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_outreach_member_summary_key,
         ltf_outreach_member_summary_id,
         activation_dim_date_key,
         activation_dim_time_key,
         claim_expiration_dim_date_key,
         claim_expiration_dim_time_key,
         dim_crm_ltf_program_cycle_reference_key,
         dim_crm_ltf_subscription_key,
         dim_crm_owner_key,
         dim_crm_product_key,
         initial_contact_cycle_dim_date_key,
         initial_contact_cycle_dim_time_key,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         last_attempt_dim_date_key,
         last_attempt_dim_time_key,
         last_contact_dim_date_key,
         last_contact_dim_time_key,
         ltf_activation_date,
         ltf_claim_expiration,
         ltf_claimed_by_dim_crm_system_user_key,
         ltf_claimed_by_name,
         ltf_contact_dim_crm_contact_key,
         ltf_contact_name,
         ltf_description,
         ltf_enrolled_by_dim_crm_system_user_key,
         ltf_enrolled_by_name,
         ltf_initial_contact_by_cycle_dim_crm_contact_key,
         ltf_initial_contact_by_cycle_name,
         ltf_initial_contact_date_cycle,
         ltf_initial_contact_type_cycle,
         ltf_initial_contact_type_cycle_name,
         ltf_intercept_attempts_cycle,
         ltf_intercept_contacts_cycle,
         ltf_last_attempt_by_dim_crm_system_user_key,
         ltf_last_attempt_by_name,
         ltf_last_attempt_date,
         ltf_last_attempt_type,
         ltf_last_attempt_type_name,
         ltf_last_contact_by_dim_crm_system_user_key,
         ltf_last_contact_by_name,
         ltf_last_contact_date,
         ltf_last_contact_type,
         ltf_last_contact_type_name,
         ltf_ltf_employee,
         ltf_ltf_employee_name,
         ltf_lthealth,
         ltf_lthealth_name,
         ltf_meeting_attempts_cycle,
         ltf_meeting_contacts_cycle,
         ltf_member_number,
         ltf_membership_product,
         ltf_next_anniversary,
         ltf_outreach_rank,
         ltf_phone_attempts_cycle,
         ltf_phone_contacts_cycle,
         ltf_product_name,
         ltf_program_cycle_reference_name,
         ltf_risk_score,
         ltf_role,
         ltf_role_name,
         ltf_segment,
         ltf_segment_name,
         ltf_star_value,
         ltf_subscription_id_name,
         ltf_subsegment_1,
         ltf_subsegment_1_name,
         ltf_subsegment_10,
         ltf_subsegment_10_name,
         ltf_subsegment_11,
         ltf_subsegment_11_name,
         ltf_subsegment_12,
         ltf_subsegment_12_name,
         ltf_subsegment_13,
         ltf_subsegment_13_name,
         ltf_subsegment_14,
         ltf_subsegment_14_name,
         ltf_subsegment_15,
         ltf_subsegment_15_name,
         ltf_subsegment_16,
         ltf_subsegment_16_name,
         ltf_subsegment_17,
         ltf_subsegment_17_name,
         ltf_subsegment_18,
         ltf_subsegment_18_name,
         ltf_subsegment_19,
         ltf_subsegment_19_name,
         ltf_subsegment_2,
         ltf_subsegment_2_name,
         ltf_subsegment_20,
         ltf_subsegment_20_name,
         ltf_subsegment_21,
         ltf_subsegment_21_name,
         ltf_subsegment_22,
         ltf_subsegment_22_name,
         ltf_subsegment_23,
         ltf_subsegment_23_name,
         ltf_subsegment_24,
         ltf_subsegment_24_name,
         ltf_subsegment_25,
         ltf_subsegment_25_name,
         ltf_subsegment_3,
         ltf_subsegment_3_name,
         ltf_subsegment_4,
         ltf_subsegment_4_name,
         ltf_subsegment_5,
         ltf_subsegment_5_name,
         ltf_subsegment_6,
         ltf_subsegment_6_name,
         ltf_subsegment_7,
         ltf_subsegment_7_name,
         ltf_subsegment_8,
         ltf_subsegment_8_name,
         ltf_subsegment_9,
         ltf_subsegment_9_name,
         ltf_talking_points,
         ltf_targeted,
         ltf_targeted_name,
         ltf_total_attempts_cycle,
         ltf_total_contacts_cycle,
         ltf_years_of_membership,
         modified_by_dim_crm_system_user_key,
         modified_by_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         modified_on_behalf_by_dim_crm_system_user_key,
         modified_on_behalf_by_name,
         next_anniversary_dim_date_key,
         next_anniversary_dim_time_key,
         owner_id_name,
         owner_id_type,
         owning_business_unit,
         owning_user_dim_crm_system_user_key,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         targeted_flag,
         time_zone_rule_version_number,
         update_user,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         version_number,
         dv_deleted,
         p_crmcloudsync_ltf_outreach_member_summary_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_outreach_member_summary)
--Done!
end
