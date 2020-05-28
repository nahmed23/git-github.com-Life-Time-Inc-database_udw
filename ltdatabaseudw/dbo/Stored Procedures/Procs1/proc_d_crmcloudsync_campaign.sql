CREATE PROC [dbo].[proc_d_crmcloudsync_campaign] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_campaign)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_campaign_insert') is not null drop table #p_crmcloudsync_campaign_insert
create table dbo.#p_crmcloudsync_campaign_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_campaign.p_crmcloudsync_campaign_id,
       p_crmcloudsync_campaign.bk_hash
  from dbo.p_crmcloudsync_campaign
 where p_crmcloudsync_campaign.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_campaign.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_campaign.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_campaign.bk_hash,
       p_crmcloudsync_campaign.bk_hash dim_crm_campaign_key,
       p_crmcloudsync_campaign.campaign_id campaign_id,
       case when p_crmcloudsync_campaign.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_campaign.bk_hash
           when l_crmcloudsync_campaign.created_by is null then '-998'
       	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_campaign.created_by as varchar(36)),'z#@$k%&P'))),2)
        end created_by_dim_crm_system_user_key,
       case when p_crmcloudsync_campaign.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_campaign.bk_hash
          when s_crmcloudsync_campaign.created_on is null then '-998'
       else convert(varchar, s_crmcloudsync_campaign.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_campaign.bk_hash in ('-997','-998','-999') then p_crmcloudsync_campaign.bk_hash
       when s_crmcloudsync_campaign.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_campaign.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_campaign.created_on created_on,
       isnull(s_crmcloudsync_campaign.description,'') description,
       s_crmcloudsync_campaign.ltf_expiration_type ltf_expiration_type,
       s_crmcloudsync_campaign.ltf_issuance_method ltf_issuance_method,
       isnull(l_crmcloudsync_campaign.ltf_job_id ,'-998') ltf_job_id,
       s_crmcloudsync_campaign.ltf_member_referral ltf_member_referral,
       s_crmcloudsync_campaign.ltf_pass_days ltf_pass_days,
       s_crmcloudsync_campaign.ltf_restricted_by_policy ltf_restricted_by_policy,
       case when s_crmcloudsync_campaign.ltf_restricted_by_policy = 1 then 'Y' else 'N' end ltf_restricted_by_policy_flag,
       s_crmcloudsync_campaign.ltf_reward_club ltf_reward_club,
       s_crmcloudsync_campaign.ltf_reward_lt_bucks ltf_reward_lt_bucks,
       s_crmcloudsync_campaign.ltf_reward_type ltf_reward_type,
       s_crmcloudsync_campaign.ltf_reward_wait_days ltf_reward_wait_days,
       s_crmcloudsync_campaign.ltf_targeted_prospects ltf_targeted_prospects,
       case when p_crmcloudsync_campaign.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_campaign.bk_hash
           when l_crmcloudsync_campaign.modified_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_campaign.modified_by as varchar(36)),'z#@$k%&P'))),2)
        end modified_by_dim_crm_system_user_key,
       case when p_crmcloudsync_campaign.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_campaign.bk_hash
          when s_crmcloudsync_campaign.modified_on is null then '-998'
       else convert(varchar, s_crmcloudsync_campaign.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_campaign.bk_hash in ('-997','-998','-999') then p_crmcloudsync_campaign.bk_hash
       when s_crmcloudsync_campaign.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_campaign.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_campaign.modified_on modified_on,
       s_crmcloudsync_campaign.name name,
       s_crmcloudsync_campaign.proposed_end proposed_end,
       case when p_crmcloudsync_campaign.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_campaign.bk_hash
          when s_crmcloudsync_campaign.proposed_end is null then '-998'
       else convert(varchar, s_crmcloudsync_campaign.proposed_end, 112)    end proposed_end_dim_date_key,
       case when p_crmcloudsync_campaign.bk_hash in ('-997','-998','-999') then p_crmcloudsync_campaign.bk_hash
       when s_crmcloudsync_campaign.proposed_end is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_campaign.proposed_end,114), 1, 5),':','') end proposed_end_dim_time_key,
       s_crmcloudsync_campaign.proposed_start proposed_start,
       case when p_crmcloudsync_campaign.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_campaign.bk_hash
          when s_crmcloudsync_campaign.proposed_start is null then '-998'
       else convert(varchar, s_crmcloudsync_campaign.proposed_start, 112)    end proposed_start_dim_date_key,
       case when p_crmcloudsync_campaign.bk_hash in ('-997','-998','-999') then p_crmcloudsync_campaign.bk_hash
       when s_crmcloudsync_campaign.proposed_start is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_campaign.proposed_start,114), 1, 5),':','') end proposed_start_dim_time_key,
       s_crmcloudsync_campaign.state_code state_code,
       s_crmcloudsync_campaign.state_code_name state_code_name,
       s_crmcloudsync_campaign.status_code status_code,
       s_crmcloudsync_campaign.status_code_name status_code_name,
       s_crmcloudsync_campaign.type_code type_code,
       s_crmcloudsync_campaign.type_code_name type_code_name,
       isnull(h_crmcloudsync_campaign.dv_deleted,0) dv_deleted,
       p_crmcloudsync_campaign.p_crmcloudsync_campaign_id,
       p_crmcloudsync_campaign.dv_batch_id,
       p_crmcloudsync_campaign.dv_load_date_time,
       p_crmcloudsync_campaign.dv_load_end_date_time
  from dbo.h_crmcloudsync_campaign
  join dbo.p_crmcloudsync_campaign
    on h_crmcloudsync_campaign.bk_hash = p_crmcloudsync_campaign.bk_hash
  join #p_crmcloudsync_campaign_insert
    on p_crmcloudsync_campaign.bk_hash = #p_crmcloudsync_campaign_insert.bk_hash
   and p_crmcloudsync_campaign.p_crmcloudsync_campaign_id = #p_crmcloudsync_campaign_insert.p_crmcloudsync_campaign_id
  join dbo.l_crmcloudsync_campaign
    on p_crmcloudsync_campaign.bk_hash = l_crmcloudsync_campaign.bk_hash
   and p_crmcloudsync_campaign.l_crmcloudsync_campaign_id = l_crmcloudsync_campaign.l_crmcloudsync_campaign_id
  join dbo.s_crmcloudsync_campaign
    on p_crmcloudsync_campaign.bk_hash = s_crmcloudsync_campaign.bk_hash
   and p_crmcloudsync_campaign.s_crmcloudsync_campaign_id = s_crmcloudsync_campaign.s_crmcloudsync_campaign_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_campaign
   where d_crmcloudsync_campaign.bk_hash in (select bk_hash from #p_crmcloudsync_campaign_insert)

  insert dbo.d_crmcloudsync_campaign(
             bk_hash,
             dim_crm_campaign_key,
             campaign_id,
             created_by_dim_crm_system_user_key,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             description,
             ltf_expiration_type,
             ltf_issuance_method,
             ltf_job_id,
             ltf_member_referral,
             ltf_pass_days,
             ltf_restricted_by_policy,
             ltf_restricted_by_policy_flag,
             ltf_reward_club,
             ltf_reward_lt_bucks,
             ltf_reward_type,
             ltf_reward_wait_days,
             ltf_targeted_prospects,
             modified_by_dim_crm_system_user_key,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             name,
             proposed_end,
             proposed_end_dim_date_key,
             proposed_end_dim_time_key,
             proposed_start,
             proposed_start_dim_date_key,
             proposed_start_dim_time_key,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             type_code,
             type_code_name,
             deleted_flag,
             p_crmcloudsync_campaign_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_campaign_key,
         campaign_id,
         created_by_dim_crm_system_user_key,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         description,
         ltf_expiration_type,
         ltf_issuance_method,
         ltf_job_id,
         ltf_member_referral,
         ltf_pass_days,
         ltf_restricted_by_policy,
         ltf_restricted_by_policy_flag,
         ltf_reward_club,
         ltf_reward_lt_bucks,
         ltf_reward_type,
         ltf_reward_wait_days,
         ltf_targeted_prospects,
         modified_by_dim_crm_system_user_key,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         name,
         proposed_end,
         proposed_end_dim_date_key,
         proposed_end_dim_time_key,
         proposed_start,
         proposed_start_dim_date_key,
         proposed_start_dim_time_key,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         type_code,
         type_code_name,
         dv_deleted,
         p_crmcloudsync_campaign_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_campaign)
--Done!
end
