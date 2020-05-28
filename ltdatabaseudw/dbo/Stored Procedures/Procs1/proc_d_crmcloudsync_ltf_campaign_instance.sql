CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_campaign_instance] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_campaign_instance)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_campaign_instance_insert') is not null drop table #p_crmcloudsync_ltf_campaign_instance_insert
create table dbo.#p_crmcloudsync_ltf_campaign_instance_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_campaign_instance.p_crmcloudsync_ltf_campaign_instance_id,
       p_crmcloudsync_ltf_campaign_instance.bk_hash
  from dbo.p_crmcloudsync_ltf_campaign_instance
 where p_crmcloudsync_ltf_campaign_instance.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_campaign_instance.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_campaign_instance.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_campaign_instance.bk_hash,
       p_crmcloudsync_ltf_campaign_instance.bk_hash dim_crm_ltf_campaign_instance_key,
       p_crmcloudsync_ltf_campaign_instance.ltf_campaign_instance_id ltf_campaign_instance_id,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
           when l_crmcloudsync_ltf_campaign_instance.created_by is null then '-998'
       	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.created_by as varchar(36)),'z#@$k%&P'))),2)
        end created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.created_by_name,'') created_by_name,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
          when s_crmcloudsync_ltf_campaign_instance.created_on is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_campaign_instance.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
       when s_crmcloudsync_ltf_campaign_instance.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_campaign_instance.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_campaign_instance.created_on created_on,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
           when l_crmcloudsync_ltf_campaign_instance.created_on_behalf_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.created_on_behalf_by as varchar(36)),'z#@$k%&P'))),2)
        end created_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.created_on_behalf_by_name,'') created_on_behalf_by_name,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash      when l_crmcloudsync_ltf_campaign_instance.ltf_referring_member is null then '-998' 
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.ltf_referring_member as varchar(36)),'z#@$k%&P'))),2)   end  dim_crm_contact_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
           when l_crmcloudsync_ltf_campaign_instance.ltf_club is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.ltf_club as varchar(36)),'z#@$k%&P'))),2)
        end dim_crm_ltf_club_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash  
           when l_crmcloudsync_ltf_campaign_instance.ltf_referring_member_id is null then '-998'   
       	when (isnumeric(l_crmcloudsync_ltf_campaign_instance.ltf_referring_member_id) = 0 
           or	len(l_crmcloudsync_ltf_campaign_instance.ltf_referring_member_id) >=10 ) or
       	l_crmcloudsync_ltf_campaign_instance.ltf_referring_member_id not like '%[0-9]%' then '-999'   
       	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_crmcloudsync_ltf_campaign_instance.ltf_referring_member_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_member_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.insert_user,'') insert_user,
       s_crmcloudsync_ltf_campaign_instance.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
          when s_crmcloudsync_ltf_campaign_instance.inserted_date_time is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_campaign_instance.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
       when s_crmcloudsync_ltf_campaign_instance.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_campaign_instance.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
           when l_crmcloudsync_ltf_campaign_instance.ltf_campaign is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.ltf_campaign as varchar(36)),'z#@$k%&P'))),2)
        end ltf_campaign_dim_crm_campaign_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.ltf_campaign_name,'') ltf_campaign_name,
       isnull(s_crmcloudsync_ltf_campaign_instance.ltf_club_name,'') ltf_club_name,
       s_crmcloudsync_ltf_campaign_instance.ltf_connect_witham ltf_connect_witham,
       case when s_crmcloudsync_ltf_campaign_instance.ltf_connect_witham = 1 then 'Y' else 'N' end ltf_connect_witham_flag,
       isnull(s_crmcloudsync_ltf_campaign_instance.ltf_connect_witham_name,'') ltf_connect_witham_name,
       s_crmcloudsync_ltf_campaign_instance.ltf_expiration_date ltf_expiration_date,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
          when s_crmcloudsync_ltf_campaign_instance.ltf_expiration_date is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_campaign_instance.ltf_expiration_date, 112)    end ltf_expiration_dim_date_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
       when s_crmcloudsync_ltf_campaign_instance.ltf_expiration_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_campaign_instance.ltf_expiration_date,114), 1, 5),':','') end ltf_expiration_dim_time_key,
       s_crmcloudsync_ltf_campaign_instance.ltf_initial_use_date ltf_initial_use_date,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
          when s_crmcloudsync_ltf_campaign_instance.ltf_initial_use_date is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_campaign_instance.ltf_initial_use_date, 112)    end ltf_initial_use_dim_date_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
       when s_crmcloudsync_ltf_campaign_instance.ltf_initial_use_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_campaign_instance.ltf_initial_use_date,114), 1, 5),':','') end ltf_initial_use_dim_time_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
           when l_crmcloudsync_ltf_campaign_instance.ltf_issued_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.ltf_issued_by as varchar(36)),'z#@$k%&P'))),2)
        end ltf_issued_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.ltf_issued_by_name,'') ltf_issued_by_name,
       s_crmcloudsync_ltf_campaign_instance.ltf_issued_date ltf_issued_date,
       s_crmcloudsync_ltf_campaign_instance.ltf_issued_days ltf_issued_days,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
          when s_crmcloudsync_ltf_campaign_instance.ltf_issued_date is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_campaign_instance.ltf_issued_date, 112)    end ltf_issued_dim_date_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
       when s_crmcloudsync_ltf_campaign_instance.ltf_issued_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_campaign_instance.ltf_issued_date,114), 1, 5),':','') end ltf_issued_dim_time_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
           when l_crmcloudsync_ltf_campaign_instance.ltf_issuing_contact is null then '-998'
       	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.ltf_issuing_contact as varchar(36)),'z#@$k%&P'))),2) 
        end ltf_issuing_contact_dim_crm_contact_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.ltf_issuing_contact_name,'') ltf_issuing_contact_name,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
           when l_crmcloudsync_ltf_campaign_instance.ltf_issuing_lead is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.ltf_issuing_lead as varchar(36)),'z#@$k%&P'))),2) 
        end ltf_issuing_lead_dim_crm_lead_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.ltf_issuing_lead_name,'') ltf_issuing_lead_name,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
           when l_crmcloudsync_ltf_campaign_instance.ltf_issuing_opportunity is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.ltf_issuing_opportunity as varchar(36)),'z#@$k%&P'))),2)
        end ltf_issuing_opportunity_dim_crm_opportunity_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.ltf_issuing_opportunity_name,'') ltf_issuing_opportunity_name,
       isnull(s_crmcloudsync_ltf_campaign_instance.ltf_name,'') ltf_name,
       s_crmcloudsync_ltf_campaign_instance.ltf_pass_begin_date ltf_pass_begin_date,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
          when s_crmcloudsync_ltf_campaign_instance.ltf_pass_begin_date is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_campaign_instance.ltf_pass_begin_date, 112)    end ltf_pass_begin_dim_date_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
       when s_crmcloudsync_ltf_campaign_instance.ltf_pass_begin_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_campaign_instance.ltf_pass_begin_date,114), 1, 5),':','') end ltf_pass_begin_dim_time_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.ltf_qr_code,'') ltf_qr_code,
       l_crmcloudsync_ltf_campaign_instance.ltf_referring_corpacct_id ltf_referring_corpacct_id,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash      when l_crmcloudsync_ltf_campaign_instance.ltf_referring_member is null then '-998' else '-998' end  ltf_referring_member_dim_mms_member_key,
       s_crmcloudsync_ltf_campaign_instance.ltf_remaining_days ltf_remaining_days,
       l_crmcloudsync_ltf_campaign_instance.ltf_send_id ltf_send_id,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
           when l_crmcloudsync_ltf_campaign_instance.modified_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.modified_by as varchar(36)),'z#@$k%&P'))),2)
        end modified_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.modified_by_name,'') modified_by_name,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
          when s_crmcloudsync_ltf_campaign_instance.modified_on is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_campaign_instance.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
       when s_crmcloudsync_ltf_campaign_instance.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_campaign_instance.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_campaign_instance.modified_on modified_on,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
           when l_crmcloudsync_ltf_campaign_instance.modified_on_behalf_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_campaign_instance.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2)
        end modified_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_campaign_instance.modified_on_behalf_by_name,'') modified_on_behalf_by_name,
       s_crmcloudsync_ltf_campaign_instance.state_code state_code,
       isnull(s_crmcloudsync_ltf_campaign_instance.state_code_name,'') state_code_name,
       s_crmcloudsync_ltf_campaign_instance.status_code status_code,
       isnull(s_crmcloudsync_ltf_campaign_instance.status_code_name,'') status_code_name,
       s_crmcloudsync_ltf_campaign_instance.time_zone_rule_version_number time_zone_rule_version_number,
       isnull(s_crmcloudsync_ltf_campaign_instance.update_user,'') update_user,
       s_crmcloudsync_ltf_campaign_instance.updated_date_time updated_date_time,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
          when s_crmcloudsync_ltf_campaign_instance.updated_date_time is null then '-998'
       else convert(varchar, s_crmcloudsync_ltf_campaign_instance.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_ltf_campaign_instance.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_campaign_instance.bk_hash
       when s_crmcloudsync_ltf_campaign_instance.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_campaign_instance.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_crmcloudsync_ltf_campaign_instance.utc_conversion_time_zone_code utc_conversion_time_zone_code,
       isnull(h_crmcloudsync_ltf_campaign_instance.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_campaign_instance.p_crmcloudsync_ltf_campaign_instance_id,
       p_crmcloudsync_ltf_campaign_instance.dv_batch_id,
       p_crmcloudsync_ltf_campaign_instance.dv_load_date_time,
       p_crmcloudsync_ltf_campaign_instance.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_campaign_instance
  join dbo.p_crmcloudsync_ltf_campaign_instance
    on h_crmcloudsync_ltf_campaign_instance.bk_hash = p_crmcloudsync_ltf_campaign_instance.bk_hash
  join #p_crmcloudsync_ltf_campaign_instance_insert
    on p_crmcloudsync_ltf_campaign_instance.bk_hash = #p_crmcloudsync_ltf_campaign_instance_insert.bk_hash
   and p_crmcloudsync_ltf_campaign_instance.p_crmcloudsync_ltf_campaign_instance_id = #p_crmcloudsync_ltf_campaign_instance_insert.p_crmcloudsync_ltf_campaign_instance_id
  join dbo.l_crmcloudsync_ltf_campaign_instance
    on p_crmcloudsync_ltf_campaign_instance.bk_hash = l_crmcloudsync_ltf_campaign_instance.bk_hash
   and p_crmcloudsync_ltf_campaign_instance.l_crmcloudsync_ltf_campaign_instance_id = l_crmcloudsync_ltf_campaign_instance.l_crmcloudsync_ltf_campaign_instance_id
  join dbo.s_crmcloudsync_ltf_campaign_instance
    on p_crmcloudsync_ltf_campaign_instance.bk_hash = s_crmcloudsync_ltf_campaign_instance.bk_hash
   and p_crmcloudsync_ltf_campaign_instance.s_crmcloudsync_ltf_campaign_instance_id = s_crmcloudsync_ltf_campaign_instance.s_crmcloudsync_ltf_campaign_instance_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_campaign_instance
   where d_crmcloudsync_ltf_campaign_instance.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_campaign_instance_insert)

  insert dbo.d_crmcloudsync_ltf_campaign_instance(
             bk_hash,
             dim_crm_ltf_campaign_instance_key,
             ltf_campaign_instance_id,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             created_on_behalf_by_dim_crm_system_user_key,
             created_on_behalf_by_name,
             dim_crm_contact_key,
             dim_crm_ltf_club_key,
             dim_mms_member_key,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             ltf_campaign_dim_crm_campaign_key,
             ltf_campaign_name,
             ltf_club_name,
             ltf_connect_witham,
             ltf_connect_witham_flag,
             ltf_connect_witham_name,
             ltf_expiration_date,
             ltf_expiration_dim_date_key,
             ltf_expiration_dim_time_key,
             ltf_initial_use_date,
             ltf_initial_use_dim_date_key,
             ltf_initial_use_dim_time_key,
             ltf_issued_by_dim_crm_system_user_key,
             ltf_issued_by_name,
             ltf_issued_date,
             ltf_issued_days,
             ltf_issued_dim_date_key,
             ltf_issued_dim_time_key,
             ltf_issuing_contact_dim_crm_contact_key,
             ltf_issuing_contact_name,
             ltf_issuing_lead_dim_crm_lead_key,
             ltf_issuing_lead_name,
             ltf_issuing_opportunity_dim_crm_opportunity_key,
             ltf_issuing_opportunity_name,
             ltf_name,
             ltf_pass_begin_date,
             ltf_pass_begin_dim_date_key,
             ltf_pass_begin_dim_time_key,
             ltf_qr_code,
             ltf_referring_corpacct_id,
             ltf_referring_member_dim_mms_member_key,
             ltf_remaining_days,
             ltf_send_id,
             modified_by_dim_crm_system_user_key,
             modified_by_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             modified_on_behalf_by_dim_crm_system_user_key,
             modified_on_behalf_by_name,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             time_zone_rule_version_number,
             update_user,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             utc_conversion_time_zone_code,
             deleted_flag,
             p_crmcloudsync_ltf_campaign_instance_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_campaign_instance_key,
         ltf_campaign_instance_id,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         created_on_behalf_by_dim_crm_system_user_key,
         created_on_behalf_by_name,
         dim_crm_contact_key,
         dim_crm_ltf_club_key,
         dim_mms_member_key,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         ltf_campaign_dim_crm_campaign_key,
         ltf_campaign_name,
         ltf_club_name,
         ltf_connect_witham,
         ltf_connect_witham_flag,
         ltf_connect_witham_name,
         ltf_expiration_date,
         ltf_expiration_dim_date_key,
         ltf_expiration_dim_time_key,
         ltf_initial_use_date,
         ltf_initial_use_dim_date_key,
         ltf_initial_use_dim_time_key,
         ltf_issued_by_dim_crm_system_user_key,
         ltf_issued_by_name,
         ltf_issued_date,
         ltf_issued_days,
         ltf_issued_dim_date_key,
         ltf_issued_dim_time_key,
         ltf_issuing_contact_dim_crm_contact_key,
         ltf_issuing_contact_name,
         ltf_issuing_lead_dim_crm_lead_key,
         ltf_issuing_lead_name,
         ltf_issuing_opportunity_dim_crm_opportunity_key,
         ltf_issuing_opportunity_name,
         ltf_name,
         ltf_pass_begin_date,
         ltf_pass_begin_dim_date_key,
         ltf_pass_begin_dim_time_key,
         ltf_qr_code,
         ltf_referring_corpacct_id,
         ltf_referring_member_dim_mms_member_key,
         ltf_remaining_days,
         ltf_send_id,
         modified_by_dim_crm_system_user_key,
         modified_by_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         modified_on_behalf_by_dim_crm_system_user_key,
         modified_on_behalf_by_name,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         time_zone_rule_version_number,
         update_user,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         utc_conversion_time_zone_code,
         dv_deleted,
         p_crmcloudsync_ltf_campaign_instance_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_campaign_instance)
--Done!
end
