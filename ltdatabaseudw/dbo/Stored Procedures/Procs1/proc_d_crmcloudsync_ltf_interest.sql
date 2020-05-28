CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_interest] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_interest)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_interest_insert') is not null drop table #p_crmcloudsync_ltf_interest_insert
create table dbo.#p_crmcloudsync_ltf_interest_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_interest.p_crmcloudsync_ltf_interest_id,
       p_crmcloudsync_ltf_interest.bk_hash
  from dbo.p_crmcloudsync_ltf_interest
 where p_crmcloudsync_ltf_interest.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_interest.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_interest.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_interest.bk_hash,
       p_crmcloudsync_ltf_interest.bk_hash dim_crm_ltf_interest_key,
       p_crmcloudsync_ltf_interest.ltf_interest_id ltf_interest_id,
       case when p_crmcloudsync_ltf_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_interest.bk_hash 
		     when l_crmcloudsync_ltf_interest.created_by is null then '-998' 
			      else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_interest.created_by as varchar(36)),'z#@$k%&P'))),2) end created_by_dim_crm_system_user_key,
       s_crmcloudsync_ltf_interest.created_by_name created_by_name,
       s_crmcloudsync_ltf_interest.created_by_yomi_name created_by_yomi_name,
       case when p_crmcloudsync_ltf_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_interest.bk_hash
           when s_crmcloudsync_ltf_interest.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_interest.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_interest.bk_hash
       when s_crmcloudsync_ltf_interest.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_interest.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_interest.created_on created_on,
       case when p_crmcloudsync_ltf_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_interest.bk_hash 
     when l_crmcloudsync_ltf_interest.created_on_behalf_by is null then '-998'
	    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_interest.created_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end created_on_behalf_by_dim_crm_system_user_key,
       s_crmcloudsync_ltf_interest.created_on_behalf_by_name created_on_behalf_by_name,
       s_crmcloudsync_ltf_interest.created_on_behalf_by_yomi_name created_on_behalf_by_yomi_name,
       s_crmcloudsync_ltf_interest.import_sequence_number import_sequence_number,
       s_crmcloudsync_ltf_interest.insert_user insert_user,
       s_crmcloudsync_ltf_interest.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_ltf_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_interest.bk_hash
           when s_crmcloudsync_ltf_interest.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_interest.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_ltf_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_interest.bk_hash
       when s_crmcloudsync_ltf_interest.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_interest.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       case when s_crmcloudsync_ltf_interest.ltf_juniors_only = 1  then 'Y' else 'N' end juniors_only_flag,
       s_crmcloudsync_ltf_interest.ltf_club_element_1 ltf_club_element_1,
       s_crmcloudsync_ltf_interest.ltf_club_element_2 ltf_club_element_2,
       s_crmcloudsync_ltf_interest.ltf_club_element_3 ltf_club_element_3,
       s_crmcloudsync_ltf_interest.ltf_interest_group ltf_interest_group,
       s_crmcloudsync_ltf_interest.ltf_juniors_only ltf_juniors_only,
       l_crmcloudsync_ltf_interest.ltf_mms_id ltf_mms_id,
       s_crmcloudsync_ltf_interest.ltf_name ltf_name,
       s_crmcloudsync_ltf_interest.ltf_shortlist ltf_shortlist,
       case when p_crmcloudsync_ltf_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_interest.bk_hash 
		     when l_crmcloudsync_ltf_interest.modified_by is null then '-998' 
			      else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_interest.modified_by as varchar(36)),'z#@$k%&P'))),2) end modified_by_dim_crm_system_user_key,
       s_crmcloudsync_ltf_interest.modified_by_name modified_by_name,
       s_crmcloudsync_ltf_interest.modified_by_yomi_name modified_by_yomi_name,
       case when p_crmcloudsync_ltf_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_interest.bk_hash
           when s_crmcloudsync_ltf_interest.modified_on is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_interest.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_interest.bk_hash
       when s_crmcloudsync_ltf_interest.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_interest.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_interest.modified_on modified_on,
       case when p_crmcloudsync_ltf_interest.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_interest.bk_hash 
		     when l_crmcloudsync_ltf_interest.modified_on_behalf_by is null then '-998' 
			      else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_interest.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end modified_on_behalf_by_dim_crm_system_user_key,
       s_crmcloudsync_ltf_interest.modified_on_behalf_by_name modified_on_behalf_by_name,
       s_crmcloudsync_ltf_interest.modified_on_behalf_by_yomi_name modified_on_behalf_by_yomi_name,
       l_crmcloudsync_ltf_interest.organization_id organization_id,
       s_crmcloudsync_ltf_interest.organization_id_name organization_id_name,
       case when p_crmcloudsync_ltf_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_interest.bk_hash
           when s_crmcloudsync_ltf_interest.overridden_created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_interest.overridden_created_on, 112)    end overridden_created_dim_date_key,
       case when p_crmcloudsync_ltf_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_interest.bk_hash
       when s_crmcloudsync_ltf_interest.overridden_created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_interest.overridden_created_on,114), 1, 5),':','') end overridden_created_dim_time_key,
       s_crmcloudsync_ltf_interest.overridden_created_on overridden_created_on,
       case when s_crmcloudsync_ltf_interest.ltf_shortlist = 1  then 'Y' else 'N' end shortlist_flag,
       l_crmcloudsync_ltf_interest.state_code state_code,
       s_crmcloudsync_ltf_interest.state_code_name state_code_name,
       l_crmcloudsync_ltf_interest.status_code status_code,
       s_crmcloudsync_ltf_interest.status_code_name status_code_name,
       s_crmcloudsync_ltf_interest.time_zone_rule_version_number time_zone_rule_version_number,
       s_crmcloudsync_ltf_interest.update_user update_user,
       s_crmcloudsync_ltf_interest.updated_date_time updated_date_time,
       case when p_crmcloudsync_ltf_interest.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_interest.bk_hash
           when s_crmcloudsync_ltf_interest.updated_date_time is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_interest.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_ltf_interest.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_interest.bk_hash
       when s_crmcloudsync_ltf_interest.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_interest.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_crmcloudsync_ltf_interest.utc_conversion_time_zone_code utc_conversion_time_zone_code,
       l_crmcloudsync_ltf_interest.version_number version_number,
       isnull(h_crmcloudsync_ltf_interest.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_interest.p_crmcloudsync_ltf_interest_id,
       p_crmcloudsync_ltf_interest.dv_batch_id,
       p_crmcloudsync_ltf_interest.dv_load_date_time,
       p_crmcloudsync_ltf_interest.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_interest
  join dbo.p_crmcloudsync_ltf_interest
    on h_crmcloudsync_ltf_interest.bk_hash = p_crmcloudsync_ltf_interest.bk_hash
  join #p_crmcloudsync_ltf_interest_insert
    on p_crmcloudsync_ltf_interest.bk_hash = #p_crmcloudsync_ltf_interest_insert.bk_hash
   and p_crmcloudsync_ltf_interest.p_crmcloudsync_ltf_interest_id = #p_crmcloudsync_ltf_interest_insert.p_crmcloudsync_ltf_interest_id
  join dbo.l_crmcloudsync_ltf_interest
    on p_crmcloudsync_ltf_interest.bk_hash = l_crmcloudsync_ltf_interest.bk_hash
   and p_crmcloudsync_ltf_interest.l_crmcloudsync_ltf_interest_id = l_crmcloudsync_ltf_interest.l_crmcloudsync_ltf_interest_id
  join dbo.s_crmcloudsync_ltf_interest
    on p_crmcloudsync_ltf_interest.bk_hash = s_crmcloudsync_ltf_interest.bk_hash
   and p_crmcloudsync_ltf_interest.s_crmcloudsync_ltf_interest_id = s_crmcloudsync_ltf_interest.s_crmcloudsync_ltf_interest_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_interest
   where d_crmcloudsync_ltf_interest.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_interest_insert)

  insert dbo.d_crmcloudsync_ltf_interest(
             bk_hash,
             dim_crm_ltf_interest_key,
             ltf_interest_id,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_by_yomi_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             created_on_behalf_by_dim_crm_system_user_key,
             created_on_behalf_by_name,
             created_on_behalf_by_yomi_name,
             import_sequence_number,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             juniors_only_flag,
             ltf_club_element_1,
             ltf_club_element_2,
             ltf_club_element_3,
             ltf_interest_group,
             ltf_juniors_only,
             ltf_mms_id,
             ltf_name,
             ltf_shortlist,
             modified_by_dim_crm_system_user_key,
             modified_by_name,
             modified_by_yomi_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             modified_on_behalf_by_dim_crm_system_user_key,
             modified_on_behalf_by_name,
             modified_on_behalf_by_yomi_name,
             organization_id,
             organization_id_name,
             overridden_created_dim_date_key,
             overridden_created_dim_time_key,
             overridden_created_on,
             shortlist_flag,
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
             version_number,
             deleted_flag,
             p_crmcloudsync_ltf_interest_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_interest_key,
         ltf_interest_id,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_by_yomi_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         created_on_behalf_by_dim_crm_system_user_key,
         created_on_behalf_by_name,
         created_on_behalf_by_yomi_name,
         import_sequence_number,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         juniors_only_flag,
         ltf_club_element_1,
         ltf_club_element_2,
         ltf_club_element_3,
         ltf_interest_group,
         ltf_juniors_only,
         ltf_mms_id,
         ltf_name,
         ltf_shortlist,
         modified_by_dim_crm_system_user_key,
         modified_by_name,
         modified_by_yomi_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         modified_on_behalf_by_dim_crm_system_user_key,
         modified_on_behalf_by_name,
         modified_on_behalf_by_yomi_name,
         organization_id,
         organization_id_name,
         overridden_created_dim_date_key,
         overridden_created_dim_time_key,
         overridden_created_on,
         shortlist_flag,
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
         version_number,
         dv_deleted,
         p_crmcloudsync_ltf_interest_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_interest)
--Done!
end
