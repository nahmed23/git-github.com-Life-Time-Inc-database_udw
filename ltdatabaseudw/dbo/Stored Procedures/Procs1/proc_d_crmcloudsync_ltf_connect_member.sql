CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_connect_member] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_connect_member)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_connect_member_insert') is not null drop table #p_crmcloudsync_ltf_connect_member_insert
create table dbo.#p_crmcloudsync_ltf_connect_member_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_connect_member.p_crmcloudsync_ltf_connect_member_id,
       p_crmcloudsync_ltf_connect_member.bk_hash
  from dbo.p_crmcloudsync_ltf_connect_member
 where p_crmcloudsync_ltf_connect_member.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_connect_member.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_connect_member.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_connect_member.bk_hash,
       p_crmcloudsync_ltf_connect_member.bk_hash dim_crm_ltf_connect_member_key,
       l_crmcloudsync_ltf_connect_member.ltf_connect_member_id ltf_connect_member_id,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_connect_member.bk_hash 
    when l_crmcloudsync_ltf_connect_member.created_by is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_connect_member.created_by as varchar(36)),'z#@$k%&P'))),2) end  created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_ltf_connect_member.created_by_name,'') created_by_name,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_connect_member.bk_hash
           when s_crmcloudsync_ltf_connect_member.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_connect_member.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_connect_member.bk_hash
       when s_crmcloudsync_ltf_connect_member.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_connect_member.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_ltf_connect_member.created_on created_on,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_connect_member.bk_hash 
    when l_crmcloudsync_ltf_connect_member.ltf_subscriber_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_connect_member.ltf_subscriber_id as varchar(36)),'z#@$k%&P'))),2) end  dim_crm_ltf_subscriber_key,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_ltf_connect_member.bk_hash 
    when l_crmcloudsync_ltf_connect_member.ltf_opportunity_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_connect_member.ltf_opportunity_id as varchar(36)),'z#@$k%&P'))),2) end  dim_crm_opportunity_key,
       s_crmcloudsync_ltf_connect_member.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_connect_member.bk_hash
           when s_crmcloudsync_ltf_connect_member.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_ltf_connect_member.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_connect_member.bk_hash
       when s_crmcloudsync_ltf_connect_member.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_connect_member.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       s_crmcloudsync_ltf_connect_member.ltf_move_it_scheduled_date ltf_move_it_scheduled_date,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_connect_member.bk_hash
           when s_crmcloudsync_ltf_connect_member.ltf_move_it_scheduled_date is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_connect_member.ltf_move_it_scheduled_date, 112)    end ltf_move_it_scheduled_dim_date_key,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_connect_member.bk_hash
       when s_crmcloudsync_ltf_connect_member.ltf_move_it_scheduled_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_connect_member.ltf_move_it_scheduled_date,114), 1, 5),':','') end ltf_move_it_scheduled_dim_time_key,
       isnull(s_crmcloudsync_ltf_connect_member.ltf_move_it_scheduled_with,'') ltf_move_it_scheduled_with,
       isnull(s_crmcloudsync_ltf_connect_member.ltf_profile_notes,'') ltf_profile_notes,
       s_crmcloudsync_ltf_connect_member.ltf_programs_of_interest ltf_programs_of_interest,
       isnull(s_crmcloudsync_ltf_connect_member.ltf_programs_of_interest_name,'') ltf_programs_of_interest_name,
       isnull(s_crmcloudsync_ltf_connect_member.ltf_subscriber_id_name,'') ltf_subscriber_id_name,
       s_crmcloudsync_ltf_connect_member.ltf_want_to_do ltf_want_to_do,
       isnull(s_crmcloudsync_ltf_connect_member.ltf_want_to_do_name,'') ltf_want_to_do_name,
       isnull(s_crmcloudsync_ltf_connect_member.ltf_who_met_with,'') ltf_who_met_with,
       s_crmcloudsync_ltf_connect_member.ltf_why_want_to_do ltf_why_want_to_do,
       isnull(s_crmcloudsync_ltf_connect_member.ltf_why_want_to_do_name,'') ltf_why_want_to_do_name,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_connect_member.bk_hash
           when s_crmcloudsync_ltf_connect_member.modified_on is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_connect_member.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_connect_member.bk_hash
       when s_crmcloudsync_ltf_connect_member.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_connect_member.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_ltf_connect_member.modified_on modified_on,
       s_crmcloudsync_ltf_connect_member.state_code state_code,
       s_crmcloudsync_ltf_connect_member.status_code status_code,
       s_crmcloudsync_ltf_connect_member.updated_date_time updated_date_time,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_ltf_connect_member.bk_hash
           when s_crmcloudsync_ltf_connect_member.updated_date_time is null then '-998'
        else convert(varchar,s_crmcloudsync_ltf_connect_member.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_ltf_connect_member.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_connect_member.bk_hash
       when s_crmcloudsync_ltf_connect_member.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_ltf_connect_member.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_crmcloudsync_ltf_connect_member.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_connect_member.p_crmcloudsync_ltf_connect_member_id,
       p_crmcloudsync_ltf_connect_member.dv_batch_id,
       p_crmcloudsync_ltf_connect_member.dv_load_date_time,
       p_crmcloudsync_ltf_connect_member.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_connect_member
  join dbo.p_crmcloudsync_ltf_connect_member
    on h_crmcloudsync_ltf_connect_member.bk_hash = p_crmcloudsync_ltf_connect_member.bk_hash
  join #p_crmcloudsync_ltf_connect_member_insert
    on p_crmcloudsync_ltf_connect_member.bk_hash = #p_crmcloudsync_ltf_connect_member_insert.bk_hash
   and p_crmcloudsync_ltf_connect_member.p_crmcloudsync_ltf_connect_member_id = #p_crmcloudsync_ltf_connect_member_insert.p_crmcloudsync_ltf_connect_member_id
  join dbo.l_crmcloudsync_ltf_connect_member
    on p_crmcloudsync_ltf_connect_member.bk_hash = l_crmcloudsync_ltf_connect_member.bk_hash
   and p_crmcloudsync_ltf_connect_member.l_crmcloudsync_ltf_connect_member_id = l_crmcloudsync_ltf_connect_member.l_crmcloudsync_ltf_connect_member_id
  join dbo.s_crmcloudsync_ltf_connect_member
    on p_crmcloudsync_ltf_connect_member.bk_hash = s_crmcloudsync_ltf_connect_member.bk_hash
   and p_crmcloudsync_ltf_connect_member.s_crmcloudsync_ltf_connect_member_id = s_crmcloudsync_ltf_connect_member.s_crmcloudsync_ltf_connect_member_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_connect_member
   where d_crmcloudsync_ltf_connect_member.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_connect_member_insert)

  insert dbo.d_crmcloudsync_ltf_connect_member(
             bk_hash,
             dim_crm_ltf_connect_member_key,
             ltf_connect_member_id,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             dim_crm_ltf_subscriber_key,
             dim_crm_opportunity_key,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             ltf_move_it_scheduled_date,
             ltf_move_it_scheduled_dim_date_key,
             ltf_move_it_scheduled_dim_time_key,
             ltf_move_it_scheduled_with,
             ltf_profile_notes,
             ltf_programs_of_interest,
             ltf_programs_of_interest_name,
             ltf_subscriber_id_name,
             ltf_want_to_do,
             ltf_want_to_do_name,
             ltf_who_met_with,
             ltf_why_want_to_do,
             ltf_why_want_to_do_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             state_code,
             status_code,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_crmcloudsync_ltf_connect_member_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_connect_member_key,
         ltf_connect_member_id,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         dim_crm_ltf_subscriber_key,
         dim_crm_opportunity_key,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         ltf_move_it_scheduled_date,
         ltf_move_it_scheduled_dim_date_key,
         ltf_move_it_scheduled_dim_time_key,
         ltf_move_it_scheduled_with,
         ltf_profile_notes,
         ltf_programs_of_interest,
         ltf_programs_of_interest_name,
         ltf_subscriber_id_name,
         ltf_want_to_do,
         ltf_want_to_do_name,
         ltf_who_met_with,
         ltf_why_want_to_do,
         ltf_why_want_to_do_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         state_code,
         status_code,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_crmcloudsync_ltf_connect_member_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_connect_member)
--Done!
end
