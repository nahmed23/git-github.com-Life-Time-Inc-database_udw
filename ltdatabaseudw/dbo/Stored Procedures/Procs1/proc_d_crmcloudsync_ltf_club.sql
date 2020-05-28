CREATE PROC [dbo].[proc_d_crmcloudsync_ltf_club] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_club)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_ltf_club_insert') is not null drop table #p_crmcloudsync_ltf_club_insert
create table dbo.#p_crmcloudsync_ltf_club_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_club.p_crmcloudsync_ltf_club_id,
       p_crmcloudsync_ltf_club.bk_hash
  from dbo.p_crmcloudsync_ltf_club
 where p_crmcloudsync_ltf_club.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_ltf_club.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_ltf_club.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_ltf_club.bk_hash,
       p_crmcloudsync_ltf_club.bk_hash dim_crm_ltf_club_key,
       p_crmcloudsync_ltf_club.ltf_club_id ltf_club_id,
       s_crmcloudsync_ltf_club.ltf_address_1_line_1 address_line_1,
       case when p_crmcloudsync_ltf_club.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_club.bk_hash
            when l_crmcloudsync_ltf_club.ltf_area_director is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_club.ltf_area_director as varchar(36)),'z#@$k%&P'))),2)
        end area_director_dim_crm_system_user_key,
       s_crmcloudsync_ltf_club.ltf_address_1_city city,
       l_crmcloudsync_ltf_club.ltf_mms_club_id club_id,
       case when p_crmcloudsync_ltf_club.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_club.bk_hash
            when l_crmcloudsync_ltf_club.ltf_club_regional_manager is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_club.ltf_club_regional_manager as varchar(36)),'z#@$k%&P'))),2)
        end club_regional_manager_dim_crm_system_user_key,
       s_crmcloudsync_ltf_club.ltf_address_1_country country,
       case when p_crmcloudsync_ltf_club.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_club.bk_hash
            when l_crmcloudsync_ltf_club.ltf_mms_club_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_crmcloudsync_ltf_club.ltf_mms_club_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       case when p_crmcloudsync_ltf_club.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_club.bk_hash
            when l_crmcloudsync_ltf_club.ltf_club_team_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_club.ltf_club_team_id as varchar(36)),'z#@$k%&P'))),2)
        end dim_crm_team_key,
       s_crmcloudsync_ltf_club.ltf_five_letter_club_code five_letter_club_code,
       s_crmcloudsync_ltf_club.ltf_four_letter_club_code four_letter_club_code,
       case when p_crmcloudsync_ltf_club.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_club.bk_hash
            when l_crmcloudsync_ltf_club.ltf_general_manager is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_club.ltf_general_manager as varchar(36)),'z#@$k%&P'))),2)
        end general_manager_dim_crm_system_user_key,
       s_crmcloudsync_ltf_club.ltf_lt_work_email ltf_lt_work_email,
       l_crmcloudsync_ltf_club.ltf_web_specialist_team ltf_web_specialist_team,
       s_crmcloudsync_ltf_club.ltf_club_marketing_name marketing_name,
       s_crmcloudsync_ltf_club.ltf_address_1_postal_code postal_code,
       case when p_crmcloudsync_ltf_club.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_club.bk_hash
            when l_crmcloudsync_ltf_club.ltf_regional_sales_lead is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_club.ltf_regional_sales_lead as varchar(36)),'z#@$k%&P'))),2)
        end regional_sales_lead_dim_crm_system_user_key,
       case when p_crmcloudsync_ltf_club.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_club.bk_hash
            when l_crmcloudsync_ltf_club.ltf_regional_vice_president is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_club.ltf_regional_vice_president as varchar(36)),'z#@$k%&P'))),2)
        end regional_vice_president_dim_crm_system_user_key,
       s_crmcloudsync_ltf_club.ltf_address_1_state_or_province state_or_province,
       s_crmcloudsync_ltf_club.status_code status_code,
       s_crmcloudsync_ltf_club.ltf_address_1_telephone_1 telephone,
       case when p_crmcloudsync_ltf_club.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_club.bk_hash
            when l_crmcloudsync_ltf_club.ltf_web_specialist_team is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_club.ltf_web_specialist_team as varchar(36)),'z#@$k%&P'))),2)
        end web_specialist_dim_crm_team_key,
       isnull(h_crmcloudsync_ltf_club.dv_deleted,0) dv_deleted,
       p_crmcloudsync_ltf_club.p_crmcloudsync_ltf_club_id,
       p_crmcloudsync_ltf_club.dv_batch_id,
       p_crmcloudsync_ltf_club.dv_load_date_time,
       p_crmcloudsync_ltf_club.dv_load_end_date_time
  from dbo.h_crmcloudsync_ltf_club
  join dbo.p_crmcloudsync_ltf_club
    on h_crmcloudsync_ltf_club.bk_hash = p_crmcloudsync_ltf_club.bk_hash
  join #p_crmcloudsync_ltf_club_insert
    on p_crmcloudsync_ltf_club.bk_hash = #p_crmcloudsync_ltf_club_insert.bk_hash
   and p_crmcloudsync_ltf_club.p_crmcloudsync_ltf_club_id = #p_crmcloudsync_ltf_club_insert.p_crmcloudsync_ltf_club_id
  join dbo.l_crmcloudsync_ltf_club
    on p_crmcloudsync_ltf_club.bk_hash = l_crmcloudsync_ltf_club.bk_hash
   and p_crmcloudsync_ltf_club.l_crmcloudsync_ltf_club_id = l_crmcloudsync_ltf_club.l_crmcloudsync_ltf_club_id
  join dbo.s_crmcloudsync_ltf_club
    on p_crmcloudsync_ltf_club.bk_hash = s_crmcloudsync_ltf_club.bk_hash
   and p_crmcloudsync_ltf_club.s_crmcloudsync_ltf_club_id = s_crmcloudsync_ltf_club.s_crmcloudsync_ltf_club_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_ltf_club
   where d_crmcloudsync_ltf_club.bk_hash in (select bk_hash from #p_crmcloudsync_ltf_club_insert)

  insert dbo.d_crmcloudsync_ltf_club(
             bk_hash,
             dim_crm_ltf_club_key,
             ltf_club_id,
             address_line_1,
             area_director_dim_crm_system_user_key,
             city,
             club_id,
             club_regional_manager_dim_crm_system_user_key,
             country,
             dim_club_key,
             dim_crm_team_key,
             five_letter_club_code,
             four_letter_club_code,
             general_manager_dim_crm_system_user_key,
             ltf_lt_work_email,
             ltf_web_specialist_team,
             marketing_name,
             postal_code,
             regional_sales_lead_dim_crm_system_user_key,
             regional_vice_president_dim_crm_system_user_key,
             state_or_province,
             status_code,
             telephone,
             web_specialist_dim_crm_team_key,
             deleted_flag,
             p_crmcloudsync_ltf_club_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_ltf_club_key,
         ltf_club_id,
         address_line_1,
         area_director_dim_crm_system_user_key,
         city,
         club_id,
         club_regional_manager_dim_crm_system_user_key,
         country,
         dim_club_key,
         dim_crm_team_key,
         five_letter_club_code,
         four_letter_club_code,
         general_manager_dim_crm_system_user_key,
         ltf_lt_work_email,
         ltf_web_specialist_team,
         marketing_name,
         postal_code,
         regional_sales_lead_dim_crm_system_user_key,
         regional_vice_president_dim_crm_system_user_key,
         state_or_province,
         status_code,
         telephone,
         web_specialist_dim_crm_team_key,
         dv_deleted,
         p_crmcloudsync_ltf_club_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_ltf_club)
--Done!
end
