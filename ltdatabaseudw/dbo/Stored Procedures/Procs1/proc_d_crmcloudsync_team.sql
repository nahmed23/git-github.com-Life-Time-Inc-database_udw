CREATE PROC [dbo].[proc_d_crmcloudsync_team] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_team)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_team_insert') is not null drop table #p_crmcloudsync_team_insert
create table dbo.#p_crmcloudsync_team_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_team.p_crmcloudsync_team_id,
       p_crmcloudsync_team.bk_hash
  from dbo.p_crmcloudsync_team
 where p_crmcloudsync_team.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_team.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_team.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_team.bk_hash,
       p_crmcloudsync_team.bk_hash dim_crm_team_key,
       p_crmcloudsync_team.team_id team_id,
       isnull(s_crmcloudsync_team.email_address,'') email_address,
       isnull(s_crmcloudsync_team.ltf_telephone_1,'') ltf_telephone_1,
       isnull(s_crmcloudsync_team.name,'') name,
       isnull(h_crmcloudsync_team.dv_deleted,0) dv_deleted,
       p_crmcloudsync_team.p_crmcloudsync_team_id,
       p_crmcloudsync_team.dv_batch_id,
       p_crmcloudsync_team.dv_load_date_time,
       p_crmcloudsync_team.dv_load_end_date_time
  from dbo.h_crmcloudsync_team
  join dbo.p_crmcloudsync_team
    on h_crmcloudsync_team.bk_hash = p_crmcloudsync_team.bk_hash
  join #p_crmcloudsync_team_insert
    on p_crmcloudsync_team.bk_hash = #p_crmcloudsync_team_insert.bk_hash
   and p_crmcloudsync_team.p_crmcloudsync_team_id = #p_crmcloudsync_team_insert.p_crmcloudsync_team_id
  join dbo.l_crmcloudsync_team
    on p_crmcloudsync_team.bk_hash = l_crmcloudsync_team.bk_hash
   and p_crmcloudsync_team.l_crmcloudsync_team_id = l_crmcloudsync_team.l_crmcloudsync_team_id
  join dbo.s_crmcloudsync_team
    on p_crmcloudsync_team.bk_hash = s_crmcloudsync_team.bk_hash
   and p_crmcloudsync_team.s_crmcloudsync_team_id = s_crmcloudsync_team.s_crmcloudsync_team_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_team
   where d_crmcloudsync_team.bk_hash in (select bk_hash from #p_crmcloudsync_team_insert)

  insert dbo.d_crmcloudsync_team(
             bk_hash,
             dim_crm_team_key,
             team_id,
             email_address,
             ltf_telephone_1,
             name,
             deleted_flag,
             p_crmcloudsync_team_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_team_key,
         team_id,
         email_address,
         ltf_telephone_1,
         name,
         dv_deleted,
         p_crmcloudsync_team_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_team)
--Done!
end
