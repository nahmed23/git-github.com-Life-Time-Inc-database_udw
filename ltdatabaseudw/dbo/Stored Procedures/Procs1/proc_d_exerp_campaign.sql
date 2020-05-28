CREATE PROC [dbo].[proc_d_exerp_campaign] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_campaign)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_campaign_insert') is not null drop table #p_exerp_campaign_insert
create table dbo.#p_exerp_campaign_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_campaign.p_exerp_campaign_id,
       p_exerp_campaign.bk_hash
  from dbo.p_exerp_campaign
 where p_exerp_campaign.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_campaign.dv_batch_id > @max_dv_batch_id
        or p_exerp_campaign.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_campaign.bk_hash,
       p_exerp_campaign.campaign_id campaign_id,
       s_exerp_campaign.campaign_codes_type campaign_codes_type,
       s_exerp_campaign.name campaign_name,
       s_exerp_campaign.state campaign_state,
       s_exerp_campaign.type campaign_type,
       case when p_exerp_campaign.bk_hash in('-997', '-998', '-999') then p_exerp_campaign.bk_hash
           when s_exerp_campaign.end_date is null then '-998'
        else convert(varchar, s_exerp_campaign.end_date, 112)    end end_dim_date_key,
       case when p_exerp_campaign.bk_hash in('-997', '-998', '-999') then p_exerp_campaign.bk_hash
           when s_exerp_campaign.start_date is null then '-998'
        else convert(varchar, s_exerp_campaign.start_date, 112)    end start_dim_date_key,
       isnull(h_exerp_campaign.dv_deleted,0) dv_deleted,
       p_exerp_campaign.p_exerp_campaign_id,
       p_exerp_campaign.dv_batch_id,
       p_exerp_campaign.dv_load_date_time,
       p_exerp_campaign.dv_load_end_date_time
  from dbo.h_exerp_campaign
  join dbo.p_exerp_campaign
    on h_exerp_campaign.bk_hash = p_exerp_campaign.bk_hash
  join #p_exerp_campaign_insert
    on p_exerp_campaign.bk_hash = #p_exerp_campaign_insert.bk_hash
   and p_exerp_campaign.p_exerp_campaign_id = #p_exerp_campaign_insert.p_exerp_campaign_id
  join dbo.s_exerp_campaign
    on p_exerp_campaign.bk_hash = s_exerp_campaign.bk_hash
   and p_exerp_campaign.s_exerp_campaign_id = s_exerp_campaign.s_exerp_campaign_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_campaign
   where d_exerp_campaign.bk_hash in (select bk_hash from #p_exerp_campaign_insert)

  insert dbo.d_exerp_campaign(
             bk_hash,
             campaign_id,
             campaign_codes_type,
             campaign_name,
             campaign_state,
             campaign_type,
             end_dim_date_key,
             start_dim_date_key,
             deleted_flag,
             p_exerp_campaign_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         campaign_id,
         campaign_codes_type,
         campaign_name,
         campaign_state,
         campaign_type,
         end_dim_date_key,
         start_dim_date_key,
         dv_deleted,
         p_exerp_campaign_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_campaign)
--Done!
end
