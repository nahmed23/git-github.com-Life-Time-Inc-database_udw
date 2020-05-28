CREATE PROC [dbo].[proc_d_pool_pass_courtesy_visits] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_pool_pass_courtesy_visits)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_pool_pass_courtesy_visits_insert') is not null drop table #p_pool_pass_courtesy_visits_insert
create table dbo.#p_pool_pass_courtesy_visits_insert with(distribution=hash(bk_hash), location=user_db) as
select p_pool_pass_courtesy_visits.p_pool_pass_courtesy_visits_id,
       p_pool_pass_courtesy_visits.bk_hash
  from dbo.p_pool_pass_courtesy_visits
 where p_pool_pass_courtesy_visits.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_pool_pass_courtesy_visits.dv_batch_id > @max_dv_batch_id
        or p_pool_pass_courtesy_visits.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_pool_pass_courtesy_visits.bk_hash,
       p_pool_pass_courtesy_visits.courtesy_visits_id courtesy_visits_id,
       l_pool_pass_courtesy_visits.club_id club_id,
       s_pool_pass_courtesy_visits.created_date created_date,
       case when p_pool_pass_courtesy_visits.bk_hash in('-997', '-998', '-999') then p_pool_pass_courtesy_visits.bk_hash
           when s_pool_pass_courtesy_visits.created_date is null then '-998'
        else convert(varchar, s_pool_pass_courtesy_visits.created_date, 112)    end created_dim_date_key,
       case when p_pool_pass_courtesy_visits.bk_hash in ('-997','-998','-999') then p_pool_pass_courtesy_visits.bk_hash
       when s_pool_pass_courtesy_visits.created_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_pool_pass_courtesy_visits.created_date,114), 1, 5),':','') end created_dim_time_key,
       case when p_pool_pass_courtesy_visits.bk_hash in('-997', '-998', '-999') then p_pool_pass_courtesy_visits.bk_hash
           when l_pool_pass_courtesy_visits.club_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_pool_pass_courtesy_visits.club_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       l_pool_pass_courtesy_visits.employee_party_id employee_party_id,
       l_pool_pass_courtesy_visits.member_party_id member_party_id,
       s_pool_pass_courtesy_visits.updated_date updated_date,
       case when p_pool_pass_courtesy_visits.bk_hash in('-997', '-998', '-999') then p_pool_pass_courtesy_visits.bk_hash
           when s_pool_pass_courtesy_visits.updated_date is null then '-998'
        else convert(varchar, s_pool_pass_courtesy_visits.updated_date, 112)    end updated_dim_date_key,
       case when p_pool_pass_courtesy_visits.bk_hash in ('-997','-998','-999') then p_pool_pass_courtesy_visits.bk_hash
       when s_pool_pass_courtesy_visits.updated_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_pool_pass_courtesy_visits.updated_date,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_pool_pass_courtesy_visits.dv_deleted,0) dv_deleted,
       p_pool_pass_courtesy_visits.p_pool_pass_courtesy_visits_id,
       p_pool_pass_courtesy_visits.dv_batch_id,
       p_pool_pass_courtesy_visits.dv_load_date_time,
       p_pool_pass_courtesy_visits.dv_load_end_date_time
  from dbo.h_pool_pass_courtesy_visits
  join dbo.p_pool_pass_courtesy_visits
    on h_pool_pass_courtesy_visits.bk_hash = p_pool_pass_courtesy_visits.bk_hash
  join #p_pool_pass_courtesy_visits_insert
    on p_pool_pass_courtesy_visits.bk_hash = #p_pool_pass_courtesy_visits_insert.bk_hash
   and p_pool_pass_courtesy_visits.p_pool_pass_courtesy_visits_id = #p_pool_pass_courtesy_visits_insert.p_pool_pass_courtesy_visits_id
  join dbo.l_pool_pass_courtesy_visits
    on p_pool_pass_courtesy_visits.bk_hash = l_pool_pass_courtesy_visits.bk_hash
   and p_pool_pass_courtesy_visits.l_pool_pass_courtesy_visits_id = l_pool_pass_courtesy_visits.l_pool_pass_courtesy_visits_id
  join dbo.s_pool_pass_courtesy_visits
    on p_pool_pass_courtesy_visits.bk_hash = s_pool_pass_courtesy_visits.bk_hash
   and p_pool_pass_courtesy_visits.s_pool_pass_courtesy_visits_id = s_pool_pass_courtesy_visits.s_pool_pass_courtesy_visits_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_pool_pass_courtesy_visits
   where d_pool_pass_courtesy_visits.bk_hash in (select bk_hash from #p_pool_pass_courtesy_visits_insert)

  insert dbo.d_pool_pass_courtesy_visits(
             bk_hash,
             courtesy_visits_id,
             club_id,
             created_date,
             created_dim_date_key,
             created_dim_time_key,
             dim_club_key,
             employee_party_id,
             member_party_id,
             updated_date,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_pool_pass_courtesy_visits_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         courtesy_visits_id,
         club_id,
         created_date,
         created_dim_date_key,
         created_dim_time_key,
         dim_club_key,
         employee_party_id,
         member_party_id,
         updated_date,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_pool_pass_courtesy_visits_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_pool_pass_courtesy_visits)
--Done!
end
