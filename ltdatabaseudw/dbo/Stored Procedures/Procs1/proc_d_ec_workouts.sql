CREATE PROC [dbo].[proc_d_ec_workouts] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ec_workouts)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ec_workouts_insert') is not null drop table #p_ec_workouts_insert
create table dbo.#p_ec_workouts_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_workouts.p_ec_workouts_id,
       p_ec_workouts.bk_hash
  from dbo.p_ec_workouts
 where p_ec_workouts.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ec_workouts.dv_batch_id > @max_dv_batch_id
        or p_ec_workouts.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_workouts.bk_hash,
       p_ec_workouts.bk_hash dim_trainerize_workout_key,
       p_ec_workouts.workouts_id workouts_id,
       case when p_ec_workouts.bk_hash in ('-997', '-998', '-999') then p_ec_workouts.bk_hash when s_ec_workouts.created_date is null then '-998' else convert(char(8), s_ec_workouts.created_date, 112) end created_dim_date_key,
       case when  p_ec_workouts.bk_hash in ('-997','-998','-999') then p_ec_workouts.bk_hash 
       when  l_ec_workouts.party_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_workouts.party_id as int) as varchar(500)),'z#@$k%&P'))),2) end d_ec_workouts_party_bk_hash,
       s_ec_workouts.description description,
       s_ec_workouts.discriminator discriminator,
       case when p_ec_workouts.bk_hash in ('-997','-998','-999') then p_ec_workouts.bk_hash
       when l_ec_workouts.party_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_workouts.party_id as int) as varchar(500)),'z#@$k%&P'))),2) end ec_workouts_party_bk_hash,
       case when p_ec_workouts.bk_hash in ('-997', '-998', '-999') then p_ec_workouts.bk_hash when s_ec_workouts.created_date is null then '-998' else convert(char(8), s_ec_workouts.inactive_date, 112) end inactive_dim_date_key,
       case when p_ec_workouts.bk_hash in ('-997', '-998', '-999') then p_ec_workouts.bk_hash when s_ec_workouts.created_date is null then '-998' else convert(char(8), s_ec_workouts.modified_date, 112) end modified_dim_date_key,
       s_ec_workouts.name name,
       case when p_ec_workouts.bk_hash in ('-997','-998','-999') then p_ec_workouts.bk_hash
       when l_ec_workouts.party_id is null then '-998'
       else l_ec_workouts.party_id end party_id,
       s_ec_workouts.tags tags,
       s_ec_workouts.type type,
       isnull(h_ec_workouts.dv_deleted,0) dv_deleted,
       p_ec_workouts.p_ec_workouts_id,
       p_ec_workouts.dv_batch_id,
       p_ec_workouts.dv_load_date_time,
       p_ec_workouts.dv_load_end_date_time
  from dbo.h_ec_workouts
  join dbo.p_ec_workouts
    on h_ec_workouts.bk_hash = p_ec_workouts.bk_hash
  join #p_ec_workouts_insert
    on p_ec_workouts.bk_hash = #p_ec_workouts_insert.bk_hash
   and p_ec_workouts.p_ec_workouts_id = #p_ec_workouts_insert.p_ec_workouts_id
  join dbo.l_ec_workouts
    on p_ec_workouts.bk_hash = l_ec_workouts.bk_hash
   and p_ec_workouts.l_ec_workouts_id = l_ec_workouts.l_ec_workouts_id
  join dbo.s_ec_workouts
    on p_ec_workouts.bk_hash = s_ec_workouts.bk_hash
   and p_ec_workouts.s_ec_workouts_id = s_ec_workouts.s_ec_workouts_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ec_workouts
   where d_ec_workouts.bk_hash in (select bk_hash from #p_ec_workouts_insert)

  insert dbo.d_ec_workouts(
             bk_hash,
             dim_trainerize_workout_key,
             workouts_id,
             created_dim_date_key,
             d_ec_workouts_party_bk_hash,
             description,
             discriminator,
             ec_workouts_party_bk_hash,
             inactive_dim_date_key,
             modified_dim_date_key,
             name,
             party_id,
             tags,
             type,
             deleted_flag,
             p_ec_workouts_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_trainerize_workout_key,
         workouts_id,
         created_dim_date_key,
         d_ec_workouts_party_bk_hash,
         description,
         discriminator,
         ec_workouts_party_bk_hash,
         inactive_dim_date_key,
         modified_dim_date_key,
         name,
         party_id,
         tags,
         type,
         dv_deleted,
         p_ec_workouts_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ec_workouts)
--Done!
end
