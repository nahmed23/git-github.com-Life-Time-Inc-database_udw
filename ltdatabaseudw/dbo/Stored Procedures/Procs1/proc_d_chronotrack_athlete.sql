CREATE PROC [dbo].[proc_d_chronotrack_athlete] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_chronotrack_athlete)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_chronotrack_athlete_insert') is not null drop table #p_chronotrack_athlete_insert
create table dbo.#p_chronotrack_athlete_insert with(distribution=hash(bk_hash), location=user_db) as
select p_chronotrack_athlete.p_chronotrack_athlete_id,
       p_chronotrack_athlete.bk_hash
  from dbo.p_chronotrack_athlete
 where p_chronotrack_athlete.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_chronotrack_athlete.dv_batch_id > @max_dv_batch_id
        or p_chronotrack_athlete.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_chronotrack_athlete.bk_hash,
       p_chronotrack_athlete.athlete_id athlete_id,
       l_chronotrack_athlete.account_id account_id,
       s_chronotrack_athlete.age age,
       s_chronotrack_athlete.birth_date birth_date,
       case when p_chronotrack_athlete.bk_hash in('-997', '-998', '-999') then p_chronotrack_athlete.bk_hash
           when s_chronotrack_athlete.birth_date is null then '-998'
       	when  convert(varchar, s_chronotrack_athlete.birth_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_chronotrack_athlete.birth_date, 112)< 19000101 then '19000101' 
        else convert(varchar, s_chronotrack_athlete.birth_date, 112)    end birth_dim_date_key,
       s_chronotrack_athlete.create_time create_time,
       case when p_chronotrack_athlete.bk_hash in('-997', '-998', '-999') then p_chronotrack_athlete.bk_hash
           when l_chronotrack_athlete.location_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_chronotrack_athlete.location_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end d_chronotrack_location_bk_hash,
       s_chronotrack_athlete.email email,
       s_chronotrack_athlete.emerg_name emerg_name,
       s_chronotrack_athlete.emerg_phone emerg_phone,
       s_chronotrack_athlete.emerg_relationship emerg_relationship,
       s_chronotrack_athlete.first_name first_name,
       s_chronotrack_athlete.home_phone home_phone,
       s_chronotrack_athlete.last_name last_name,
       l_chronotrack_athlete.location_id location_id,
       s_chronotrack_athlete.medical_notes medical_notes,
       s_chronotrack_athlete.middle_name middle_name,
       s_chronotrack_athlete.mobile_phone mobile_phone,
       s_chronotrack_athlete.modified_time modified_time,
       s_chronotrack_athlete.name_pronunciation name_pronunciation,
       s_chronotrack_athlete.sex sex,
       s_chronotrack_athlete.tshirt_size tshirt_size,
       s_chronotrack_athlete.usat_num usat_num,
       isnull(h_chronotrack_athlete.dv_deleted,0) dv_deleted,
       p_chronotrack_athlete.p_chronotrack_athlete_id,
       p_chronotrack_athlete.dv_batch_id,
       p_chronotrack_athlete.dv_load_date_time,
       p_chronotrack_athlete.dv_load_end_date_time
  from dbo.h_chronotrack_athlete
  join dbo.p_chronotrack_athlete
    on h_chronotrack_athlete.bk_hash = p_chronotrack_athlete.bk_hash
  join #p_chronotrack_athlete_insert
    on p_chronotrack_athlete.bk_hash = #p_chronotrack_athlete_insert.bk_hash
   and p_chronotrack_athlete.p_chronotrack_athlete_id = #p_chronotrack_athlete_insert.p_chronotrack_athlete_id
  join dbo.l_chronotrack_athlete
    on p_chronotrack_athlete.bk_hash = l_chronotrack_athlete.bk_hash
   and p_chronotrack_athlete.l_chronotrack_athlete_id = l_chronotrack_athlete.l_chronotrack_athlete_id
  join dbo.s_chronotrack_athlete
    on p_chronotrack_athlete.bk_hash = s_chronotrack_athlete.bk_hash
   and p_chronotrack_athlete.s_chronotrack_athlete_id = s_chronotrack_athlete.s_chronotrack_athlete_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_chronotrack_athlete
   where d_chronotrack_athlete.bk_hash in (select bk_hash from #p_chronotrack_athlete_insert)

  insert dbo.d_chronotrack_athlete(
             bk_hash,
             athlete_id,
             account_id,
             age,
             birth_date,
             birth_dim_date_key,
             create_time,
             d_chronotrack_location_bk_hash,
             email,
             emerg_name,
             emerg_phone,
             emerg_relationship,
             first_name,
             home_phone,
             last_name,
             location_id,
             medical_notes,
             middle_name,
             mobile_phone,
             modified_time,
             name_pronunciation,
             sex,
             tshirt_size,
             usat_num,
             deleted_flag,
             p_chronotrack_athlete_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         athlete_id,
         account_id,
         age,
         birth_date,
         birth_dim_date_key,
         create_time,
         d_chronotrack_location_bk_hash,
         email,
         emerg_name,
         emerg_phone,
         emerg_relationship,
         first_name,
         home_phone,
         last_name,
         location_id,
         medical_notes,
         middle_name,
         mobile_phone,
         modified_time,
         name_pronunciation,
         sex,
         tshirt_size,
         usat_num,
         dv_deleted,
         p_chronotrack_athlete_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_chronotrack_athlete)
--Done!
end
