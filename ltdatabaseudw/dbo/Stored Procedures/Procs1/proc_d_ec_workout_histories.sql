CREATE PROC [dbo].[proc_d_ec_workout_histories] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ec_workout_histories)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ec_workout_histories_insert') is not null drop table #p_ec_workout_histories_insert
create table dbo.#p_ec_workout_histories_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_workout_histories.p_ec_workout_histories_id,
       p_ec_workout_histories.bk_hash
  from dbo.p_ec_workout_histories
 where p_ec_workout_histories.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ec_workout_histories.dv_batch_id > @max_dv_batch_id
        or p_ec_workout_histories.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_workout_histories.bk_hash,
       p_ec_Workout_Histories.bk_hash fact_trainerize_workout_history_key,
       p_ec_workout_histories.workout_history_id workout_history_id,
       case when s_ec_workout_histories.is_active = 1 then 'Y'
            else 'N'
        end active_flag,
       isnull(s_ec_workout_histories.activity_type, '') activity_type,
       s_ec_workout_histories.average_heart_rate average_heart_rate,
       s_ec_workout_histories.average_miles_per_hour average_miles_per_hour,
       s_ec_workout_histories.average_watts average_watts,
       isnull(s_ec_workout_histories.comments, '') comments,
       case when s_ec_workout_histories.completed = 1 then 'Y'
            else 'N'
        end completed_flag,
       case when p_ec_workout_histories.bk_hash in ('-997', '-998', '-999') then p_ec_workout_histories.bk_hash when s_ec_workout_histories.date_created is null then '-998' else convert(char(8), s_ec_workout_histories.date_created, 112) end created_dim_date_key,
       case when s_ec_workout_histories.is_custom = 1 then 'Y'
            else 'N'
        end custom_flag,
       case when  p_ec_workout_histories.bk_hash in ('-997','-998','-999') then p_ec_workout_histories.bk_hash 
       when  l_ec_workout_histories.party_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_workout_histories.party_id as int) as varchar(500)),'z#@$k%&P'))),2) end d_ec_workout_histories_party_bk_hash,
       case when p_ec_workout_histories.bk_hash in ('-997','-998','-999') then p_ec_workout_histories.bk_hash when l_ec_workout_histories.workout_id is null then '-998' else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_workout_histories.workout_id as int) as varchar(500)),'z#@$k%&P'))),2) end d_workout_key,
       s_ec_workout_histories.distance_in_miles distance_in_miles,
       case when p_ec_workout_histories.bk_hash in ('-997', '-998', '-999') then p_ec_workout_histories.bk_hash when s_ec_workout_histories.date_ended is null then '-998' else convert(char(8), s_ec_workout_histories.date_ended, 112) end ended_dim_date_key,
       case when p_ec_Workout_Histories.bk_hash in ('-997','-998','-999') then p_ec_Workout_Histories.bk_hash
       when s_ec_Workout_Histories.date_ended is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_Workout_Histories.date_ended,114), 1, 5),':','') end ended_dim_time_key,
       s_ec_workout_histories.fat_calories fat_calories,
       s_ec_workout_histories.heart_rate_zone_five_seconds heart_rate_zone_five_seconds,
       s_ec_workout_histories.heart_rate_zone_four_seconds heart_rate_zone_four_seconds,
       s_ec_workout_histories.heart_rate_zone_one_seconds heart_rate_zone_one_seconds,
       s_ec_workout_histories.heart_rate_zone_three_seconds heart_rate_zone_three_seconds,
       s_ec_workout_histories.heart_rate_zone_two_seconds heart_rate_zone_two_seconds,
       s_ec_workout_histories.[key] key_value,
       case when p_ec_Workout_Histories.bk_hash in ('-997','-998','-999') then p_ec_Workout_Histories.bk_hash
       when l_ec_Workout_Histories.party_id is null then '-998'
       else l_ec_Workout_Histories.party_id end party_id,
       s_ec_workout_histories.rating rating,
       case when s_ec_workout_histories.scheduled = 1 then 'Y'
            else 'N'
        end scheduled_flag,
       isnull(s_ec_workout_histories.source_name, '') source_name,
       isnull(s_ec_workout_histories.source_workout_id, '') source_workout_id,
       case when p_ec_workout_histories.bk_hash in ('-997', '-998', '-999') then p_ec_workout_histories.bk_hash when s_ec_workout_histories.date_started is null then '-998' else convert(char(8), s_ec_workout_histories.date_started, 112) end started_dim_date_key,
       case when p_ec_Workout_Histories.bk_hash in ('-997','-998','-999') then p_ec_Workout_Histories.bk_hash
       when s_ec_Workout_Histories.date_started is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_Workout_Histories.date_started,114), 1, 5),':','') end started_dim_time_key,
       case when s_ec_workout_histories.is_started = 1 then 'Y'
            else 'N'
        end started_flag,
       s_ec_workout_histories.total_calories total_calories,
       case when s_ec_workout_histories.tracked = 1 then 'Y'
            else 'N'
        end tracked_flag,
       isnull(s_ec_workout_histories.description, '') workout_description,
       isnull(s_ec_workout_histories.type, '') workout_type,
       isnull(h_ec_workout_histories.dv_deleted,0) dv_deleted,
       p_ec_workout_histories.p_ec_workout_histories_id,
       p_ec_workout_histories.dv_batch_id,
       p_ec_workout_histories.dv_load_date_time,
       p_ec_workout_histories.dv_load_end_date_time
  from dbo.h_ec_workout_histories
  join dbo.p_ec_workout_histories
    on h_ec_workout_histories.bk_hash = p_ec_workout_histories.bk_hash
  join #p_ec_workout_histories_insert
    on p_ec_workout_histories.bk_hash = #p_ec_workout_histories_insert.bk_hash
   and p_ec_workout_histories.p_ec_workout_histories_id = #p_ec_workout_histories_insert.p_ec_workout_histories_id
  join dbo.l_ec_workout_histories
    on p_ec_workout_histories.bk_hash = l_ec_workout_histories.bk_hash
   and p_ec_workout_histories.l_ec_workout_histories_id = l_ec_workout_histories.l_ec_workout_histories_id
  join dbo.s_ec_workout_histories
    on p_ec_workout_histories.bk_hash = s_ec_workout_histories.bk_hash
   and p_ec_workout_histories.s_ec_workout_histories_id = s_ec_workout_histories.s_ec_workout_histories_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ec_workout_histories
   where d_ec_workout_histories.bk_hash in (select bk_hash from #p_ec_workout_histories_insert)

  insert dbo.d_ec_workout_histories(
             bk_hash,
             fact_trainerize_workout_history_key,
             workout_history_id,
             active_flag,
             activity_type,
             average_heart_rate,
             average_miles_per_hour,
             average_watts,
             comments,
             completed_flag,
             created_dim_date_key,
             custom_flag,
             d_ec_workout_histories_party_bk_hash,
             d_workout_key,
             distance_in_miles,
             ended_dim_date_key,
             ended_dim_time_key,
             fat_calories,
             heart_rate_zone_five_seconds,
             heart_rate_zone_four_seconds,
             heart_rate_zone_one_seconds,
             heart_rate_zone_three_seconds,
             heart_rate_zone_two_seconds,
             key_value,
             party_id,
             rating,
             scheduled_flag,
             source_name,
             source_workout_id,
             started_dim_date_key,
             started_dim_time_key,
             started_flag,
             total_calories,
             tracked_flag,
             workout_description,
             workout_type,
             deleted_flag,
             p_ec_workout_histories_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_trainerize_workout_history_key,
         workout_history_id,
         active_flag,
         activity_type,
         average_heart_rate,
         average_miles_per_hour,
         average_watts,
         comments,
         completed_flag,
         created_dim_date_key,
         custom_flag,
         d_ec_workout_histories_party_bk_hash,
         d_workout_key,
         distance_in_miles,
         ended_dim_date_key,
         ended_dim_time_key,
         fat_calories,
         heart_rate_zone_five_seconds,
         heart_rate_zone_four_seconds,
         heart_rate_zone_one_seconds,
         heart_rate_zone_three_seconds,
         heart_rate_zone_two_seconds,
         key_value,
         party_id,
         rating,
         scheduled_flag,
         source_name,
         source_workout_id,
         started_dim_date_key,
         started_dim_time_key,
         started_flag,
         total_calories,
         tracked_flag,
         workout_description,
         workout_type,
         dv_deleted,
         p_ec_workout_histories_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ec_workout_histories)
--Done!
end
