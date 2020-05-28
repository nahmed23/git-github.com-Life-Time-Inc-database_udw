CREATE PROC [dbo].[proc_d_ec_plans] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ec_plans)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ec_plans_insert') is not null drop table #p_ec_plans_insert
create table dbo.#p_ec_plans_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_plans.p_ec_plans_id,
       p_ec_plans.bk_hash
  from dbo.p_ec_plans
 where p_ec_plans.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ec_plans.dv_batch_id > @max_dv_batch_id
        or p_ec_plans.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_plans.bk_hash,
       p_ec_plans.bk_hash dim_trainerize_plan_key,
       p_ec_plans.plan_id plan_id,
       case when p_ec_plans.bk_hash in ('-997','-998','-999') then p_ec_plans.bk_hash        
        when l_ec_plans.party_id is null then '-998'       
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_plans.party_id as int) as varchar(500)),'z#@$k%&P'))),2)   end coach_d_ltfeb_ltf_user_identity_bk_hash,
       l_ec_plans.coach_party_id coach_party_id,
       case when p_ec_plans.bk_hash in ('-997', '-998', '-999') then p_ec_plans.bk_hash   
           when s_ec_plans.created_date is null then '-998'   
       	 else convert(char(8), s_ec_plans.created_date, 112)   end created_dim_date_key,
       case when p_ec_plans.bk_hash in ('-997','-998','-999') then p_ec_plans.bk_hash         
       when l_ec_plans.coach_party_id is null then '-998'       
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_plans.coach_party_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_ltfeb_ltf_user_identity_bk_hash,
       case when p_ec_plans.bk_hash in ('-997','-998','-999') then p_ec_plans.bk_hash
                when l_ec_plans.program_id is null then '-998'       
       		 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_plans.program_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_trainerize_program_key,
       isnull(s_ec_plans.duration,'') duration,
       s_ec_plans.duration_type duration_type,
       case when p_ec_plans.bk_hash in ('-997', '-998', '-999') then p_ec_plans.bk_hash   
           when s_ec_plans.end_date is null then '-998'   
       	 else convert(char(8), s_ec_plans.end_date, 112)   end end_dim_date_key,
       case when p_ec_plans.bk_hash in ('-997','-998','-999') then p_ec_plans.bk_hash
       when s_ec_plans.end_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_plans.end_date,114), 1, 5),':','') end end_dim_time_key,
       l_ec_plans.party_id party_id,
       isnull(s_ec_plans.name,'') plan_name,
       l_ec_plans.source_id source_id,
       s_ec_plans.source_type source_type,
       case when p_ec_plans.bk_hash in ('-997', '-998', '-999') then p_ec_plans.bk_hash   
           when s_ec_plans.start_date is null then '-998'   
       	 else convert(char(8), s_ec_plans.start_date, 112)   end start_dim_date_key,
       case when p_ec_plans.bk_hash in ('-997','-998','-999') then p_ec_plans.bk_hash
       when s_ec_plans.start_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_plans.start_date,114), 1, 5),':','') end start_dim_time_key,
       case when p_ec_plans.bk_hash in ('-997', '-998', '-999') then p_ec_plans.bk_hash   
           when s_ec_plans.updated_date is null then '-998'   
       	 else convert(char(8), s_ec_plans.updated_date, 112)   end updated_dim_date_key,
       isnull(h_ec_plans.dv_deleted,0) dv_deleted,
       p_ec_plans.p_ec_plans_id,
       p_ec_plans.dv_batch_id,
       p_ec_plans.dv_load_date_time,
       p_ec_plans.dv_load_end_date_time
  from dbo.h_ec_plans
  join dbo.p_ec_plans
    on h_ec_plans.bk_hash = p_ec_plans.bk_hash
  join #p_ec_plans_insert
    on p_ec_plans.bk_hash = #p_ec_plans_insert.bk_hash
   and p_ec_plans.p_ec_plans_id = #p_ec_plans_insert.p_ec_plans_id
  join dbo.l_ec_plans
    on p_ec_plans.bk_hash = l_ec_plans.bk_hash
   and p_ec_plans.l_ec_plans_id = l_ec_plans.l_ec_plans_id
  join dbo.s_ec_plans
    on p_ec_plans.bk_hash = s_ec_plans.bk_hash
   and p_ec_plans.s_ec_plans_id = s_ec_plans.s_ec_plans_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ec_plans
   where d_ec_plans.bk_hash in (select bk_hash from #p_ec_plans_insert)

  insert dbo.d_ec_plans(
             bk_hash,
             dim_trainerize_plan_key,
             plan_id,
             coach_d_ltfeb_ltf_user_identity_bk_hash,
             coach_party_id,
             created_dim_date_key,
             d_ltfeb_ltf_user_identity_bk_hash,
             dim_trainerize_program_key,
             duration,
             duration_type,
             end_dim_date_key,
             end_dim_time_key,
             party_id,
             plan_name,
             source_id,
             source_type,
             start_dim_date_key,
             start_dim_time_key,
             updated_dim_date_key,
             deleted_flag,
             p_ec_plans_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_trainerize_plan_key,
         plan_id,
         coach_d_ltfeb_ltf_user_identity_bk_hash,
         coach_party_id,
         created_dim_date_key,
         d_ltfeb_ltf_user_identity_bk_hash,
         dim_trainerize_program_key,
         duration,
         duration_type,
         end_dim_date_key,
         end_dim_time_key,
         party_id,
         plan_name,
         source_id,
         source_type,
         start_dim_date_key,
         start_dim_time_key,
         updated_dim_date_key,
         dv_deleted,
         p_ec_plans_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ec_plans)
--Done!
end
