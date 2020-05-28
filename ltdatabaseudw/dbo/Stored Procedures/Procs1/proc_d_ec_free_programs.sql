CREATE PROC [dbo].[proc_d_ec_free_programs] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ec_free_programs)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ec_free_programs_insert') is not null drop table #p_ec_free_programs_insert
create table dbo.#p_ec_free_programs_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_free_programs.p_ec_free_programs_id,
       p_ec_free_programs.bk_hash
  from dbo.p_ec_free_programs
 where p_ec_free_programs.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ec_free_programs.dv_batch_id > @max_dv_batch_id
        or p_ec_free_programs.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_free_programs.bk_hash,
       p_ec_free_programs.bk_hash dim_ec_free_programs_keys,
       p_ec_free_programs.free_program_id free_program_id,
       case when s_ec_free_programs.is_active = 1  then 'Y' else 'N' end active_flag,
       s_ec_free_programs.created_date created_date,
       case when p_ec_free_programs.bk_hash in ('-997', '-998', '-999') then p_ec_free_programs.bk_hash
           when s_ec_free_programs.created_date is null then '-998'
        else convert(varchar, s_ec_free_programs.created_date, 112)    end created_dim_date_key,
       case when p_ec_free_programs.bk_hash in ('-997','-998','-999') then p_ec_free_programs.bk_hash
       when s_ec_free_programs.created_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_free_programs.created_date,114), 1, 5),':','') end created_dim_time_key,
       case when p_ec_free_programs.bk_hash in ('-997','-998','-999') then p_ec_free_programs.bk_hash     
         when l_ec_free_programs.program_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_free_programs.program_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_ec_programs_bk_hash,
       s_ec_free_programs.duration duration,
       s_ec_free_programs.end_date end_date,
       case when p_ec_free_programs.bk_hash in ('-997', '-998', '-999') then p_ec_free_programs.bk_hash
           when s_ec_free_programs.end_date is null then '-998'
        else convert(varchar, s_ec_free_programs.end_date, 112)    end end_dim_date_key,
       case when p_ec_free_programs.bk_hash in ('-997','-998','-999') then p_ec_free_programs.bk_hash
       when s_ec_free_programs.end_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_free_programs.end_date,114), 1, 5),':','') end end_dim_time_key,
       s_ec_free_programs.equipment equipment,
       s_ec_free_programs.exercise exercise,
       case when s_ec_free_programs.featured = 1  then 'Y' else 'N' end featured_flag,
       s_ec_free_programs.frequency frequency,
       s_ec_free_programs.goal goal,
       s_ec_free_programs.level level,
       s_ec_free_programs.priority priority,
       s_ec_free_programs.program_description program_description,
       l_ec_free_programs.program_id program_id,
       s_ec_free_programs.program_image program_image,
       s_ec_free_programs.program_name program_name,
       s_ec_free_programs.updated_date updated_date,
       case when p_ec_free_programs.bk_hash in ('-997', '-998', '-999') then p_ec_free_programs.bk_hash
           when s_ec_free_programs.updated_date is null then '-998'
        else convert(varchar, s_ec_free_programs.updated_date, 112)    end updated_dim_date_key,
       case when p_ec_free_programs.bk_hash in ('-997','-998','-999') then p_ec_free_programs.bk_hash
       when s_ec_free_programs.updated_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_free_programs.updated_date,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_ec_free_programs.dv_deleted,0) dv_deleted,
       p_ec_free_programs.p_ec_free_programs_id,
       p_ec_free_programs.dv_batch_id,
       p_ec_free_programs.dv_load_date_time,
       p_ec_free_programs.dv_load_end_date_time
  from dbo.h_ec_free_programs
  join dbo.p_ec_free_programs
    on h_ec_free_programs.bk_hash = p_ec_free_programs.bk_hash
  join #p_ec_free_programs_insert
    on p_ec_free_programs.bk_hash = #p_ec_free_programs_insert.bk_hash
   and p_ec_free_programs.p_ec_free_programs_id = #p_ec_free_programs_insert.p_ec_free_programs_id
  join dbo.l_ec_free_programs
    on p_ec_free_programs.bk_hash = l_ec_free_programs.bk_hash
   and p_ec_free_programs.l_ec_free_programs_id = l_ec_free_programs.l_ec_free_programs_id
  join dbo.s_ec_free_programs
    on p_ec_free_programs.bk_hash = s_ec_free_programs.bk_hash
   and p_ec_free_programs.s_ec_free_programs_id = s_ec_free_programs.s_ec_free_programs_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ec_free_programs
   where d_ec_free_programs.bk_hash in (select bk_hash from #p_ec_free_programs_insert)

  insert dbo.d_ec_free_programs(
             bk_hash,
             dim_ec_free_programs_keys,
             free_program_id,
             active_flag,
             created_date,
             created_dim_date_key,
             created_dim_time_key,
             d_ec_programs_bk_hash,
             duration,
             end_date,
             end_dim_date_key,
             end_dim_time_key,
             equipment,
             exercise,
             featured_flag,
             frequency,
             goal,
             level,
             priority,
             program_description,
             program_id,
             program_image,
             program_name,
             updated_date,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_ec_free_programs_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_ec_free_programs_keys,
         free_program_id,
         active_flag,
         created_date,
         created_dim_date_key,
         created_dim_time_key,
         d_ec_programs_bk_hash,
         duration,
         end_date,
         end_dim_date_key,
         end_dim_time_key,
         equipment,
         exercise,
         featured_flag,
         frequency,
         goal,
         level,
         priority,
         program_description,
         program_id,
         program_image,
         program_name,
         updated_date,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_ec_free_programs_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ec_free_programs)
--Done!
end
