CREATE PROC [dbo].[proc_d_boss_asi_res_inst] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_res_inst)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_res_inst_insert') is not null drop table #p_boss_asi_res_inst_insert
create table dbo.#p_boss_asi_res_inst_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_res_inst.p_boss_asi_res_inst_id,
       p_boss_asi_res_inst.bk_hash
  from dbo.p_boss_asi_res_inst
 where p_boss_asi_res_inst.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_res_inst.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_res_inst.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_res_inst.bk_hash,
       p_boss_asi_res_inst.asi_res_inst_id asi_res_inst_id,
       case when p_boss_asi_res_inst.bk_hash in ('-997', '-998', '-999') then p_boss_asi_res_inst.bk_hash
           when l_boss_asi_res_inst.reservation is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(l_boss_asi_res_inst.reservation)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_boss_reservation_key,
       case when p_boss_asi_res_inst.bk_hash in ('-997', '-998', '-999') then p_boss_asi_res_inst.bk_hash
           when l_boss_asi_res_inst.instructor_id is null then '-998'
       	when l_boss_asi_res_inst.instructor_id = 0 then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(l_boss_asi_res_inst.instructor_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_employee_key,
       h_boss_asi_res_inst.dv_deleted dv_deleted_flag,
       case when p_boss_asi_res_inst.bk_hash in('-997', '-998', '-999') then p_boss_asi_res_inst.bk_hash
           when s_boss_asi_res_inst.end_date is null then '-998'
       	else convert(varchar, s_boss_asi_res_inst.end_date, 112) 
       end instructor_end_dim_date_key,
       case when p_boss_asi_res_inst.bk_hash in('-997', '-998', '-999') then p_boss_asi_res_inst.bk_hash
           when s_boss_asi_res_inst.start_date is null then '-998'
       	else convert(varchar, s_boss_asi_res_inst.start_date, 112) 
       end instructor_start_dim_date_key,
       case when s_boss_asi_res_inst.substitute='' or s_boss_asi_res_inst.substitute is null or len(s_boss_asi_res_inst.substitute)=0 then 'P'
       	else s_boss_asi_res_inst.substitute
       end instructor_type,
       isnull(h_boss_asi_res_inst.dv_deleted,0) dv_deleted,
       p_boss_asi_res_inst.p_boss_asi_res_inst_id,
       p_boss_asi_res_inst.dv_batch_id,
       p_boss_asi_res_inst.dv_load_date_time,
       p_boss_asi_res_inst.dv_load_end_date_time
  from dbo.h_boss_asi_res_inst
  join dbo.p_boss_asi_res_inst
    on h_boss_asi_res_inst.bk_hash = p_boss_asi_res_inst.bk_hash
  join #p_boss_asi_res_inst_insert
    on p_boss_asi_res_inst.bk_hash = #p_boss_asi_res_inst_insert.bk_hash
   and p_boss_asi_res_inst.p_boss_asi_res_inst_id = #p_boss_asi_res_inst_insert.p_boss_asi_res_inst_id
  join dbo.l_boss_asi_res_inst
    on p_boss_asi_res_inst.bk_hash = l_boss_asi_res_inst.bk_hash
   and p_boss_asi_res_inst.l_boss_asi_res_inst_id = l_boss_asi_res_inst.l_boss_asi_res_inst_id
  join dbo.s_boss_asi_res_inst
    on p_boss_asi_res_inst.bk_hash = s_boss_asi_res_inst.bk_hash
   and p_boss_asi_res_inst.s_boss_asi_res_inst_id = s_boss_asi_res_inst.s_boss_asi_res_inst_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_res_inst
   where d_boss_asi_res_inst.bk_hash in (select bk_hash from #p_boss_asi_res_inst_insert)

  insert dbo.d_boss_asi_res_inst(
             bk_hash,
             asi_res_inst_id,
             dim_boss_reservation_key,
             dim_employee_key,
             dv_deleted_flag,
             instructor_end_dim_date_key,
             instructor_start_dim_date_key,
             instructor_type,
             deleted_flag,
             p_boss_asi_res_inst_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         asi_res_inst_id,
         dim_boss_reservation_key,
         dim_employee_key,
         dv_deleted_flag,
         instructor_end_dim_date_key,
         instructor_start_dim_date_key,
         instructor_type,
         dv_deleted,
         p_boss_asi_res_inst_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_res_inst)
--Done!
end
