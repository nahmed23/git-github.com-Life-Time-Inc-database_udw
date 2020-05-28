CREATE PROC [dbo].[proc_d_mms_package_adjustment] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_package_adjustment)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_package_adjustment_insert') is not null drop table #p_mms_package_adjustment_insert
create table dbo.#p_mms_package_adjustment_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_package_adjustment.p_mms_package_adjustment_id,
       p_mms_package_adjustment.bk_hash
  from dbo.p_mms_package_adjustment
 where p_mms_package_adjustment.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_package_adjustment.dv_batch_id > @max_dv_batch_id
        or p_mms_package_adjustment.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_package_adjustment.bk_hash,
       p_mms_package_adjustment.bk_hash fact_mms_package_adjustment_key,
       p_mms_package_adjustment.package_adjustment_id package_adjustment_id,
       s_mms_package_adjustment.adjusted_date_time adjusted_date_time,
       case when p_mms_package_adjustment.bk_hash in ('-997', '-998', '-999') then p_mms_package_adjustment.bk_hash 
              when s_mms_package_adjustment.adjusted_date_time is null then '-998'    
              else convert(varchar, s_mms_package_adjustment.adjusted_date_time, 112)   end adjusted_dim_date_key,
       case when p_mms_package_adjustment.bk_hash in ('-997', '-998', '-999') then p_mms_package_adjustment.bk_hash 
              when s_mms_package_adjustment.adjusted_date_time is null then '-998'    
              else '1' + replace(substring(convert(varchar,s_mms_package_adjustment.adjusted_date_time,114), 1, 5),':','')   end adjusted_dim_time_key,
       s_mms_package_adjustment.comment adjustment_comment,
       case when p_mms_package_adjustment.bk_hash in ('-997','-998','-999') then p_mms_package_adjustment.bk_hash  
            when l_mms_package_adjustment.employee_id is null then '-998'  
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package_adjustment.employee_id as int) as varchar(500)),'z#@$k%&P'))),2) end adjustment_dim_employee_key,
       l_mms_package_adjustment.mms_tran_id adjustment_mms_tran_id,
       case when p_mms_package_adjustment.bk_hash in ('-997','-998','-999') then p_mms_package_adjustment.bk_hash 
             when l_mms_package_adjustment.val_package_adjustment_type_id is null then '-998'
       	  when l_mms_package_adjustment.val_package_adjustment_type_id in (0) then '-998'  
       	  else 'r_mms_val_package_adjustment_type_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package_adjustment.val_package_adjustment_type_id as smallint) as varchar(500)),'z#@$k%&P'))),2)  end adjustment_type_dim_description_key,
       case when p_mms_package_adjustment.bk_hash in ('-997','-998','-999') then p_mms_package_adjustment.bk_hash     
         when l_mms_package_adjustment.package_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_package_adjustment.package_id as int) as varchar(500)),'z#@$k%&P'))),2) end fact_mms_package_key,
       isnull(s_mms_package_adjustment.sessions_adjusted,0) number_of_sessions_adjusted,
       isnull(s_mms_package_adjustment.amount_adjusted, 0) package_adjustment_amount,
       isnull(h_mms_package_adjustment.dv_deleted,0) dv_deleted,
       p_mms_package_adjustment.p_mms_package_adjustment_id,
       p_mms_package_adjustment.dv_batch_id,
       p_mms_package_adjustment.dv_load_date_time,
       p_mms_package_adjustment.dv_load_end_date_time
  from dbo.h_mms_package_adjustment
  join dbo.p_mms_package_adjustment
    on h_mms_package_adjustment.bk_hash = p_mms_package_adjustment.bk_hash
  join #p_mms_package_adjustment_insert
    on p_mms_package_adjustment.bk_hash = #p_mms_package_adjustment_insert.bk_hash
   and p_mms_package_adjustment.p_mms_package_adjustment_id = #p_mms_package_adjustment_insert.p_mms_package_adjustment_id
  join dbo.l_mms_package_adjustment
    on p_mms_package_adjustment.bk_hash = l_mms_package_adjustment.bk_hash
   and p_mms_package_adjustment.l_mms_package_adjustment_id = l_mms_package_adjustment.l_mms_package_adjustment_id
  join dbo.s_mms_package_adjustment
    on p_mms_package_adjustment.bk_hash = s_mms_package_adjustment.bk_hash
   and p_mms_package_adjustment.s_mms_package_adjustment_id = s_mms_package_adjustment.s_mms_package_adjustment_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_package_adjustment
   where d_mms_package_adjustment.bk_hash in (select bk_hash from #p_mms_package_adjustment_insert)

  insert dbo.d_mms_package_adjustment(
             bk_hash,
             fact_mms_package_adjustment_key,
             package_adjustment_id,
             adjusted_date_time,
             adjusted_dim_date_key,
             adjusted_dim_time_key,
             adjustment_comment,
             adjustment_dim_employee_key,
             adjustment_mms_tran_id,
             adjustment_type_dim_description_key,
             fact_mms_package_key,
             number_of_sessions_adjusted,
             package_adjustment_amount,
             deleted_flag,
             p_mms_package_adjustment_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_package_adjustment_key,
         package_adjustment_id,
         adjusted_date_time,
         adjusted_dim_date_key,
         adjusted_dim_time_key,
         adjustment_comment,
         adjustment_dim_employee_key,
         adjustment_mms_tran_id,
         adjustment_type_dim_description_key,
         fact_mms_package_key,
         number_of_sessions_adjusted,
         package_adjustment_amount,
         dv_deleted,
         p_mms_package_adjustment_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_package_adjustment)
--Done!
end
