CREATE PROC [dbo].[proc_d_mms_employee] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_employee)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_employee_insert') is not null drop table #p_mms_employee_insert
create table dbo.#p_mms_employee_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_employee.p_mms_employee_id,
       p_mms_employee.bk_hash
  from dbo.p_mms_employee
 where p_mms_employee.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_employee.dv_batch_id > @max_dv_batch_id
        or p_mms_employee.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_employee.bk_hash,
       p_mms_employee.bk_hash dim_employee_key,
       p_mms_employee.employee_id employee_id,
       case when p_mms_employee.bk_hash in ('-997','-998','-999') then p_mms_employee.bk_hash
                    when l_mms_employee.club_id is null then '-998'
                    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_employee.club_id as varchar(500)),'z#@$k%&P'))),2)
                 end  dim_club_key,
       case when s_mms_employee.active_status_flag = 1 then 'Y'
                  else 'N'
       		   end employee_active_flag,
       case when s_mms_employee.first_name is not null and  s_mms_employee.last_name is not null
                 then s_mms_employee.first_name + ' ' + s_mms_employee.last_name
         when s_mms_employee.first_name is null then s_mms_employee.last_name
                  else s_mms_employee.first_name
       		   end employee_name,
       case when s_mms_employee.first_name is not null and  s_mms_employee.last_name is not null
                 then s_mms_employee.last_name + ', ' + s_mms_employee.first_name
         when s_mms_employee.first_name is null then s_mms_employee.last_name
                  else s_mms_employee.first_name
       		   end employee_name_last_first,
       isnull(s_mms_employee.first_name,'') first_name,
       s_mms_employee.inserted_date_time inserted_date_time,
       isnull(s_mms_employee.last_name,'') last_name,
       l_mms_employee.member_id member_id,
       p_mms_employee.p_mms_employee_id,
       p_mms_employee.dv_batch_id,
       p_mms_employee.dv_load_date_time,
       p_mms_employee.dv_load_end_date_time
  from dbo.p_mms_employee
  join #p_mms_employee_insert
    on p_mms_employee.bk_hash = #p_mms_employee_insert.bk_hash
   and p_mms_employee.p_mms_employee_id = #p_mms_employee_insert.p_mms_employee_id
  join dbo.l_mms_employee
    on p_mms_employee.bk_hash = l_mms_employee.bk_hash
   and p_mms_employee.l_mms_employee_id = l_mms_employee.l_mms_employee_id
  join dbo.s_mms_employee
    on p_mms_employee.bk_hash = s_mms_employee.bk_hash
   and p_mms_employee.s_mms_employee_id = s_mms_employee.s_mms_employee_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_employee
   where d_mms_employee.bk_hash in (select bk_hash from #p_mms_employee_insert)

  insert dbo.d_mms_employee(
             bk_hash,
             dim_employee_key,
             employee_id,
             dim_club_key,
             employee_active_flag,
             employee_name,
             employee_name_last_first,
             first_name,
             inserted_date_time,
             last_name,
             member_id,
             p_mms_employee_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_employee_key,
         employee_id,
         dim_club_key,
         employee_active_flag,
         employee_name,
         employee_name_last_first,
         first_name,
         inserted_date_time,
         last_name,
         member_id,
         p_mms_employee_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_employee)
--Done!
end
