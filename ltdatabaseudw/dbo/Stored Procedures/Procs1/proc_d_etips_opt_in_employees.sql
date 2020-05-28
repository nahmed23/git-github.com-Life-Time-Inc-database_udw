CREATE PROC [dbo].[proc_d_etips_opt_in_employees] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_etips_opt_in_employees)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_etips_opt_in_employees_insert') is not null drop table #p_etips_opt_in_employees_insert
create table dbo.#p_etips_opt_in_employees_insert with(distribution=hash(bk_hash), location=user_db) as
select p_etips_opt_in_employees.p_etips_opt_in_employees_id,
       p_etips_opt_in_employees.bk_hash
  from dbo.p_etips_opt_in_employees
 where p_etips_opt_in_employees.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_etips_opt_in_employees.dv_batch_id > @max_dv_batch_id
        or p_etips_opt_in_employees.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_etips_opt_in_employees.bk_hash,
       p_etips_opt_in_employees.bk_hash d_etips_opt_in_employees_key,
       l_etips_opt_in_employees.employee_id employee_id,
       l_etips_opt_in_employees.pay_card_start_date pay_card_start_date,
       l_etips_opt_in_employees.pay_card_status pay_card_status,
       s_etips_opt_in_employees.ltf_file_name ltf_file_name,
       hashbytes('md5', concat(s_etips_opt_in_employees.employee_id,s_etips_opt_in_employees.pay_card_start_date,s_etips_opt_in_employees.pay_card_status)) employee_id_pay_start_date_status_key,
       cast(concat(
       substring(SUBSTRING(s_etips_opt_in_employees.ltf_file_name,CHARINDEX('.csv',(s_etips_opt_in_employees.ltf_file_name))-8,8),5,4),
       substring(SUBSTRING(s_etips_opt_in_employees.ltf_file_name,CHARINDEX('.csv',(s_etips_opt_in_employees.ltf_file_name))-8,8),1,2),
       substring(SUBSTRING(s_etips_opt_in_employees.ltf_file_name,CHARINDEX('.csv',(s_etips_opt_in_employees.ltf_file_name))-8,8),3,2)
       ) as date) file_arrive_date,
       case
         when l_etips_opt_in_employees.pay_card_status='No' then convert(varchar,'12/31/9999',23)
         when l_etips_opt_in_employees.pay_card_status='Yes' then convert(varchar,'12/31/9999',23)
         else
         null
       end pay_card_end_date,
       isnull(h_etips_opt_in_employees.dv_deleted,0) dv_deleted,
       p_etips_opt_in_employees.p_etips_opt_in_employees_id,
       p_etips_opt_in_employees.dv_batch_id,
       p_etips_opt_in_employees.dv_load_date_time,
       p_etips_opt_in_employees.dv_load_end_date_time
  from dbo.h_etips_opt_in_employees
  join dbo.p_etips_opt_in_employees
    on h_etips_opt_in_employees.bk_hash = p_etips_opt_in_employees.bk_hash
  join #p_etips_opt_in_employees_insert
    on p_etips_opt_in_employees.bk_hash = #p_etips_opt_in_employees_insert.bk_hash
   and p_etips_opt_in_employees.p_etips_opt_in_employees_id = #p_etips_opt_in_employees_insert.p_etips_opt_in_employees_id
  join dbo.l_etips_opt_in_employees
    on p_etips_opt_in_employees.bk_hash = l_etips_opt_in_employees.bk_hash
   and p_etips_opt_in_employees.l_etips_opt_in_employees_id = l_etips_opt_in_employees.l_etips_opt_in_employees_id
  join dbo.s_etips_opt_in_employees
    on p_etips_opt_in_employees.bk_hash = s_etips_opt_in_employees.bk_hash
   and p_etips_opt_in_employees.s_etips_opt_in_employees_id = s_etips_opt_in_employees.s_etips_opt_in_employees_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_etips_opt_in_employees
   where d_etips_opt_in_employees.bk_hash in (select bk_hash from #p_etips_opt_in_employees_insert)

  insert dbo.d_etips_opt_in_employees(
             bk_hash,
             d_etips_opt_in_employees_key,
             employee_id,
             pay_card_start_date,
             pay_card_status,
             ltf_file_name,
             employee_id_pay_start_date_status_key,
             file_arrive_date,
             pay_card_end_date,
             deleted_flag,
             p_etips_opt_in_employees_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_etips_opt_in_employees_key,
         employee_id,
         pay_card_start_date,
         pay_card_status,
         ltf_file_name,
         employee_id_pay_start_date_status_key,
         file_arrive_date,
         pay_card_end_date,
         dv_deleted,
         p_etips_opt_in_employees_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_etips_opt_in_employees)
--Done!
end
