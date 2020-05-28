CREATE PROC [dbo].[proc_d_boss_employees] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_employees)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_employees_insert') is not null drop table #p_boss_employees_insert
create table dbo.#p_boss_employees_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_employees.p_boss_employees_id,
       p_boss_employees.bk_hash
  from dbo.p_boss_employees
 where p_boss_employees.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_employees.dv_batch_id > @max_dv_batch_id
        or p_boss_employees.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_employees.bk_hash,
       p_boss_employees.employee_id employee_id,
       s_boss_employees.cost cost,
       case when p_boss_employees.bk_hash in('-997', '-998', '-999') then p_boss_employees.bk_hash
           when l_boss_employees.interestID is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_employees.interestID as int) as varchar(500)),'z#@$k%&P'))),2)   end d_boss_interest_bk_hash,
       s_boss_employees.employee_url employee_url,
       s_boss_employees.badge employees_badge,
       s_boss_employees.email employees_email,
       s_boss_employees.first employees_first,
       s_boss_employees.last employees_last,
       s_boss_employees.nickname employees_nickname,
       s_boss_employees.status employees_status,
       s_boss_employees.user_profile employees_user_profile,
       l_boss_employees.home_club home_club,
       l_boss_employees.id id,
       l_boss_employees.interestID interest_id,
       l_boss_employees.member_ID member_ID,
       s_boss_employees.MI MI,
       s_boss_employees.phone phone,
       s_boss_employees.res_color res_color,
       l_boss_employees.roleID role_id,
       isnull(h_boss_employees.dv_deleted,0) dv_deleted,
       p_boss_employees.p_boss_employees_id,
       p_boss_employees.dv_batch_id,
       p_boss_employees.dv_load_date_time,
       p_boss_employees.dv_load_end_date_time
  from dbo.h_boss_employees
  join dbo.p_boss_employees
    on h_boss_employees.bk_hash = p_boss_employees.bk_hash
  join #p_boss_employees_insert
    on p_boss_employees.bk_hash = #p_boss_employees_insert.bk_hash
   and p_boss_employees.p_boss_employees_id = #p_boss_employees_insert.p_boss_employees_id
  join dbo.l_boss_employees
    on p_boss_employees.bk_hash = l_boss_employees.bk_hash
   and p_boss_employees.l_boss_employees_id = l_boss_employees.l_boss_employees_id
  join dbo.s_boss_employees
    on p_boss_employees.bk_hash = s_boss_employees.bk_hash
   and p_boss_employees.s_boss_employees_id = s_boss_employees.s_boss_employees_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_employees
   where d_boss_employees.bk_hash in (select bk_hash from #p_boss_employees_insert)

  insert dbo.d_boss_employees(
             bk_hash,
             employee_id,
             cost,
             d_boss_interest_bk_hash,
             employee_url,
             employees_badge,
             employees_email,
             employees_first,
             employees_last,
             employees_nickname,
             employees_status,
             employees_user_profile,
             home_club,
             id,
             interest_id,
             member_ID,
             MI,
             phone,
             res_color,
             role_id,
             deleted_flag,
             p_boss_employees_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         employee_id,
         cost,
         d_boss_interest_bk_hash,
         employee_url,
         employees_badge,
         employees_email,
         employees_first,
         employees_last,
         employees_nickname,
         employees_status,
         employees_user_profile,
         home_club,
         id,
         interest_id,
         member_ID,
         MI,
         phone,
         res_color,
         role_id,
         dv_deleted,
         p_boss_employees_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_employees)
--Done!
end
