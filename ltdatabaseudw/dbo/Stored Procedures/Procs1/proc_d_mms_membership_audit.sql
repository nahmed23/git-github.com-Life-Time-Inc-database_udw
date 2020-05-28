CREATE PROC [dbo].[proc_d_mms_membership_audit] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_membership_audit)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_membership_audit_insert') is not null drop table #p_mms_membership_audit_insert
create table dbo.#p_mms_membership_audit_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_membership_audit.p_mms_membership_audit_id,
       p_mms_membership_audit.bk_hash
  from dbo.p_mms_membership_audit
 where p_mms_membership_audit.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_membership_audit.dv_batch_id > @max_dv_batch_id
        or p_mms_membership_audit.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_membership_audit.bk_hash,
       p_mms_membership_audit.bk_hash fact_mms_membership_audit_key,
       p_mms_membership_audit.membership_audit_id membership_audit_id,
       case when p_mms_membership_audit.bk_hash in ('-997', '-998', '-999') then p_mms_membership_audit.bk_hash
              when s_mms_membership_audit.modified_date_time is null then '-998'
                else convert(varchar, s_mms_membership_audit.modified_date_time, 112)
              end modified_dim_date_key,
       case when s_mms_membership_audit.modified_user is null then '-998'
              when isnumeric(s_mms_membership_audit.modified_user) = 0 then '-998'
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_mms_membership_audit.modified_user as int) as varchar(500)),'z#@$k%&P'))),2)
             end modified_dim_employee_key,
       case when p_mms_membership_audit.bk_hash in ('-997', '-998', '-999') then p_mms_membership_audit.bk_hash
              when s_mms_membership_audit.modified_date_time is null then '-998'
               else '1' + replace(substring(convert(varchar,s_mms_membership_audit.modified_date_time,114), 1, 5),':','')
            end modified_dim_time_key,
       s_mms_membership_audit.new_value new_value,
       s_mms_membership_audit.old_value old_value,
       s_mms_membership_audit.column_name source_column_name,
       case when p_mms_membership_audit.bk_hash in ('-997','-998','-999') then p_mms_membership_audit.bk_hash
              when l_mms_membership_audit.row_id is null then '-998'
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership_audit.row_id as varchar(500)),'z#@$k%&P'))),2)
             end source_row_key,
       case when s_mms_membership_audit.operation = 'UPDATE' then 'Y'
         else 'N'
        end update_flag,
       p_mms_membership_audit.p_mms_membership_audit_id,
       p_mms_membership_audit.dv_batch_id,
       p_mms_membership_audit.dv_load_date_time,
       p_mms_membership_audit.dv_load_end_date_time
  from dbo.h_mms_membership_audit
  join dbo.p_mms_membership_audit
    on h_mms_membership_audit.bk_hash = p_mms_membership_audit.bk_hash  join #p_mms_membership_audit_insert
    on p_mms_membership_audit.bk_hash = #p_mms_membership_audit_insert.bk_hash
   and p_mms_membership_audit.p_mms_membership_audit_id = #p_mms_membership_audit_insert.p_mms_membership_audit_id
  join dbo.l_mms_membership_audit
    on p_mms_membership_audit.bk_hash = l_mms_membership_audit.bk_hash
   and p_mms_membership_audit.l_mms_membership_audit_id = l_mms_membership_audit.l_mms_membership_audit_id
  join dbo.s_mms_membership_audit
    on p_mms_membership_audit.bk_hash = s_mms_membership_audit.bk_hash
   and p_mms_membership_audit.s_mms_membership_audit_id = s_mms_membership_audit.s_mms_membership_audit_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_membership_audit
   where d_mms_membership_audit.bk_hash in (select bk_hash from #p_mms_membership_audit_insert)

  insert dbo.d_mms_membership_audit(
             bk_hash,
             fact_mms_membership_audit_key,
             membership_audit_id,
             modified_dim_date_key,
             modified_dim_employee_key,
             modified_dim_time_key,
             new_value,
             old_value,
             source_column_name,
             source_row_key,
             update_flag,
             p_mms_membership_audit_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_membership_audit_key,
         membership_audit_id,
         modified_dim_date_key,
         modified_dim_employee_key,
         modified_dim_time_key,
         new_value,
         old_value,
         source_column_name,
         source_row_key,
         update_flag,
         p_mms_membership_audit_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_membership_audit)
--Done!
end
