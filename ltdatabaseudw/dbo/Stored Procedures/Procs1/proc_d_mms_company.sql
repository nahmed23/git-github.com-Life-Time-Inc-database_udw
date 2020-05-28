CREATE PROC [dbo].[proc_d_mms_company] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_company)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_company_insert') is not null drop table #p_mms_company_insert
create table dbo.#p_mms_company_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_company.p_mms_company_id,
       p_mms_company.bk_hash
  from dbo.p_mms_company
 where p_mms_company.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_company.dv_batch_id > @max_dv_batch_id
        or p_mms_company.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_company.bk_hash,
       p_mms_company.bk_hash dim_mms_company_key,
       p_mms_company.company_id company_id,
       isnull(s_mms_company.account_rep_name,'') account_rep_name,
       isnull(s_mms_company.company_name,'') company_name,
       s_mms_company.corporate_code corporate_code,
       case when p_mms_company.bk_hash in ('-997','-998','-999') then 'N'
            when s_mms_company.eft_account_number is null then 'N'
            else 'Y'
        end eft_account_number_on_file_flag,
       case when p_mms_company.bk_hash in ('-997','-998','-999') then 'N'
            when s_mms_company.invoice_flag = 1 then 'Y'
            else 'N'
        end invoice_flag,
       isnull(s_mms_company.report_to_email_address,'') report_to_email_address,
       case when p_mms_company.bk_hash in ('-997','-998','-999') then 'N'
            when s_mms_company.small_business_flag = 1 then 'Y'
            else 'N'
        end small_business_flag,
       case when p_mms_company.bk_hash in ('-997','-998','-999') then 'N'
            when s_mms_company.usage_report_flag = 1 then 'Y'
            else 'N'
        end usage_report_flag,
       isnull(s_mms_company.usage_report_member_type,'') usage_report_member_type,
       p_mms_company.p_mms_company_id,
       p_mms_company.dv_batch_id,
       p_mms_company.dv_load_date_time,
       p_mms_company.dv_load_end_date_time
  from dbo.p_mms_company
  join #p_mms_company_insert
    on p_mms_company.bk_hash = #p_mms_company_insert.bk_hash
   and p_mms_company.p_mms_company_id = #p_mms_company_insert.p_mms_company_id
  join dbo.s_mms_company
    on p_mms_company.bk_hash = s_mms_company.bk_hash
   and p_mms_company.s_mms_company_id = s_mms_company.s_mms_company_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_company
   where d_mms_company.bk_hash in (select bk_hash from #p_mms_company_insert)

  insert dbo.d_mms_company(
             bk_hash,
             dim_mms_company_key,
             company_id,
             account_rep_name,
             company_name,
             corporate_code,
             eft_account_number_on_file_flag,
             invoice_flag,
             report_to_email_address,
             small_business_flag,
             usage_report_flag,
             usage_report_member_type,
             p_mms_company_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_company_key,
         company_id,
         account_rep_name,
         company_name,
         corporate_code,
         eft_account_number_on_file_flag,
         invoice_flag,
         report_to_email_address,
         small_business_flag,
         usage_report_flag,
         usage_report_member_type,
         p_mms_company_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_company)
--Done!
end
