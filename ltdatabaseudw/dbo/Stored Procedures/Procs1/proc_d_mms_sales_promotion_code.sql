CREATE PROC [dbo].[proc_d_mms_sales_promotion_code] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_sales_promotion_code)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_sales_promotion_code_insert') is not null drop table #p_mms_sales_promotion_code_insert
create table dbo.#p_mms_sales_promotion_code_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_sales_promotion_code.p_mms_sales_promotion_code_id,
       p_mms_sales_promotion_code.bk_hash
  from dbo.p_mms_sales_promotion_code
 where p_mms_sales_promotion_code.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_sales_promotion_code.dv_batch_id > @max_dv_batch_id
        or p_mms_sales_promotion_code.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_sales_promotion_code.bk_hash,
       p_mms_sales_promotion_code.sales_promotion_code_id sales_promotion_code_id,
       case when p_mms_sales_promotion_code.bk_hash in ('-997','-998','-999') then p_mms_sales_promotion_code.bk_hash     
         when l_mms_sales_promotion_code.member_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_sales_promotion_code.member_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_member_key,
       case when p_mms_sales_promotion_code.bk_hash in ('-997','-998','-999') then p_mms_sales_promotion_code.bk_hash     
         when l_mms_sales_promotion_code.sales_promotion_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_sales_promotion_code.sales_promotion_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_sales_promotion_key,
       case when s_mms_sales_promotion_code.display_ui_flag = 1  then 'Y' else 'N' end display_ui_flag,
       case when p_mms_sales_promotion_code.bk_hash in('-997', '-998', '-999') then p_mms_sales_promotion_code.bk_hash
           when s_mms_sales_promotion_code.expiration_date is null then '-998'
        else convert(varchar, s_mms_sales_promotion_code.expiration_date, 112)    end expiration_dim_date_key,
       case when p_mms_sales_promotion_code.bk_hash in ('-997','-998','-999') then p_mms_sales_promotion_code.bk_hash
       when s_mms_sales_promotion_code.expiration_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_sales_promotion_code.expiration_date,114), 1, 5),':','') end expiration_dim_time_key,
       case when p_mms_sales_promotion_code.bk_hash in ('-997', '-998', '-999') then p_mms_sales_promotion_code.bk_hash
           when s_mms_sales_promotion_code.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_sales_promotion_code.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_sales_promotion_code.bk_hash in ('-997','-998','-999') then p_mms_sales_promotion_code.bk_hash
       when s_mms_sales_promotion_code.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_sales_promotion_code.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       l_mms_sales_promotion_code.member_id member_id,
       s_mms_sales_promotion_code.notify_email_address notify_email_address,
       s_mms_sales_promotion_code.number_of_code_recipients number_of_code_recipients,
       s_mms_sales_promotion_code.promotion_code promotion_code,
       s_mms_sales_promotion_code.usage_limit sales_promotion_code_usage_limit,
       l_mms_sales_promotion_code.sales_promotion_id sales_promotion_id,
       case when p_mms_sales_promotion_code.bk_hash in ('-997', '-998', '-999') then p_mms_sales_promotion_code.bk_hash
           when s_mms_sales_promotion_code.updated_date_time is null then '-998'
        else convert(varchar, s_mms_sales_promotion_code.updated_date_time, 112)    end updated_dim_date_key,
       case when p_mms_sales_promotion_code.bk_hash in ('-997','-998','-999') then p_mms_sales_promotion_code.bk_hash
       when s_mms_sales_promotion_code.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_sales_promotion_code.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_mms_sales_promotion_code.dv_deleted,0) dv_deleted,
       p_mms_sales_promotion_code.p_mms_sales_promotion_code_id,
       p_mms_sales_promotion_code.dv_batch_id,
       p_mms_sales_promotion_code.dv_load_date_time,
       p_mms_sales_promotion_code.dv_load_end_date_time
  from dbo.h_mms_sales_promotion_code
  join dbo.p_mms_sales_promotion_code
    on h_mms_sales_promotion_code.bk_hash = p_mms_sales_promotion_code.bk_hash
  join #p_mms_sales_promotion_code_insert
    on p_mms_sales_promotion_code.bk_hash = #p_mms_sales_promotion_code_insert.bk_hash
   and p_mms_sales_promotion_code.p_mms_sales_promotion_code_id = #p_mms_sales_promotion_code_insert.p_mms_sales_promotion_code_id
  join dbo.l_mms_sales_promotion_code
    on p_mms_sales_promotion_code.bk_hash = l_mms_sales_promotion_code.bk_hash
   and p_mms_sales_promotion_code.l_mms_sales_promotion_code_id = l_mms_sales_promotion_code.l_mms_sales_promotion_code_id
  join dbo.s_mms_sales_promotion_code
    on p_mms_sales_promotion_code.bk_hash = s_mms_sales_promotion_code.bk_hash
   and p_mms_sales_promotion_code.s_mms_sales_promotion_code_id = s_mms_sales_promotion_code.s_mms_sales_promotion_code_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_sales_promotion_code
   where d_mms_sales_promotion_code.bk_hash in (select bk_hash from #p_mms_sales_promotion_code_insert)

  insert dbo.d_mms_sales_promotion_code(
             bk_hash,
             sales_promotion_code_id,
             dim_mms_member_key,
             dim_mms_sales_promotion_key,
             display_ui_flag,
             expiration_dim_date_key,
             expiration_dim_time_key,
             inserted_dim_date_key,
             inserted_dim_time_key,
             member_id,
             notify_email_address,
             number_of_code_recipients,
             promotion_code,
             sales_promotion_code_usage_limit,
             sales_promotion_id,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_mms_sales_promotion_code_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         sales_promotion_code_id,
         dim_mms_member_key,
         dim_mms_sales_promotion_key,
         display_ui_flag,
         expiration_dim_date_key,
         expiration_dim_time_key,
         inserted_dim_date_key,
         inserted_dim_time_key,
         member_id,
         notify_email_address,
         number_of_code_recipients,
         promotion_code,
         sales_promotion_code_usage_limit,
         sales_promotion_id,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_mms_sales_promotion_code_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_sales_promotion_code)
--Done!
end
