CREATE PROC [dbo].[proc_d_healthcheckusa_transactions] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_healthcheckusa_transactions)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_healthcheckusa_transactions_insert') is not null drop table #p_healthcheckusa_transactions_insert
create table dbo.#p_healthcheckusa_transactions_insert with(distribution=hash(bk_hash), location=user_db) as
select p_healthcheckusa_transactions.p_healthcheckusa_transactions_id,
       p_healthcheckusa_transactions.bk_hash
  from dbo.p_healthcheckusa_transactions
 where p_healthcheckusa_transactions.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_healthcheckusa_transactions.dv_batch_id > @max_dv_batch_id
        or p_healthcheckusa_transactions.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_healthcheckusa_transactions.bk_hash,
       p_healthcheckusa_transactions.bk_hash d_healthcheckusa_transactions_bk_hash,
       p_healthcheckusa_transactions.order_number order_number,
       p_healthcheckusa_transactions.sku product_sku,
       convert(varchar(8),DATEADD(mm,DATEDIFF(mm,0,s_healthcheckusa_transactions.transaction_date),0),112) allocated_month_starting_dim_date_key,
       dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,s_healthcheckusa_transactions.transaction_date)),0)) allocated_recalculate_through_datetime,
       convert(varchar(8),dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,s_healthcheckusa_transactions.transaction_date)),0)),112) allocated_recalculate_through_dim_date_key,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast('HealthCheckUSA' as varchar(255)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast('HealthCheckUSA' as varchar(255)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast('HealthCheckUSA' as varchar(255)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast('' as varchar(255)),'z#@$k%&P'))),2) default_dim_reporting_hierarchy_key,
       case when p_healthcheckusa_transactions.bk_hash in ('-997','-998','-999') then p_healthcheckusa_transactions.bk_hash   when l_healthcheckusa_transactions.ltf_employee_id is not null then  convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_healthcheckusa_transactions.ltf_employee_id as int) as varchar(500)),'z#@$k%&P'))),2) else '-998'   end dim_employee_key,
       case when p_healthcheckusa_transactions.bk_hash in ('-997', '-998','-999') then p_healthcheckusa_transactions.bk_hash
            when l_healthcheckusa_transactions.sku is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_healthcheckusa_transactions.sku as nvarchar(4000)),'z#@$k%&P'))),2)
        end dim_healthcheckusa_product_key,
       l_healthcheckusa_transactions.ltf_employee_id employee_id,
       l_healthcheckusa_transactions.ltf_gl_club_id gl_club_id,
       s_healthcheckusa_transactions.item_amount item_amount,
       s_healthcheckusa_transactions.item_discount item_discount,
       s_healthcheckusa_transactions.order_for_employee_flag order_for_employee_flag,
       s_healthcheckusa_transactions.quantity quantity,
       s_healthcheckusa_transactions.transaction_date transaction_date,
       case when p_healthcheckusa_transactions.bk_hash in ('-997', '-998', '-999') then p_healthcheckusa_transactions.bk_hash
                   when s_healthcheckusa_transactions.transaction_date is null then '-998'
                   else convert(varchar, s_healthcheckusa_transactions.transaction_date, 112)  
               end transaction_post_dim_date_key,
       case when s_healthcheckusa_transactions.transaction_date is null then  '-998'
       else '1'+ replace(substring(convert(varchar,convert(datetime,s_healthcheckusa_transactions.transaction_date,126),114), 1, 5),':','')  end transaction_post_dim_time_key,
       s_healthcheckusa_transactions.transaction_type transaction_type,
       isnull(h_healthcheckusa_transactions.dv_deleted,0) dv_deleted,
       p_healthcheckusa_transactions.p_healthcheckusa_transactions_id,
       p_healthcheckusa_transactions.dv_batch_id,
       p_healthcheckusa_transactions.dv_load_date_time,
       p_healthcheckusa_transactions.dv_load_end_date_time
  from dbo.h_healthcheckusa_transactions
  join dbo.p_healthcheckusa_transactions
    on h_healthcheckusa_transactions.bk_hash = p_healthcheckusa_transactions.bk_hash
  join #p_healthcheckusa_transactions_insert
    on p_healthcheckusa_transactions.bk_hash = #p_healthcheckusa_transactions_insert.bk_hash
   and p_healthcheckusa_transactions.p_healthcheckusa_transactions_id = #p_healthcheckusa_transactions_insert.p_healthcheckusa_transactions_id
  join dbo.l_healthcheckusa_transactions
    on p_healthcheckusa_transactions.bk_hash = l_healthcheckusa_transactions.bk_hash
   and p_healthcheckusa_transactions.l_healthcheckusa_transactions_id = l_healthcheckusa_transactions.l_healthcheckusa_transactions_id
  join dbo.s_healthcheckusa_transactions
    on p_healthcheckusa_transactions.bk_hash = s_healthcheckusa_transactions.bk_hash
   and p_healthcheckusa_transactions.s_healthcheckusa_transactions_id = s_healthcheckusa_transactions.s_healthcheckusa_transactions_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_healthcheckusa_transactions
   where d_healthcheckusa_transactions.bk_hash in (select bk_hash from #p_healthcheckusa_transactions_insert)

  insert dbo.d_healthcheckusa_transactions(
             bk_hash,
             d_healthcheckusa_transactions_bk_hash,
             order_number,
             product_sku,
             allocated_month_starting_dim_date_key,
             allocated_recalculate_through_datetime,
             allocated_recalculate_through_dim_date_key,
             default_dim_reporting_hierarchy_key,
             dim_employee_key,
             dim_healthcheckusa_product_key,
             employee_id,
             gl_club_id,
             item_amount,
             item_discount,
             order_for_employee_flag,
             quantity,
             transaction_date,
             transaction_post_dim_date_key,
             transaction_post_dim_time_key,
             transaction_type,
             deleted_flag,
             p_healthcheckusa_transactions_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_healthcheckusa_transactions_bk_hash,
         order_number,
         product_sku,
         allocated_month_starting_dim_date_key,
         allocated_recalculate_through_datetime,
         allocated_recalculate_through_dim_date_key,
         default_dim_reporting_hierarchy_key,
         dim_employee_key,
         dim_healthcheckusa_product_key,
         employee_id,
         gl_club_id,
         item_amount,
         item_discount,
         order_for_employee_flag,
         quantity,
         transaction_date,
         transaction_post_dim_date_key,
         transaction_post_dim_time_key,
         transaction_type,
         dv_deleted,
         p_healthcheckusa_transactions_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_healthcheckusa_transactions)
--Done!
end
