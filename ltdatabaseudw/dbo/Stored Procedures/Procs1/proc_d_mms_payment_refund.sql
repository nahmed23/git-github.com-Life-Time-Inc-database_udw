CREATE PROC [dbo].[proc_d_mms_payment_refund] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_payment_refund)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_payment_refund_insert') is not null drop table #p_mms_payment_refund_insert
create table dbo.#p_mms_payment_refund_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_payment_refund.p_mms_payment_refund_id,
       p_mms_payment_refund.bk_hash
  from dbo.p_mms_payment_refund
 where p_mms_payment_refund.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_payment_refund.dv_batch_id > @max_dv_batch_id
        or p_mms_payment_refund.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_payment_refund.bk_hash,
       p_mms_payment_refund.payment_refund_id payment_refund_id,
       s_mms_payment_refund.comment comment,
       case when p_mms_payment_refund.bk_hash in ('-997','-998','-999') then p_mms_payment_refund.bk_hash     
         when l_mms_payment_refund.payment_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_payment_refund.payment_id as int) as varchar(500)),'z#@$k%&P'))),2)   end fact_mms_payment_key,
       s_mms_payment_refund.inserted_date_time inserted_date_time,
       case when p_mms_payment_refund.bk_hash in('-997', '-998', '-999') then p_mms_payment_refund.bk_hash
           when s_mms_payment_refund.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_payment_refund.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_payment_refund.bk_hash in ('-997','-998','-999') then p_mms_payment_refund.bk_hash
       when s_mms_payment_refund.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_payment_refund.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       l_mms_payment_refund.payment_id payment_id,
       s_mms_payment_refund.payment_issued_date_time payment_issued_date_time,
       case when p_mms_payment_refund.bk_hash in('-997', '-998', '-999') then p_mms_payment_refund.bk_hash
           when s_mms_payment_refund.payment_issued_date_time is null then '-998'
        else convert(varchar, s_mms_payment_refund.payment_issued_date_time, 112)    end payment_issued_dim_date_key,
       case when p_mms_payment_refund.bk_hash in ('-997','-998','-999') then p_mms_payment_refund.bk_hash
       when s_mms_payment_refund.payment_issued_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_payment_refund.payment_issued_date_time,114), 1, 5),':','') end payment_issued_dim_time_key,
       case when p_mms_payment_refund.bk_hash in ('-997','-998','-999') then p_mms_payment_refund.bk_hash     
         when l_mms_payment_refund.val_payment_status_id is null then '-998'   
         else 'r_mms_val_payment_status_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_payment_refund.val_payment_status_id as int) as varchar(500)),'z#@$k%&P'))),2)   end payment_status_dim_description_key,
       s_mms_payment_refund.reference_number reference_number,
       s_mms_payment_refund.status_change_date_time status_change_date_time,
       s_mms_payment_refund.status_change_date_time_zone status_change_date_time_zone,
       case when p_mms_payment_refund.bk_hash in('-997', '-998', '-999') then p_mms_payment_refund.bk_hash
           when s_mms_payment_refund.status_change_date_time is null then '-998'
        else convert(varchar, s_mms_payment_refund.status_change_date_time, 112)    end status_change_dim_date_key,
       case when p_mms_payment_refund.bk_hash in ('-997','-998','-999') then p_mms_payment_refund.bk_hash     
         when l_mms_payment_refund.status_change_employee_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_payment_refund.status_change_employee_id as int) as varchar(500)),'z#@$k%&P'))),2)   end status_change_dim_employee_key,
       case when p_mms_payment_refund.bk_hash in ('-997','-998','-999') then p_mms_payment_refund.bk_hash
       when s_mms_payment_refund.status_change_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_payment_refund.status_change_date_time,114), 1, 5),':','') end status_change_dim_time_key,
       l_mms_payment_refund.status_change_employee_id status_change_employee_id,
       s_mms_payment_refund.updated_date_time updated_date_time,
       case when p_mms_payment_refund.bk_hash in('-997', '-998', '-999') then p_mms_payment_refund.bk_hash
           when s_mms_payment_refund.updated_date_time is null then '-998'
        else convert(varchar, s_mms_payment_refund.updated_date_time, 112)    end updated_dim_date_key,
       case when p_mms_payment_refund.bk_hash in ('-997','-998','-999') then p_mms_payment_refund.bk_hash
       when s_mms_payment_refund.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_payment_refund.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_mms_payment_refund.utc_status_change_date_time utc_status_change_date_time,
       case when p_mms_payment_refund.bk_hash in('-997', '-998', '-999') then p_mms_payment_refund.bk_hash
           when s_mms_payment_refund.utc_status_change_date_time is null then '-998'
        else convert(varchar, s_mms_payment_refund.utc_status_change_date_time, 112)    end utc_status_change_dim_date_key,
       case when p_mms_payment_refund.bk_hash in ('-997','-998','-999') then p_mms_payment_refund.bk_hash
       when s_mms_payment_refund.utc_status_change_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_payment_refund.utc_status_change_date_time,114), 1, 5),':','') end utc_status_change_dim_time_key,
       l_mms_payment_refund.val_payment_status_id val_payment_status_id,
       isnull(h_mms_payment_refund.dv_deleted,0) dv_deleted,
       p_mms_payment_refund.p_mms_payment_refund_id,
       p_mms_payment_refund.dv_batch_id,
       p_mms_payment_refund.dv_load_date_time,
       p_mms_payment_refund.dv_load_end_date_time
  from dbo.h_mms_payment_refund
  join dbo.p_mms_payment_refund
    on h_mms_payment_refund.bk_hash = p_mms_payment_refund.bk_hash
  join #p_mms_payment_refund_insert
    on p_mms_payment_refund.bk_hash = #p_mms_payment_refund_insert.bk_hash
   and p_mms_payment_refund.p_mms_payment_refund_id = #p_mms_payment_refund_insert.p_mms_payment_refund_id
  join dbo.l_mms_payment_refund
    on p_mms_payment_refund.bk_hash = l_mms_payment_refund.bk_hash
   and p_mms_payment_refund.l_mms_payment_refund_id = l_mms_payment_refund.l_mms_payment_refund_id
  join dbo.s_mms_payment_refund
    on p_mms_payment_refund.bk_hash = s_mms_payment_refund.bk_hash
   and p_mms_payment_refund.s_mms_payment_refund_id = s_mms_payment_refund.s_mms_payment_refund_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_payment_refund
   where d_mms_payment_refund.bk_hash in (select bk_hash from #p_mms_payment_refund_insert)

  insert dbo.d_mms_payment_refund(
             bk_hash,
             payment_refund_id,
             comment,
             fact_mms_payment_key,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             payment_id,
             payment_issued_date_time,
             payment_issued_dim_date_key,
             payment_issued_dim_time_key,
             payment_status_dim_description_key,
             reference_number,
             status_change_date_time,
             status_change_date_time_zone,
             status_change_dim_date_key,
             status_change_dim_employee_key,
             status_change_dim_time_key,
             status_change_employee_id,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             utc_status_change_date_time,
             utc_status_change_dim_date_key,
             utc_status_change_dim_time_key,
             val_payment_status_id,
             deleted_flag,
             p_mms_payment_refund_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         payment_refund_id,
         comment,
         fact_mms_payment_key,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         payment_id,
         payment_issued_date_time,
         payment_issued_dim_date_key,
         payment_issued_dim_time_key,
         payment_status_dim_description_key,
         reference_number,
         status_change_date_time,
         status_change_date_time_zone,
         status_change_dim_date_key,
         status_change_dim_employee_key,
         status_change_dim_time_key,
         status_change_employee_id,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         utc_status_change_date_time,
         utc_status_change_dim_date_key,
         utc_status_change_dim_time_key,
         val_payment_status_id,
         dv_deleted,
         p_mms_payment_refund_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_payment_refund)
--Done!
end
