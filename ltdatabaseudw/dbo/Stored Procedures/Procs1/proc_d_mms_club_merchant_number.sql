CREATE PROC [dbo].[proc_d_mms_club_merchant_number] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_club_merchant_number)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_club_merchant_number_insert') is not null drop table #p_mms_club_merchant_number_insert
create table dbo.#p_mms_club_merchant_number_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_club_merchant_number.p_mms_club_merchant_number_id,
       p_mms_club_merchant_number.bk_hash
  from dbo.p_mms_club_merchant_number
 where p_mms_club_merchant_number.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_club_merchant_number.dv_batch_id > @max_dv_batch_id
        or p_mms_club_merchant_number.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_club_merchant_number.bk_hash,
       p_mms_club_merchant_number.bk_hash dim_mms_merchant_number_key,
       p_mms_club_merchant_number.club_merchant_number_id club_merchant_number_id,
       case when cast(s_mms_club_merchant_number.auto_reconcile_flag as varchar(10))='1' then 'Y'
            else 'N'
        end auto_reconcile_flag,
       case when p_mms_club_merchant_number.bk_hash in ('-997','-998','-999') then p_mms_club_merchant_number.bk_hash
            when l_mms_club_merchant_number.val_business_area_id is null then '-998'
            when l_mms_club_merchant_number.val_business_area_id in (0) then '-998'
            else 'r_mms_val_business_area_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club_merchant_number.val_business_area_id as smallint) as varchar(500)),'z#@$k%&P'))),2)
       end business_area_dim_description_key,
       isnull(r.currency_code,'-998') currency_code,
       case when p_mms_club_merchant_number.bk_hash in ('-997','-998','-999') then p_mms_club_merchant_number.bk_hash
            when l_mms_club_merchant_number.club_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club_merchant_number.club_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       s_mms_club_merchant_number.merchant_location_number merchant_location_number,
       l_mms_club_merchant_number.merchant_number merchant_number,
       isnull(cast(l_mms_club_merchant_number.val_business_area_id as int),'-998') val_business_area_id,
       isnull(cast(l_mms_club_merchant_number.val_currency_code_id as int),'-998') val_currency_code_id,
       isnull(h_mms_club_merchant_number.dv_deleted,0) dv_deleted,
       p_mms_club_merchant_number.p_mms_club_merchant_number_id,
       p_mms_club_merchant_number.dv_batch_id,
       p_mms_club_merchant_number.dv_load_date_time,
       p_mms_club_merchant_number.dv_load_end_date_time
  from dbo.h_mms_club_merchant_number
  join dbo.p_mms_club_merchant_number
    on h_mms_club_merchant_number.bk_hash = p_mms_club_merchant_number.bk_hash
  join #p_mms_club_merchant_number_insert
    on p_mms_club_merchant_number.bk_hash = #p_mms_club_merchant_number_insert.bk_hash
   and p_mms_club_merchant_number.p_mms_club_merchant_number_id = #p_mms_club_merchant_number_insert.p_mms_club_merchant_number_id
  join dbo.l_mms_club_merchant_number
    on p_mms_club_merchant_number.bk_hash = l_mms_club_merchant_number.bk_hash
   and p_mms_club_merchant_number.l_mms_club_merchant_number_id = l_mms_club_merchant_number.l_mms_club_merchant_number_id
  join dbo.s_mms_club_merchant_number
    on p_mms_club_merchant_number.bk_hash = s_mms_club_merchant_number.bk_hash
   and p_mms_club_merchant_number.s_mms_club_merchant_number_id = s_mms_club_merchant_number.s_mms_club_merchant_number_id
 join r_mms_val_currency_code r      on isnull(cast(l_mms_club_merchant_number.val_currency_code_id as int),'-998') = r.val_currency_code_id 

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_club_merchant_number
   where d_mms_club_merchant_number.bk_hash in (select bk_hash from #p_mms_club_merchant_number_insert)

  insert dbo.d_mms_club_merchant_number(
             bk_hash,
             dim_mms_merchant_number_key,
             club_merchant_number_id,
             auto_reconcile_flag,
             business_area_dim_description_key,
             currency_code,
             dim_club_key,
             merchant_location_number,
             merchant_number,
             val_business_area_id,
             val_currency_code_id,
             deleted_flag,
             p_mms_club_merchant_number_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_merchant_number_key,
         club_merchant_number_id,
         auto_reconcile_flag,
         business_area_dim_description_key,
         currency_code,
         dim_club_key,
         merchant_location_number,
         merchant_number,
         val_business_area_id,
         val_currency_code_id,
         dv_deleted,
         p_mms_club_merchant_number_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_club_merchant_number)
--Done!
end
