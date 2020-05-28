CREATE PROC [dbo].[proc_d_mms_membership_attribute] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_membership_attribute)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_membership_attribute_insert') is not null drop table #p_mms_membership_attribute_insert
create table dbo.#p_mms_membership_attribute_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_membership_attribute.p_mms_membership_attribute_id,
       p_mms_membership_attribute.bk_hash
  from dbo.p_mms_membership_attribute
 where p_mms_membership_attribute.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_membership_attribute.dv_batch_id > @max_dv_batch_id
        or p_mms_membership_attribute.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_membership_attribute.bk_hash,
       p_mms_membership_attribute.bk_hash dim_mms_membership_attribute_key,
       p_mms_membership_attribute.membership_attribute_id membership_attribute_id,
       case when p_mms_membership_attribute.bk_hash in ('-997','-998','-999') then p_mms_membership_attribute.bk_hash
            when l_mms_membership_attribute.membership_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_attribute.membership_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_membership_key,
       s_mms_membership_attribute.effective_from_date_time effective_from_date_time,
       s_mms_membership_attribute.effective_thru_date_time effective_thru_date_time,
       s_mms_membership_attribute.attribute_value membership_attribute_value,
       l_mms_membership_attribute.val_membership_attribute_type_id val_membership_attribute_type_id,
       isnull(h_mms_membership_attribute.dv_deleted,0) dv_deleted,
       p_mms_membership_attribute.p_mms_membership_attribute_id,
       p_mms_membership_attribute.dv_batch_id,
       p_mms_membership_attribute.dv_load_date_time,
       p_mms_membership_attribute.dv_load_end_date_time
  from dbo.h_mms_membership_attribute
  join dbo.p_mms_membership_attribute
    on h_mms_membership_attribute.bk_hash = p_mms_membership_attribute.bk_hash
  join #p_mms_membership_attribute_insert
    on p_mms_membership_attribute.bk_hash = #p_mms_membership_attribute_insert.bk_hash
   and p_mms_membership_attribute.p_mms_membership_attribute_id = #p_mms_membership_attribute_insert.p_mms_membership_attribute_id
  join dbo.l_mms_membership_attribute
    on p_mms_membership_attribute.bk_hash = l_mms_membership_attribute.bk_hash
   and p_mms_membership_attribute.l_mms_membership_attribute_id = l_mms_membership_attribute.l_mms_membership_attribute_id
  join dbo.s_mms_membership_attribute
    on p_mms_membership_attribute.bk_hash = s_mms_membership_attribute.bk_hash
   and p_mms_membership_attribute.s_mms_membership_attribute_id = s_mms_membership_attribute.s_mms_membership_attribute_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_membership_attribute
   where d_mms_membership_attribute.bk_hash in (select bk_hash from #p_mms_membership_attribute_insert)

  insert dbo.d_mms_membership_attribute(
             bk_hash,
             dim_mms_membership_attribute_key,
             membership_attribute_id,
             dim_mms_membership_key,
             effective_from_date_time,
             effective_thru_date_time,
             membership_attribute_value,
             val_membership_attribute_type_id,
             deleted_flag,
             p_mms_membership_attribute_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_membership_attribute_key,
         membership_attribute_id,
         dim_mms_membership_key,
         effective_from_date_time,
         effective_thru_date_time,
         membership_attribute_value,
         val_membership_attribute_type_id,
         dv_deleted,
         p_mms_membership_attribute_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_membership_attribute)
--Done!
end
