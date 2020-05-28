CREATE PROC [dbo].[proc_d_hybris_cat_2_prodrel] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_hybris_cat_2_prodrel)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_hybris_cat_2_prodrel_insert') is not null drop table #p_hybris_cat_2_prodrel_insert
create table dbo.#p_hybris_cat_2_prodrel_insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_cat_2_prodrel.p_hybris_cat_2_prodrel_id,
       p_hybris_cat_2_prodrel.bk_hash
  from dbo.p_hybris_cat_2_prodrel
 where p_hybris_cat_2_prodrel.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_hybris_cat_2_prodrel.dv_batch_id > @max_dv_batch_id
        or p_hybris_cat_2_prodrel.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_cat_2_prodrel.bk_hash,
       p_hybris_cat_2_prodrel.bk_hash d_hybris_cat_2_prodrel_key,
       s_hybris_cat_2_prodrel.acl_ts acl_ts,
       p_hybris_cat_2_prodrel.cat_2_prodrel_pk cat_2_prodrel_pk,
       s_hybris_cat_2_prodrel.created_ts created_ts,
       s_hybris_cat_2_prodrel.hjmpts hjmpts,
       l_hybris_cat_2_prodrel.language_pk language_pk,
       s_hybris_cat_2_prodrel.modified_ts modified_ts,
       l_hybris_cat_2_prodrel.Owner_Pk_String Owner_Pk_String,
       s_hybris_cat_2_prodrel.prop_ts prop_ts,
       s_hybris_cat_2_prodrel.qualifier qualifier,
       s_hybris_cat_2_prodrel.r_sequence_number r_sequence_number,
       s_hybris_cat_2_prodrel.sequence_number sequence_number,
       l_hybris_cat_2_prodrel.source_pk source_pk,
       l_hybris_cat_2_prodrel.target_pk target_pk,
       l_hybris_cat_2_prodrel.type_pk_string type_pk_string,
       p_hybris_cat_2_prodrel.p_hybris_cat_2_prodrel_id,
       p_hybris_cat_2_prodrel.dv_batch_id,
       p_hybris_cat_2_prodrel.dv_load_date_time,
       p_hybris_cat_2_prodrel.dv_load_end_date_time
  from dbo.h_hybris_cat_2_prodrel
  join dbo.p_hybris_cat_2_prodrel
    on h_hybris_cat_2_prodrel.bk_hash = p_hybris_cat_2_prodrel.bk_hash  join #p_hybris_cat_2_prodrel_insert
    on p_hybris_cat_2_prodrel.bk_hash = #p_hybris_cat_2_prodrel_insert.bk_hash
   and p_hybris_cat_2_prodrel.p_hybris_cat_2_prodrel_id = #p_hybris_cat_2_prodrel_insert.p_hybris_cat_2_prodrel_id
  join dbo.l_hybris_cat_2_prodrel
    on p_hybris_cat_2_prodrel.bk_hash = l_hybris_cat_2_prodrel.bk_hash
   and p_hybris_cat_2_prodrel.l_hybris_cat_2_prodrel_id = l_hybris_cat_2_prodrel.l_hybris_cat_2_prodrel_id
  join dbo.s_hybris_cat_2_prodrel
    on p_hybris_cat_2_prodrel.bk_hash = s_hybris_cat_2_prodrel.bk_hash
   and p_hybris_cat_2_prodrel.s_hybris_cat_2_prodrel_id = s_hybris_cat_2_prodrel.s_hybris_cat_2_prodrel_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_hybris_cat_2_prodrel
   where d_hybris_cat_2_prodrel.bk_hash in (select bk_hash from #p_hybris_cat_2_prodrel_insert)

  insert dbo.d_hybris_cat_2_prodrel(
             bk_hash,
             d_hybris_cat_2_prodrel_key,
             acl_ts,
             cat_2_prodrel_pk,
             created_ts,
             hjmpts,
             language_pk,
             modified_ts,
             Owner_Pk_String,
             prop_ts,
             qualifier,
             r_sequence_number,
             sequence_number,
             source_pk,
             target_pk,
             type_pk_string,
             p_hybris_cat_2_prodrel_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_hybris_cat_2_prodrel_key,
         acl_ts,
         cat_2_prodrel_pk,
         created_ts,
         hjmpts,
         language_pk,
         modified_ts,
         Owner_Pk_String,
         prop_ts,
         qualifier,
         r_sequence_number,
         sequence_number,
         source_pk,
         target_pk,
         type_pk_string,
         p_hybris_cat_2_prodrel_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_hybris_cat_2_prodrel)
--Done!
end
