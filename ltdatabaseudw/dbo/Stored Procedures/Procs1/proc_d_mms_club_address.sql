CREATE PROC [dbo].[proc_d_mms_club_address] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_club_address)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_club_address_insert') is not null drop table #p_mms_club_address_insert
create table dbo.#p_mms_club_address_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_club_address.p_mms_club_address_id,
       p_mms_club_address.bk_hash
  from dbo.p_mms_club_address
 where p_mms_club_address.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_club_address.dv_batch_id > @max_dv_batch_id
        or p_mms_club_address.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_club_address.bk_hash,
       p_mms_club_address.bk_hash dim_mms_club_address_key,
       p_mms_club_address.club_address_id club_address_id,
       s_mms_club_address.address_line1 address_line_1,
       s_mms_club_address.address_line2 address_line_2,
       s_mms_club_address.city city,
       l_mms_club_address.club_id club_id,
       case when p_mms_club_address.bk_hash in ('-997','-998','-999') then p_mms_club_address.bk_hash
            when l_mms_club_address.val_country_id is null then '-998'
            else 'r_mms_val_country_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_club_address.val_country_id as varchar(500)),'z#@$k%&P'))),2)
        end country_dim_description_key,
       case when p_mms_club_address.bk_hash in ('-997','-998','-999') then p_mms_club_address.bk_hash
            when l_mms_club_address.club_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_club_address.club_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       s_mms_club_address.latitude latitude,
       s_mms_club_address.longitude longitude,
       s_mms_club_address.zip_code postal_code,
       case when p_mms_club_address.bk_hash in ('-997','-998','-999') then p_mms_club_address.bk_hash
            when l_mms_club_address.val_state_id is null then '-998'
            else 'r_mms_val_state_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_club_address.val_state_id as varchar(500)),'z#@$k%&P'))),2)
        end state_dim_description_key,
       l_mms_club_address.val_address_type_id val_address_type_id,
       p_mms_club_address.p_mms_club_address_id,
       p_mms_club_address.dv_batch_id,
       p_mms_club_address.dv_load_date_time,
       p_mms_club_address.dv_load_end_date_time
  from dbo.h_mms_club_address
  join dbo.p_mms_club_address
    on h_mms_club_address.bk_hash = p_mms_club_address.bk_hash  join #p_mms_club_address_insert
    on p_mms_club_address.bk_hash = #p_mms_club_address_insert.bk_hash
   and p_mms_club_address.p_mms_club_address_id = #p_mms_club_address_insert.p_mms_club_address_id
  join dbo.l_mms_club_address
    on p_mms_club_address.bk_hash = l_mms_club_address.bk_hash
   and p_mms_club_address.l_mms_club_address_id = l_mms_club_address.l_mms_club_address_id
  join dbo.s_mms_club_address
    on p_mms_club_address.bk_hash = s_mms_club_address.bk_hash
   and p_mms_club_address.s_mms_club_address_id = s_mms_club_address.s_mms_club_address_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_club_address
   where d_mms_club_address.bk_hash in (select bk_hash from #p_mms_club_address_insert)

  insert dbo.d_mms_club_address(
             bk_hash,
             dim_mms_club_address_key,
             club_address_id,
             address_line_1,
             address_line_2,
             city,
             club_id,
             country_dim_description_key,
             dim_club_key,
             latitude,
             longitude,
             postal_code,
             state_dim_description_key,
             val_address_type_id,
             p_mms_club_address_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_club_address_key,
         club_address_id,
         address_line_1,
         address_line_2,
         city,
         club_id,
         country_dim_description_key,
         dim_club_key,
         latitude,
         longitude,
         postal_code,
         state_dim_description_key,
         val_address_type_id,
         p_mms_club_address_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_club_address)
--Done!
end
