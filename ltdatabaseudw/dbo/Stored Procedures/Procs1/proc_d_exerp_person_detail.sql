CREATE PROC [dbo].[proc_d_exerp_person_detail] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_person_detail)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_person_detail_insert') is not null drop table #p_exerp_person_detail_insert
create table dbo.#p_exerp_person_detail_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_person_detail.p_exerp_person_detail_id,
       p_exerp_person_detail.bk_hash
  from dbo.p_exerp_person_detail
 where p_exerp_person_detail.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_person_detail.dv_batch_id > @max_dv_batch_id
        or p_exerp_person_detail.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_person_detail.bk_hash,
       p_exerp_person_detail.person_id person_id,
       s_exerp_person_detail.address_1 address_1,
       s_exerp_person_detail.address_2 address_2,
       s_exerp_person_detail.address_3 address_3,
       l_exerp_person_detail.center_id center_id,
       case when p_exerp_person_detail.bk_hash in ('-997','-998','-999') then p_exerp_person_detail.bk_hash     
         when l_exerp_person_detail.center_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_person_detail.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       case when p_exerp_person_detail.bk_hash in('-997', '-998', '-999') then p_exerp_person_detail.bk_hash 
              when ((l_exerp_person_detail.person_id is null) OR (l_exerp_person_detail.person_id LIKE '%e%') or (l_exerp_person_detail.person_id LIKE '%OLDe%')
       	    or (len(l_exerp_person_detail.person_id) > 9)  or (d_exerp_person.person_type = 'STAFF' and l_exerp_person_detail.person_id not LIKE '%e%') 
       		  or (d_exerp_person.person_type = 'STAFF')    or (isnumeric(l_exerp_person_detail.person_id) = 0)) then '-998' 
       		       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_person_detail.person_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       s_exerp_person_detail.email email,
       s_exerp_person_detail.ets ets,
       s_exerp_person_detail.first_name first_name,
       s_exerp_person_detail.full_name full_name,
       s_exerp_person_detail.home_phone home_phone,
       s_exerp_person_detail.last_name last_name,
       s_exerp_person_detail.mobile_phone mobile_phone,
       s_exerp_person_detail.work_phone work_phone,
       isnull(h_exerp_person_detail.dv_deleted,0) dv_deleted,
       p_exerp_person_detail.p_exerp_person_detail_id,
       p_exerp_person_detail.dv_batch_id,
       p_exerp_person_detail.dv_load_date_time,
       p_exerp_person_detail.dv_load_end_date_time
  from dbo.h_exerp_person_detail
  join dbo.p_exerp_person_detail
    on h_exerp_person_detail.bk_hash = p_exerp_person_detail.bk_hash
  join #p_exerp_person_detail_insert
    on p_exerp_person_detail.bk_hash = #p_exerp_person_detail_insert.bk_hash
   and p_exerp_person_detail.p_exerp_person_detail_id = #p_exerp_person_detail_insert.p_exerp_person_detail_id
  join dbo.l_exerp_person_detail
    on p_exerp_person_detail.bk_hash = l_exerp_person_detail.bk_hash
   and p_exerp_person_detail.l_exerp_person_detail_id = l_exerp_person_detail.l_exerp_person_detail_id
  join dbo.s_exerp_person_detail
    on p_exerp_person_detail.bk_hash = s_exerp_person_detail.bk_hash
   and p_exerp_person_detail.s_exerp_person_detail_id = s_exerp_person_detail.s_exerp_person_detail_id
 left join 	d_exerp_person		on l_exerp_person_detail.person_id = d_exerp_person.person_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_person_detail
   where d_exerp_person_detail.bk_hash in (select bk_hash from #p_exerp_person_detail_insert)

  insert dbo.d_exerp_person_detail(
             bk_hash,
             person_id,
             address_1,
             address_2,
             address_3,
             center_id,
             dim_club_key,
             dim_mms_member_key,
             email,
             ets,
             first_name,
             full_name,
             home_phone,
             last_name,
             mobile_phone,
             work_phone,
             deleted_flag,
             p_exerp_person_detail_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         person_id,
         address_1,
         address_2,
         address_3,
         center_id,
         dim_club_key,
         dim_mms_member_key,
         email,
         ets,
         first_name,
         full_name,
         home_phone,
         last_name,
         mobile_phone,
         work_phone,
         dv_deleted,
         p_exerp_person_detail_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_person_detail)
--Done!
end
