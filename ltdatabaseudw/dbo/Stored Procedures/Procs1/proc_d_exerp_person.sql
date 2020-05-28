CREATE PROC [dbo].[proc_d_exerp_person] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_person)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_person_insert') is not null drop table #p_exerp_person_insert
create table dbo.#p_exerp_person_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_person.p_exerp_person_id,
       p_exerp_person.bk_hash
  from dbo.p_exerp_person
 where p_exerp_person.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_person.dv_batch_id > @max_dv_batch_id
        or p_exerp_person.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_person.bk_hash,
       p_exerp_person.person_id person_id,
       case when s_exerp_person.can_email = 1 then 'Y'
             else 'N' end can_email_flag,
       case when s_exerp_person.can_sms = 1 then 'Y'
             else 'N'  end can_sms_flag,
       s_exerp_person.city city,
       s_exerp_person.company_id company_id,
       s_exerp_person.country_id country_id,
       s_exerp_person.county county,
       case when p_exerp_person.bk_hash in('-997', '-998', '-999') then p_exerp_person.bk_hash
           when s_exerp_person.creation_date is null then '-998'
        else convert(varchar, s_exerp_person.creation_date, 112)    end creation_dim_date_key,
       case when p_exerp_person.bk_hash in ('-997','-998','-999') then p_exerp_person.bk_hash     
         when l_exerp_person.center_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_person.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_center_bk_hash,
       case when p_exerp_person.bk_hash in('-997', '-998', '-999') then p_exerp_person.bk_hash
           when s_exerp_person.date_of_birth is null then '-998'
        else convert(varchar, s_exerp_person.date_of_birth, 112)   end date_of_birth_dim_date_key,
       case when p_exerp_person.bk_hash in('-997', '-998', '-999') then p_exerp_person.bk_hash    
         when p_exerp_person.person_id is null then '-998'   
          when s_exerp_person.person_type = 'STAFF' 
          then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(substring(p_exerp_person.person_id, PATINDEX('%[0-9]%',p_exerp_person.person_id), 500) as int) as varchar(500)),'z#@$k%&P'))),2) 
          else '-998' end dim_employee_key,
       case when p_exerp_person.bk_hash in('-997', '-998', '-999') then p_exerp_person.bk_hash 
              when ((l_exerp_person.person_id is null) OR (l_exerp_person.person_id LIKE '%e%') or (l_exerp_person.person_id LIKE '%OLDe%')
       	    or (len(l_exerp_person.person_id) > 9)   or (s_exerp_person.person_type = 'STAFF' and l_exerp_person.person_id not LIKE '%e%') 
       		  or (s_exerp_person.person_type = 'STAFF') or (isnumeric(l_exerp_person.person_id) = 0)) then '-998' 
       		       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_person.person_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       s_exerp_person.duplicate_of_person_id duplicate_of_person_id,
       s_exerp_person.employee_title employee_title,
       s_exerp_person.ets ets,
       s_exerp_person.gender gender,
       l_exerp_person.home_center_person_id home_center_person_id,
       case when p_exerp_person.bk_hash in ('-997','-998','-999') then p_exerp_person.bk_hash     
         when l_exerp_person.home_center_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_person.home_center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end home_d_exerp_center_bk_hash,
       s_exerp_person.payer_person_id payer_person_id,
       s_exerp_person.person_status person_status,
       s_exerp_person.person_type person_type,
       s_exerp_person.postal_code postal_code,
       s_exerp_person.staff_external_id staff_external_id,
       s_exerp_person.state state,
       s_exerp_person.title title,
       isnull(h_exerp_person.dv_deleted,0) dv_deleted,
       p_exerp_person.p_exerp_person_id,
       p_exerp_person.dv_batch_id,
       p_exerp_person.dv_load_date_time,
       p_exerp_person.dv_load_end_date_time
  from dbo.h_exerp_person
  join dbo.p_exerp_person
    on h_exerp_person.bk_hash = p_exerp_person.bk_hash
  join #p_exerp_person_insert
    on p_exerp_person.bk_hash = #p_exerp_person_insert.bk_hash
   and p_exerp_person.p_exerp_person_id = #p_exerp_person_insert.p_exerp_person_id
  join dbo.l_exerp_person
    on p_exerp_person.bk_hash = l_exerp_person.bk_hash
   and p_exerp_person.l_exerp_person_id = l_exerp_person.l_exerp_person_id
  join dbo.s_exerp_person
    on p_exerp_person.bk_hash = s_exerp_person.bk_hash
   and p_exerp_person.s_exerp_person_id = s_exerp_person.s_exerp_person_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_person
   where d_exerp_person.bk_hash in (select bk_hash from #p_exerp_person_insert)

  insert dbo.d_exerp_person(
             bk_hash,
             person_id,
             can_email_flag,
             can_sms_flag,
             city,
             company_id,
             country_id,
             county,
             creation_dim_date_key,
             d_exerp_center_bk_hash,
             date_of_birth_dim_date_key,
             dim_employee_key,
             dim_mms_member_key,
             duplicate_of_person_id,
             employee_title,
             ets,
             gender,
             home_center_person_id,
             home_d_exerp_center_bk_hash,
             payer_person_id,
             person_status,
             person_type,
             postal_code,
             staff_external_id,
             state,
             title,
             deleted_flag,
             p_exerp_person_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         person_id,
         can_email_flag,
         can_sms_flag,
         city,
         company_id,
         country_id,
         county,
         creation_dim_date_key,
         d_exerp_center_bk_hash,
         date_of_birth_dim_date_key,
         dim_employee_key,
         dim_mms_member_key,
         duplicate_of_person_id,
         employee_title,
         ets,
         gender,
         home_center_person_id,
         home_d_exerp_center_bk_hash,
         payer_person_id,
         person_status,
         person_type,
         postal_code,
         staff_external_id,
         state,
         title,
         dv_deleted,
         p_exerp_person_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_person)
--Done!
end
