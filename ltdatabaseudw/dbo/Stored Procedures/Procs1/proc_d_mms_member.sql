CREATE PROC [dbo].[proc_d_mms_member] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_member)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_member_insert') is not null drop table #p_mms_member_insert
create table dbo.#p_mms_member_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_member.p_mms_member_id,
       p_mms_member.bk_hash
  from dbo.p_mms_member
 where p_mms_member.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_member.dv_batch_id > @max_dv_batch_id
        or p_mms_member.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_member.bk_hash,
       p_mms_member.bk_hash dim_mms_member_key,
       p_mms_member.member_id member_id,
       case when s_mms_member.assess_jr_member_dues_flag = 0 then 'N'
              else 'Y'  
       	    end assess_junior_member_dues_flag,
       case when isnull(s_mms_member.first_name,'') != '' and isnull(s_mms_member.last_name,'')!=''
       then s_mms_member.first_name+' '+s_mms_member.last_name 
       when isnull(s_mms_member.first_name,'') ='' then isnull(s_mms_member.last_name,'') 
       else isnull(s_mms_member.first_name,'')  end customer_name,
       case when isnull(s_mms_member.first_name, '') != '' and isnull(s_mms_member.last_name, '') != '' 
        then s_mms_member.last_name + ', ' + s_mms_member.first_name  
        when isnull(s_mms_member.first_name, '') = ''   
        then isnull(s_mms_member.last_name, '') 
        else isnull(s_mms_member.first_name, '')   end customer_name_last_first,
       case when s_mms_member.dob is null or s_mms_member.dob < convert(datetime, '1900.01.01', 102) or s_mms_member.dob >= convert(datetime, '2100.01.01', 102) 
                  then null  
       		  else s_mms_member.dob   end date_of_birth,
       isnull (r_mms_val_member_type.description, '') description_member,
       case when p_mms_member.bk_hash in ('-997','-998','-999') 
                  then p_mms_member.bk_hash     
       		     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_member.membership_id as int) as varchar(500)),'z#@$k%&P'))),2) 
       	  end dim_mms_membership_key,
       isnull(ltrim(rtrim(s_mms_member.email_address)),'') email_address,
       isnull(s_mms_member.first_name,'') first_name,
       case when s_mms_member.gender is null or s_mms_member.gender not in ('m', 'f') 
                  then 'U'
       		   else upper(s_mms_member.gender) 
       		     end gender_abbreviation,
       case when s_mms_member.join_date >= convert(datetime, '2100.01.01', 102) 
       then convert(datetime, '9999.12.31', 102)     
       else s_mms_member.join_date
          end join_date,
       case when p_mms_member.bk_hash in ('-997', '-998', '-999') then p_mms_member.bk_hash
       when s_mms_member.join_date is null then '-998'
        else convert(varchar,  case when s_mms_member.join_date >= convert(datetime, '2100.01.01', 102)   then convert(datetime, '9999.12.31', 102)
        else s_mms_member.join_date   end, 112)  end join_date_key,
       isnull (s_mms_member.last_name,'') last_name,
       case when s_mms_member.active_flag = 1 then 'Y'
              else 'N'   
       	   end member_active_flag,
       case when p_mms_member.bk_hash in ('-997','-998','-999') then p_mms_member.bk_hash
            when  l_mms_member.val_member_type_id is null then '-998'
            else 'r_mms_val_member_type_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_member.val_member_type_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end member_type_dim_description_key,
       l_mms_member.membership_id membership_id,
       l_mms_member.party_id party_id,
       l_mms_member.val_member_type_id val_member_type_id,
       isnull(h_mms_member.dv_deleted,0) dv_deleted,
       p_mms_member.p_mms_member_id,
       p_mms_member.dv_batch_id,
       p_mms_member.dv_load_date_time,
       p_mms_member.dv_load_end_date_time
  from dbo.h_mms_member
  join dbo.p_mms_member
    on h_mms_member.bk_hash = p_mms_member.bk_hash
  join #p_mms_member_insert
    on p_mms_member.bk_hash = #p_mms_member_insert.bk_hash
   and p_mms_member.p_mms_member_id = #p_mms_member_insert.p_mms_member_id
  join dbo.l_mms_member
    on p_mms_member.bk_hash = l_mms_member.bk_hash
   and p_mms_member.l_mms_member_id = l_mms_member.l_mms_member_id
  join dbo.s_mms_member
    on p_mms_member.bk_hash = s_mms_member.bk_hash
   and p_mms_member.s_mms_member_id = s_mms_member.s_mms_member_id
 left join r_mms_val_member_type    
  on l_mms_member.val_member_type_id = r_mms_val_member_type.val_member_type_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_member
   where d_mms_member.bk_hash in (select bk_hash from #p_mms_member_insert)

  insert dbo.d_mms_member(
             bk_hash,
             dim_mms_member_key,
             member_id,
             assess_junior_member_dues_flag,
             customer_name,
             customer_name_last_first,
             date_of_birth,
             description_member,
             dim_mms_membership_key,
             email_address,
             first_name,
             gender_abbreviation,
             join_date,
             join_date_key,
             last_name,
             member_active_flag,
             member_type_dim_description_key,
             membership_id,
             party_id,
             val_member_type_id,
             deleted_flag,
             p_mms_member_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_member_key,
         member_id,
         assess_junior_member_dues_flag,
         customer_name,
         customer_name_last_first,
         date_of_birth,
         description_member,
         dim_mms_membership_key,
         email_address,
         first_name,
         gender_abbreviation,
         join_date,
         join_date_key,
         last_name,
         member_active_flag,
         member_type_dim_description_key,
         membership_id,
         party_id,
         val_member_type_id,
         dv_deleted,
         p_mms_member_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_member)
--Done!
end
