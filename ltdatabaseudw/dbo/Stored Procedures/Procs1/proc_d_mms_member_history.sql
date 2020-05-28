CREATE PROC [dbo].[proc_d_mms_member_history] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_member_history);

if object_id('tempdb..#p_mms_member_id_list') is not null drop table #p_mms_member_id_list
create table dbo.#p_mms_member_id_list with(distribution=hash(bk_hash), location=user_db, heap) as
with undo_delete (p_mms_member_id,bk_hash,dv_load_date_time) as 
(
-- Find any updates with the current batch ID to undo in case of retry - just like in the PIT Proc
--   Find the records in the current batch
--   Find the records related to the current batch
--   Note that this needs to be done using the PIT ids within the fact/dimension base table since a workflow retry would have deleted those IDs from the PIT table and reinserted with new IDs
    select p_mms_member_id,
           bk_hash,
           dv_load_date_time
      from dbo.d_mms_member_history
     where dv_batch_id = @current_dv_batch_id
),
undo_update (p_mms_member_id,bk_hash) as
(
    select d_mms_member_history.p_mms_member_id,
           d_mms_member_history.bk_hash
      from dbo.d_mms_member_history
      join undo_delete
        on d_mms_member_history.bk_hash = undo_delete.bk_hash
       and d_mms_member_history.dv_load_end_date_time = undo_delete.dv_load_date_time
),
p_mms_member_insert (p_mms_member_id,bk_hash,dv_load_date_time) as 
(
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those have not been physically deleted yet - if they exist
-- Then find the PIT ids in the PIT table that correspond to the dimension/fact records to end-date
    select p_mms_member_id,
           bk_hash,
           dv_load_date_time
      from dbo.p_mms_member
     where dv_batch_id > @max_dv_batch_id
        or dv_batch_id = @current_dv_batch_id
),
p_mms_member_update (p_mms_member_id,bk_hash) as
(
    select p_mms_member.p_mms_member_id,
           p_mms_member.bk_hash
      from dbo.p_mms_member
      join p_mms_member_insert
        on p_mms_member.bk_hash = p_mms_member_insert.bk_hash
       and p_mms_member.dv_load_end_date_time = p_mms_member_insert.dv_load_date_time
)
select undo_delete.p_mms_member_id,
       bk_hash
  from undo_delete
union
select undo_update.p_mms_member_id,
       bk_hash
  from undo_update
union
select p_mms_member_insert.p_mms_member_id,
       bk_hash
  from p_mms_member_insert
union
select p_mms_member_update.p_mms_member_id,
       bk_hash
  from p_mms_member_update

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db, heap) as
select #p_mms_member_id_list.bk_hash,
       p_mms_member.bk_hash dim_mms_member_key,
       p_mms_member.member_id member_id,
       isnull(p_mms_member.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102)) effective_date_time,
       case when p_mms_member.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
        then p_mms_member.dv_load_end_date_time     
        else p_mms_member.dv_next_greatest_satellite_date_time    end expiration_date_time,
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
       		     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_member.membership_id as varchar(500)),'z#@$k%&P'))),2) 
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
       isnull (s_mms_member.last_name,'') last_name,
       case when s_mms_member.active_flag = 1 then 'Y'
              else 'N'   
       	   end member_active_flag,
       case when p_mms_member.bk_hash in ('-997','-998','-999') then p_mms_member.bk_hash
            when  l_mms_member.val_member_type_id is null then '-998'
            else 'r_mms_val_member_type_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_member.val_member_type_id as varchar(500)),'z#@$k%&P'))),2)
       end member_type_dim_description_key,
       l_mms_member.membership_id membership_id,
       l_mms_member.val_member_type_id val_member_type_id,
       p_mms_member.p_mms_member_id,
       p_mms_member.dv_batch_id,
       p_mms_member.dv_load_date_time,
       p_mms_member.dv_load_end_date_time
  from dbo.p_mms_member
  join #p_mms_member_id_list
    on p_mms_member.p_mms_member_id = #p_mms_member_id_list.p_mms_member_id
   and p_mms_member.bk_hash = #p_mms_member_id_list.bk_hash
  join dbo.l_mms_member
    on p_mms_member.bk_hash = l_mms_member.bk_hash
   and p_mms_member.l_mms_member_id = l_mms_member.l_mms_member_id
  join dbo.s_mms_member
    on p_mms_member.bk_hash = s_mms_member.bk_hash
   and p_mms_member.s_mms_member_id = s_mms_member.s_mms_member_id
 left join r_mms_val_member_type    
  on l_mms_member.val_member_type_id = r_mms_val_member_type.val_member_type_id
   and isnull(p_mms_member.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102))!= case when p_mms_member.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
 then p_mms_member.dv_load_end_date_time     
 else p_mms_member.dv_next_greatest_satellite_date_time    end


-- do as a single transaction
--   delete records from dimension where PIT_id = #PIT.PIT_id
--     Note that this also gets rid of any records where the existing effective_date_time equals the soon to be newly calculated expiration_date_time
--   insert records from all of the joins to the pit table and to #PIT.PIT_id
    begin tran
      delete dbo.d_mms_member_history
       where d_mms_member_history.p_mms_member_id in (select p_mms_member_id from #p_mms_member_id_list)

      insert dbo.d_mms_member_history(
                 bk_hash,
                 dim_mms_member_key,
                 member_id,
                 effective_date_time,
                 expiration_date_time,
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
                 last_name,
                 member_active_flag,
                 member_type_dim_description_key,
                 membership_id,
                 val_member_type_id,
                 p_mms_member_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select bk_hash,
             dim_mms_member_key,
             member_id,
             effective_date_time,
             expiration_date_time,
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
             last_name,
             member_active_flag,
             member_type_dim_description_key,
             membership_id,
             val_member_type_id,
             p_mms_member_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
    commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_member_history)
--Done!
end
