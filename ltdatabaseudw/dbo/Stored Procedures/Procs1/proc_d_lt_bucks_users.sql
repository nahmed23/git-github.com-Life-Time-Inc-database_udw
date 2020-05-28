CREATE PROC [dbo].[proc_d_lt_bucks_users] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_users)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_lt_bucks_users_insert') is not null drop table #p_lt_bucks_users_insert
create table dbo.#p_lt_bucks_users_insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_users.p_lt_bucks_users_id,
       p_lt_bucks_users.bk_hash
  from dbo.p_lt_bucks_users
 where p_lt_bucks_users.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_lt_bucks_users.dv_batch_id > @max_dv_batch_id
        or p_lt_bucks_users.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_users.bk_hash,
       p_lt_bucks_users.bk_hash dim_lt_bucks_users_key,
       p_lt_bucks_users.user_id user_id,
       case when p_lt_bucks_users.bk_hash in ('-997','-998','-999') then 'N'
            when s_lt_bucks_users.user_active = 1 then 'Y'
            else 'N'
        end active_flag,
       case when s_lt_bucks_users.user_city is null then ''
            else s_lt_bucks_users.user_city
        end address_city,
       case when s_lt_bucks_users.user_addr1 is null then ''
            else s_lt_bucks_users.user_addr1
        end address_line_1,
       case when s_lt_bucks_users.user_addr2 is null then ''
            else s_lt_bucks_users.user_addr2
        end address_line_2,
       case when s_lt_bucks_users.user_zip is null then ''
            else s_lt_bucks_users.user_zip
        end address_postal_code,
       case when s_lt_bucks_users.user_state is null then ''
            else s_lt_bucks_users.user_state
        end address_state,
       case when s_lt_bucks_users.user_curr_points is null then 0
            else s_lt_bucks_users.user_curr_points
        end current_points,
       case when l_lt_bucks_users.user_dist_id is not null
             and isnumeric(l_lt_bucks_users.user_dist_id) = 1 
       	  and convert(int, l_lt_bucks_users.user_dist_id) > 100000000 
             and len(l_lt_bucks_users.user_dist_id) = 9
       	then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_users.user_dist_id as varchar(500)),'z#@$k%&P'))),2)
            else '-998'
        end dim_mms_member_key,
       case when s_lt_bucks_users.user_email is null then ''
            else s_lt_bucks_users.user_email
        end email,
       case when s_lt_bucks_users.user_fname is null then ''
            else s_lt_bucks_users.user_fname
        end first_name,
       case when s_lt_bucks_users.user_lname is null then ''
            else s_lt_bucks_users.user_lname
        end last_name,
       case when l_lt_bucks_users.user_parent is null 
             or l_lt_bucks_users.user_parent=0
              then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_users.user_parent as varchar(500)),'z#@$k%&P'))),2)
       end referring_dim_lt_bucks_user_key,
       case when p_lt_bucks_users.bk_hash in ('-997','-998','-999') then null
            when s_lt_bucks_users.user_register_date < convert(datetime, '1900.01.01', 102) then null
       	 when s_lt_bucks_users.user_register_date >= convert(datetime, '2101.01.01', 102) then null
            else s_lt_bucks_users.user_register_date
        end register_date_time,
       l_lt_bucks_users.user_parent user_parent,
       case when s_lt_bucks_users.user_phone is null then ''  else s_lt_bucks_users.user_phone   end user_phone,
       case when s_lt_bucks_users.user_promotion = 227 then 'member'
            when s_lt_bucks_users.user_promotion = 215 then 'admin - access to portal'
       	 when s_lt_bucks_users.user_promotion = 28 then 'admin - eGroup'	 
            else convert(varchar, s_lt_bucks_users.user_promotion)
        end user_type,
       case when s_lt_bucks_users.user_username is null then ''  else s_lt_bucks_users.user_username   end user_username,
       p_lt_bucks_users.p_lt_bucks_users_id,
       p_lt_bucks_users.dv_batch_id,
       p_lt_bucks_users.dv_load_date_time,
       p_lt_bucks_users.dv_load_end_date_time
  from dbo.h_lt_bucks_users
  join dbo.p_lt_bucks_users
    on h_lt_bucks_users.bk_hash = p_lt_bucks_users.bk_hash  join #p_lt_bucks_users_insert
    on p_lt_bucks_users.bk_hash = #p_lt_bucks_users_insert.bk_hash
   and p_lt_bucks_users.p_lt_bucks_users_id = #p_lt_bucks_users_insert.p_lt_bucks_users_id
  join dbo.l_lt_bucks_users
    on p_lt_bucks_users.bk_hash = l_lt_bucks_users.bk_hash
   and p_lt_bucks_users.l_lt_bucks_users_id = l_lt_bucks_users.l_lt_bucks_users_id
  join dbo.s_lt_bucks_users
    on p_lt_bucks_users.bk_hash = s_lt_bucks_users.bk_hash
   and p_lt_bucks_users.s_lt_bucks_users_id = s_lt_bucks_users.s_lt_bucks_users_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_lt_bucks_users
   where d_lt_bucks_users.bk_hash in (select bk_hash from #p_lt_bucks_users_insert)

  insert dbo.d_lt_bucks_users(
             bk_hash,
             dim_lt_bucks_users_key,
             user_id,
             active_flag,
             address_city,
             address_line_1,
             address_line_2,
             address_postal_code,
             address_state,
             current_points,
             dim_mms_member_key,
             email,
             first_name,
             last_name,
             referring_dim_lt_bucks_user_key,
             register_date_time,
             user_parent,
             user_phone,
             user_type,
             user_username,
             p_lt_bucks_users_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_lt_bucks_users_key,
         user_id,
         active_flag,
         address_city,
         address_line_1,
         address_line_2,
         address_postal_code,
         address_state,
         current_points,
         dim_mms_member_key,
         email,
         first_name,
         last_name,
         referring_dim_lt_bucks_user_key,
         register_date_time,
         user_parent,
         user_phone,
         user_type,
         user_username,
         p_lt_bucks_users_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_users)
--Done!
end
