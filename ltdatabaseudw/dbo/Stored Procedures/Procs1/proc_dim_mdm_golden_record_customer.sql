CREATE PROC [dbo].[proc_dim_mdm_golden_record_customer] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @dv_batch_id as current_dv_batch_id
  from dbo.dim_mdm_golden_record_customer

if object_id('tempdb..#p_mdm_golden_record_customer_insert') is not null drop table #p_mdm_golden_record_customer_insert
create table dbo.#p_mdm_golden_record_customer_insert with(distribution=hash(entity_id), location=user_db, heap) as
select p_mdm_golden_record_customer.p_mdm_golden_record_customer_id,
       p_mdm_golden_record_customer.entity_id, 
       p_mdm_golden_record_customer.source_id,
	   p_mdm_golden_record_customer.source_code,
       p_mdm_golden_record_customer.bk_hash,
	   row_number() over (order by p_mdm_golden_record_customer_id) row_num
  from dbo.p_mdm_golden_record_customer
  join #dv_batch_id
    on p_mdm_golden_record_customer.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or p_mdm_golden_record_customer.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where p_mdm_golden_record_customer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- FormerMemberFlag
--We are joining on Entity_id for performance reasons
if object_id('tempdb..#FormerMemberFlag') is not null drop table #FormerMemberFlag
create table dbo.#FormerMemberFlag with (location = user_db, distribution = hash(entity_id)) as
select count(distinct l_mdm_golden_record_customer.member_id) memberidcount, 
       l_mdm_golden_record_customer.entity_id
  from p_mdm_golden_record_customer
  join #p_mdm_golden_record_customer_insert
    on p_mdm_golden_record_customer.p_mdm_golden_record_customer_id = #p_mdm_golden_record_customer_insert.p_mdm_golden_record_customer_id
   and p_mdm_golden_record_customer.entity_id = #p_mdm_golden_record_customer_insert.entity_id
  join l_mdm_golden_record_customer
    on p_mdm_golden_record_customer.l_mdm_golden_record_customer_id = l_mdm_golden_record_customer.l_mdm_golden_record_customer_id
   and p_mdm_golden_record_customer.entity_id = l_mdm_golden_record_customer.entity_id
 where p_mdm_golden_record_customer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
 group by l_mdm_golden_record_customer.entity_id
 having count(distinct member_id) > 1


if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with (location = user_db, distribution = hash(entity_id)) as
select #p_mdm_golden_record_customer_insert.row_num,
       p_mdm_golden_record_customer.entity_id entity_id,
       s_mdm_golden_record_customer.first_name,
	   s_mdm_golden_record_customer.middle_name,
	   s_mdm_golden_record_customer.last_name,
	   s_mdm_golden_record_customer.prefix_name,
	   s_mdm_golden_record_customer.suffix_name,
	   s_mdm_golden_record_customer.postal_address_line_1,
	   s_mdm_golden_record_customer.postal_address_line_2,
	   s_mdm_golden_record_customer.postal_address_city,
	   s_mdm_golden_record_customer.postal_address_state,
	   s_mdm_golden_record_customer.postal_address_zip_code,
	   s_mdm_golden_record_customer.phone_1,
	   s_mdm_golden_record_customer.email_1,
	   s_mdm_golden_record_customer.birth_date,
	   s_mdm_golden_record_customer.sex,
	   case
	      when #FormerMemberFlag.memberidcount is not null then 'Y'
		  else 'N'
	   end former_member_flag,
	   p_mdm_golden_record_customer.dv_load_date_time,
	   p_mdm_golden_record_customer.dv_load_end_date_time,
	   p_mdm_golden_record_customer.dv_batch_id
  from p_mdm_golden_record_customer
  join #p_mdm_golden_record_customer_insert
    on p_mdm_golden_record_customer.p_mdm_golden_record_customer_id = #p_mdm_golden_record_customer_insert.p_mdm_golden_record_customer_id
   and p_mdm_golden_record_customer.entity_id = #p_mdm_golden_record_customer_insert.entity_id
  join s_mdm_golden_record_customer
    on p_mdm_golden_record_customer.s_mdm_golden_record_customer_id = s_mdm_golden_record_customer.s_mdm_golden_record_customer_id
   and p_mdm_golden_record_customer.entity_id = s_mdm_golden_record_customer.entity_id
  left join #FormerMemberFlag
    on p_mdm_golden_record_customer.entity_id = #FormerMemberFlag.entity_id
   --and p_mdm_golden_record_customer.source_id = #FormerMemberFlag.source_id
   --and p_mdm_golden_record_customer.source_code = #FormerMemberFlag.source_code
 where p_mdm_golden_record_customer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (s_mdm_golden_record_customer.first_name is not null or s_mdm_golden_record_customer.last_name is not null)


--figure out which entity_id records we need to delete from dimension
if object_id('tempdb..#delete') is not null drop table #delete
create table dbo.#delete with (location = user_db, distribution = hash(entity_id)) as
select entity_id,
       sum(dv_deleted ^ 1) deletedsum
  from h_mdm_golden_record_customer
group by entity_id
having sum(dv_deleted ^ 1) = 0

--delete the entity_id that no longer valid from dimension
delete dbo.dim_mdm_golden_record_customer
   where entity_id in (select entity_id from #delete)



-- Delete and re-insert
-- Do as a single transaction
--   Delete records from the dim table that exist
--   Insert records from current and missing batches

begin tran
  delete dbo.dim_mdm_golden_record_customer
   where entity_id in (select entity_id from #insert)

  insert dbo.dim_mdm_golden_record_customer(
               dim_mdm_golden_record_customer_key,
               entity_id,
			   birth_date,
			   email_1,
               first_name,
			   former_member_flag,
			   last_name,
               middle_name,
			   phone_1,       
			   postal_address_city,        
               postal_address_line_1,
	           postal_address_line_2,	          
	           postal_address_state,
	           postal_address_zip_code,
	           prefix_name,
			   sex,
			   suffix_name,		   
               dv_load_date_time,
               dv_load_end_date_time,
               dv_batch_id,
               dv_inserted_date_time,
               dv_insert_user)
  select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#insert.entity_id,'z#@$k%&P'))),2),
         #insert.entity_id,
		 #insert.birth_date,
		 #insert.email_1,
         #insert.first_name,
		 #insert.former_member_flag,
		 #insert.last_name,
         #insert.middle_name,
		 #insert.phone_1,
		 #insert.postal_address_city,
         #insert.postal_address_line_1,
	     #insert.postal_address_line_2,	     
	     #insert.postal_address_state,
	     #insert.postal_address_zip_code,	     	          
         #insert.prefix_name,
		 #insert.sex,
		 #insert.suffix_name,	 
         #insert.dv_load_date_time,
         #insert.dv_load_end_date_time,
         #insert.dv_batch_id,
         getdate(),
         suser_sname()
    from #insert

commit tran

end
