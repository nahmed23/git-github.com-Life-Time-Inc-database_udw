CREATE PROC [dbo].[proc_dim_mdm_golden_record_customer_email_list] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @dv_batch_id as current_dv_batch_id
  from dbo.dim_mdm_golden_record_customer_email_list

if object_id('tempdb..#p_mdm_golden_record_customer_insert') is not null drop table #p_mdm_golden_record_customer_insert
create table dbo.#p_mdm_golden_record_customer_insert with(distribution=hash(entity_id), location=user_db) as
select p_mdm_golden_record_customer.p_mdm_golden_record_customer_id,
       p_mdm_golden_record_customer.entity_id, 
       p_mdm_golden_record_customer.bk_hash,
	   row_number() over (order by p_mdm_golden_record_customer_id) row_num
  from dbo.p_mdm_golden_record_customer
  join #dv_batch_id
    on p_mdm_golden_record_customer.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or p_mdm_golden_record_customer.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where p_mdm_golden_record_customer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)


if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with (distribution = hash(entity_id), location = user_db) as

with 
goldenrecordcustomer_Email_list (row_num, entity_id, email_1, email_2, dv_load_date_time, dv_load_end_date_time, dv_batch_id) as

   (select #p_mdm_golden_record_customer_insert.row_num,
           p_mdm_golden_record_customer.entity_id,
           s_mdm_golden_record_customer.email_1, 
           s_mdm_golden_record_customer.email_2,
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
     where p_mdm_golden_record_customer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
       and (s_mdm_golden_record_customer.email_1 is not null or s_mdm_golden_record_customer.email_2 is not null)
   )

select row_num,
       entity_id,
       email_1 as email,
	   'Primary' as type,
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id
  from goldenrecordcustomer_Email_list
where email_1 is not null
union
select row_num,
       entity_id,
       email_2 as email,
	   'Secondary' as type,
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id
  from goldenrecordcustomer_Email_list
where email_2 is not null



--figure out which entity_id records we need to delete from dimension
if object_id('tempdb..#delete') is not null drop table #delete
create table dbo.#delete with (location = user_db, distribution = hash(entity_id)) as
select entity_id,
       sum(dv_deleted ^ 1) deletedsum
  from h_mdm_golden_record_customer
group by entity_id
having sum(dv_deleted ^ 1) = 0

--delete the entity_id that no longer valid from dimension
delete dbo.dim_mdm_golden_record_customer_email_list
   where entity_id in (select entity_id from #delete)


declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'dim_mdm_golden_record_customer_email_list', @id_count = @insert_count, @start_id = @start_p_id out

-- Delete and re-insert
-- Do as a single transaction
--   Delete records from the dim table that exist
--   Insert records from current and missing batches

begin tran
  delete dbo.dim_mdm_golden_record_customer_email_list
   where entity_id in (select entity_id from #insert where row_num >= @start and row_num < @start+1000000)

  insert dbo.dim_mdm_golden_record_customer_email_list(
               dim_mdm_golden_record_customer_email_list_key,
			   entity_id,
               email,
			   type,
               dv_load_date_time,
               dv_load_end_date_time,
               dv_batch_id,
               dv_inserted_date_time,
               dv_insert_user)
  select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#insert.entity_id,'z#@$k%&P'))),2),
         #insert.entity_id,
         #insert.email,
         #insert.type,
         #insert.dv_load_date_time,
         #insert.dv_load_end_date_time,
         #insert.dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
   where row_num >= @start
     and row_num < @start+1000000
commit tran
set @start = @start + 1000000
end
end
