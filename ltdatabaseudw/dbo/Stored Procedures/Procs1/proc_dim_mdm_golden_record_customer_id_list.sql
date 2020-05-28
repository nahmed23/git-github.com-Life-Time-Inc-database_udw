CREATE PROC [dbo].[proc_dim_mdm_golden_record_customer_id_list] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @dv_batch_id as current_dv_batch_id
  from dbo.dim_mdm_golden_record_customer_id_list

if object_id('tempdb..#p_mdm_golden_record_customer_insert') is not null drop table #p_mdm_golden_record_customer_insert
create table dbo.#p_mdm_golden_record_customer_insert with(distribution=hash(entity_id), location=user_db) as
select p_mdm_golden_record_customer.p_mdm_golden_record_customer_id,
       p_mdm_golden_record_customer.entity_id,
       p_mdm_golden_record_customer.bk_hash
  from dbo.p_mdm_golden_record_customer
  join #dv_batch_id
    on p_mdm_golden_record_customer.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or p_mdm_golden_record_customer.dv_batch_id = #dv_batch_id.current_dv_batch_id
 where p_mdm_golden_record_customer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)


if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with (distribution = hash(entity_id), location = user_db) as

with
goldenrecordcustomer_id_list (entity_id, member_id, membership_id, contact_id, lead_id, party_id, dv_load_date_time, dv_load_end_date_time, dv_batch_id,dv_inserted_date_time) as

   (select p_mdm_golden_record_customer.entity_id,
           l_mdm_golden_record_customer.member_id,
           l_mdm_golden_record_customer.membership_id,
		   l_mdm_golden_record_customer.contact_id,
           l_mdm_golden_record_customer.lead_id,
		   l_mdm_golden_record_customer.party_id,
           p_mdm_golden_record_customer.dv_load_date_time,
           p_mdm_golden_record_customer.dv_load_end_date_time,
           p_mdm_golden_record_customer.dv_batch_id,
		   p_mdm_golden_record_customer.dv_inserted_date_time
      from p_mdm_golden_record_customer
      join #p_mdm_golden_record_customer_insert
        on p_mdm_golden_record_customer.p_mdm_golden_record_customer_id = #p_mdm_golden_record_customer_insert.p_mdm_golden_record_customer_id
	   and p_mdm_golden_record_customer.entity_id = #p_mdm_golden_record_customer_insert.entity_id
      join l_mdm_golden_record_customer
        on p_mdm_golden_record_customer.l_mdm_golden_record_customer_id = l_mdm_golden_record_customer.l_mdm_golden_record_customer_id
	   and p_mdm_golden_record_customer.entity_id = l_mdm_golden_record_customer.entity_id
     where p_mdm_golden_record_customer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
       and (l_mdm_golden_record_customer.member_id is not null
	        or l_mdm_golden_record_customer.membership_id is not null
			or l_mdm_golden_record_customer.contact_id is not null
			or l_mdm_golden_record_customer.lead_id is not null
			or l_mdm_golden_record_customer.party_id is not null)
   )

select
        entity_id,
       '1' as id_type,
	   'mdm_golden_record_customer.id_type_1' as dim_description_key,
	   member_id as id,
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id,
	   dv_inserted_date_time
  from goldenrecordcustomer_id_list
where member_id is not null
union all
select
       entity_id,
       '2' as id_type,
	   'mdm_golden_record_customer.id_type_2' as dim_description_key,
	   membership_id as id,
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id,
	   dv_inserted_date_time
  from goldenrecordcustomer_id_list
where membership_id is not null
union all
select
       entity_id,
       '3' as id_type,
	   'mdm_golden_record_customer.id_type_3' as dim_description_key,
	   lead_id as id,
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id,
	   dv_inserted_date_time
  from goldenrecordcustomer_id_list
where lead_id is not null
union all
select
       entity_id,
       '4' as id_type,
	   'mdm_golden_record_customer.id_type_4' as dim_description_key,
	   contact_id as id,
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id,
	   dv_inserted_date_time
  from goldenrecordcustomer_id_list
where contact_id is not null
union all
select
       entity_id,
       '5' as id_type,
	   'mdm_golden_record_customer.id_type_5' as dim_description_key,
	   party_id as id,
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id,
	   dv_inserted_date_time
  from goldenrecordcustomer_id_list
where party_id is not null

if object_id('tempdb..#insertfinal') is not null drop table #insertfinal
create table dbo.#insertfinal with (location = user_db, distribution = hash(entity_id)) as
select entity_id,
       id_type,
	   dim_description_key,
	   id,
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id,
	   DATEADD(HOUR,-(map_utc_time_zone_conversion.offset),dv_inserted_date_time) udw_load_date_time,
	   row_number() over (order by entity_id) row_num
  from #insert
   join map_utc_time_zone_conversion
    on #insert.dv_inserted_date_time >= map_utc_time_zone_conversion.utc_start_date_time
   and #insert.dv_inserted_date_time < map_utc_time_zone_conversion.utc_end_date_time
   and map_utc_time_zone_conversion.val_time_zone_id = 3


/*figure out which entity_id records we need to delete from dimension*/
if object_id('tempdb..#delete') is not null drop table #delete
create table dbo.#delete with (location = user_db, distribution = hash(entity_id)) as
select entity_id,
       sum(dv_deleted ^ 1) deletedsum
  from h_mdm_golden_record_customer
group by entity_id
having sum(dv_deleted ^ 1) = 0

/*delete the entity_id that no longer valid from dimension*/
delete dbo.dim_mdm_golden_record_customer_id_list
   where entity_id in (select entity_id from #delete)

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insertfinal)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insertfinal where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'dim_mdm_golden_record_customer_id_list', @id_count = @insert_count, @start_id = @start_p_id out

/* Delete and re-insert*/
/* Do as a single transaction*/
/*   Delete records from the dim table that exist*/
/*   Insert records from current and missing batches*/

begin tran
  delete dbo.dim_mdm_golden_record_customer_id_list
   where entity_id in (select entity_id from #insertfinal where row_num >= @start and row_num < @start+1000000)
/*
declare @sys_date datetime=getdate()

declare @offset_val decimal=(select offset from  map_utc_time_zone_conversion map_utc_time_zone_conversion
where utc_start_date_time <= @sys_date  and utc_end_date_time > @sys_date and map_utc_time_zone_conversion.val_time_zone_id=3) */



  insert dbo.dim_mdm_golden_record_customer_id_list(
               dim_mdm_golden_record_customer_id_list_key,
			   entity_id,
               id_type,
			   dim_description_key,
			   id,
			   mdm_load_date_time,
			   udw_load_date_time,
               dv_load_date_time,
               dv_load_end_date_time,
               dv_batch_id,
               dv_inserted_date_time,
               dv_insert_user)
  select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#insertfinal.entity_id,'z#@$k%&P'))),2),
         #insertfinal.entity_id,
         #insertfinal.id_type,
         #insertfinal.dim_description_key,
		 #insertfinal.id,
		 #insertfinal.dv_load_date_time,
		 #insertfinal.udw_load_date_time,
         #insertfinal.dv_load_date_time,
         #insertfinal.dv_load_end_date_time,
         #insertfinal.dv_batch_id,
         getdate(),
         suser_sname()
    from #insertfinal
   where row_num >= @start
     and row_num < @start+1000000
commit tran
set @start = @start + 1000000
end
end
