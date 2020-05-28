CREATE PROC [dbo].[proc_fact_boss_participation] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_boss_participation)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

 /**/
 /* Find the distinct set of dim_boss_reservation_keys in d_boss_asi_res_inst, d_boss_asi_reserv, d_boss_participation that are in the current batch(es)*/
 /**/
if object_id('tempdb.dbo.#d_boss_participation_dim_boss_reservation_key') is not null drop table #d_boss_participation_dim_boss_reservation_key
create table dbo.#d_boss_participation_dim_boss_reservation_key with(distribution = hash(dim_boss_reservation_key), location=user_db) as
select d_boss_participation.dim_boss_reservation_key
  from d_boss_participation
 where d_boss_participation.dv_batch_id >= @load_dv_batch_id

if object_id('tempdb.dbo.#d_boss_asi_reserv_dim_boss_reservation_key') is not null drop table #d_boss_asi_reserv_dim_boss_reservation_key
create table dbo.#d_boss_asi_reserv_dim_boss_reservation_key with(distribution = hash(dim_boss_reservation_key), location=user_db) as
select d_boss_asi_reserv.dim_boss_reservation_key
  from d_boss_asi_reserv
 where d_boss_asi_reserv.dv_batch_id >= @load_dv_batch_id

if object_id('tempdb.dbo.#d_boss_asi_res_inst_dim_boss_reservation_key') is not null drop table #d_boss_asi_res_inst_dim_boss_reservation_key
create table dbo.#d_boss_asi_res_inst_dim_boss_reservation_key with(distribution = hash(dim_boss_reservation_key), location=user_db) as
select d_boss_asi_res_inst.dim_boss_reservation_key
  from d_boss_asi_res_inst
 where d_boss_asi_res_inst.dv_batch_id >= @load_dv_batch_id

if object_id('tempdb.dbo.#dim_boss_reservation_key') is not null drop table #dim_boss_reservation_key
create table dbo.#dim_boss_reservation_key with(distribution = hash(dim_boss_reservation_key), location=user_db) as
select dim_boss_reservation_key
  from #d_boss_participation_dim_boss_reservation_key
union
select dim_boss_reservation_key
  from #d_boss_asi_reserv_dim_boss_reservation_key
union
select dim_boss_reservation_key 
  from #d_boss_asi_res_inst_dim_boss_reservation_key

 /* Get the fact_boss_participation related data from d_boss_asi_res_inst, d_boss_asi_reserv, d_boss_participation for the set of dim_boss_reservation_keys*/
 /**/
if object_id('tempdb.dbo.#d_boss_participation') is not null drop table #d_boss_participation
create table dbo.#d_boss_participation with(distribution = hash(dim_boss_reservation_key), location=user_db) as
select a.* from
  ( select d_boss_participation.bk_hash fact_boss_participation_key,
           d_boss_participation.participation_id,
           d_boss_participation.dim_boss_reservation_key,
           d_boss_participation.mod_count,
           d_boss_participation.number_of_participants,
           d_boss_participation.participation_dim_date_key,
           d_boss_participation.dv_load_date_time,
           d_boss_participation.dv_batch_id,
           rank() over (partition by d_boss_participation.dim_boss_reservation_key,d_boss_participation.participation_dim_date_key order by dv_load_date_time desc ) rec_order
    from d_boss_participation
    join #dim_boss_reservation_key
    on d_boss_participation.dim_boss_reservation_key = #dim_boss_reservation_key.dim_boss_reservation_key
          ) a
 where rec_order = 1
 
if object_id('tempdb.dbo.#d_boss_asi_reserv') is not null drop table #d_boss_asi_reserv
create table dbo.#d_boss_asi_reserv with(distribution = hash(dim_boss_reservation_key), location=user_db) as
select d_boss_asi_reserv.dim_boss_reservation_key,
       d_boss_asi_reserv.dim_employee_key,
       d_boss_asi_reserv.dv_load_date_time,
       d_boss_asi_reserv.dv_batch_id
  from d_boss_asi_reserv
  join #dim_boss_reservation_key
    on d_boss_asi_reserv.dim_boss_reservation_key = #dim_boss_reservation_key.dim_boss_reservation_key

if object_id('tempdb.dbo.#d_boss_asi_res_inst') is not null drop table #d_boss_asi_res_inst
create table dbo.#d_boss_asi_res_inst with(distribution = hash(dim_boss_reservation_key), location=user_db) as
select d_boss_asi_res_inst.asi_res_inst_id,
       d_boss_asi_res_inst.dim_boss_reservation_key,
       d_boss_asi_res_inst.dim_employee_key,
       d_boss_asi_res_inst.instructor_start_dim_date_key,
       d_boss_asi_res_inst.instructor_end_dim_date_key,
       d_boss_asi_res_inst.instructor_type,
       d_boss_asi_res_inst.dv_load_date_time,
       d_boss_asi_res_inst.dv_batch_id
  from d_boss_asi_res_inst
  join #dim_boss_reservation_key
    on d_boss_asi_res_inst.dim_boss_reservation_key = #dim_boss_reservation_key.dim_boss_reservation_key

 /* There may be more than one instructor associated with a participation record.  */
 /* In that case use substitue (S) over primary (P).  */
 /* Note that Assistants (A) are only for the secondary instructor.  */
 /* Since S comes alphabetically before P and P before A we can just use a descending sort to determine the rank of the primary instructor.*/
 /* In case there are multiples order by latest start date then earliest end date then latest reservation instructor record id.*/
 /* Since A comes alphabetically before P and S we can use an ascending sort to determine the rank of the associate instructor.*/
 /* Note that there may not be an asi_res_inst record so use a left join.*/
if object_id('tempdb.dbo.#combined') is not null drop table #combined
create table dbo.#combined with(distribution = hash(dim_boss_reservation_key), location=user_db) as
select #d_boss_participation.fact_boss_participation_key,
       #d_boss_participation.participation_id,
       #d_boss_participation.dim_boss_reservation_key,
       #d_boss_participation.mod_count,
       #d_boss_participation.number_of_participants,
       #d_boss_participation.participation_dim_date_key,
       #d_boss_asi_reserv.dim_employee_key reserv_dim_employee_key,
       #d_boss_asi_res_inst.dim_employee_key res_inst_dim_employee_key,
       #d_boss_asi_res_inst.instructor_type,
       case when #d_boss_participation.dv_load_date_time >= isnull(#d_boss_asi_reserv.dv_load_date_time,'Jan 1, 1753')
             and #d_boss_participation.dv_load_date_time >= isnull(#d_boss_asi_res_inst.dv_load_date_time,'Jan 1, 1753')
            then #d_boss_participation.dv_load_date_time
            when #d_boss_asi_reserv.dv_load_date_time >= isnull(#d_boss_asi_res_inst.dv_load_date_time,'Jan 1, 1753')
            then #d_boss_asi_reserv.dv_load_date_time
            else isnull(#d_boss_asi_res_inst.dv_load_date_time,'Jan 1, 1753')
             end dv_load_date_time,
       case when #d_boss_participation.dv_batch_id >= isnull(#d_boss_asi_reserv.dv_batch_id,-1)
             and #d_boss_participation.dv_batch_id >= isnull(#d_boss_asi_res_inst.dv_batch_id,-1)
            then #d_boss_participation.dv_batch_id
            when #d_boss_asi_reserv.dv_batch_id >= isnull(#d_boss_asi_res_inst.dv_batch_id,-1)
            then #d_boss_asi_reserv.dv_batch_id
            else isnull(#d_boss_asi_res_inst.dv_batch_id,-1)
        end dv_batch_id,
       row_number() over(partition by #d_boss_participation.fact_boss_participation_key, #d_boss_participation.dim_boss_reservation_key order by #d_boss_asi_res_inst.instructor_type desc, #d_boss_asi_res_inst.instructor_start_dim_date_key desc, #d_boss_asi_res_inst.instructor_end_dim_date_key, #d_boss_asi_res_inst.asi_res_inst_id desc) primary_rank,
       row_number() over(partition by #d_boss_participation.fact_boss_participation_key, #d_boss_participation.dim_boss_reservation_key order by #d_boss_asi_res_inst.instructor_type, #d_boss_asi_res_inst.instructor_start_dim_date_key desc, #d_boss_asi_res_inst.instructor_end_dim_date_key, #d_boss_asi_res_inst.asi_res_inst_id desc) secondary_rank
  into #combined
  from #d_boss_participation
  join #d_boss_asi_reserv
    on #d_boss_participation.dim_boss_reservation_key = #d_boss_asi_reserv.dim_boss_reservation_key
  left join #d_boss_asi_res_inst
    on #d_boss_participation.dim_boss_reservation_key = #d_boss_asi_res_inst.dim_boss_reservation_key
   and #d_boss_participation.participation_dim_date_key >= #d_boss_asi_res_inst.instructor_start_dim_date_key
   and #d_boss_participation.participation_dim_date_key <= #d_boss_asi_res_inst.instructor_end_dim_date_key

 /* Combine the primary data with the optional secondary data*/
 /* Note that a full outer join is required since there are a few cases where there is an A record, but not a P or S.*/
 /* Therefore all returned columns require a case statement to determine if there is a primary record*/

if object_id('tempdb..#fact_boss_participation') is not null drop table #fact_boss_participation
create table dbo.#fact_boss_participation with(distribution = hash(dim_boss_reservation_key), location=user_db) as
with 
  primary_combined (fact_boss_participation_key, participation_id, dim_boss_reservation_key, mod_count, number_of_participants, participation_dim_date_key,
                    reserv_dim_employee_key, res_inst_dim_employee_key, instructor_type, dv_load_date_time, dv_batch_id) as
      (select #combined.fact_boss_participation_key,
              #combined.participation_id,
              #combined.dim_boss_reservation_key,
              #combined.mod_count,
              #combined.number_of_participants,
              #combined.participation_dim_date_key,
              #combined.reserv_dim_employee_key,
              #combined.res_inst_dim_employee_key,
              #combined.instructor_type,
              #combined.dv_load_date_time,
              #combined.dv_batch_id
         from #combined
        where #combined.primary_rank = 1
          and (#combined.instructor_type in ('S','P')
               or #combined.instructor_type is null)),
  secondary_combined (fact_boss_participation_key, participation_id, dim_boss_reservation_key, mod_count, number_of_participants, participation_dim_date_key,
                    reserv_dim_employee_key, res_inst_dim_employee_key, instructor_type, dv_load_date_time, dv_batch_id) as
      (select #combined.fact_boss_participation_key,
              #combined.participation_id,
              #combined.dim_boss_reservation_key,
              #combined.mod_count,
              #combined.number_of_participants,
              #combined.participation_dim_date_key,
              #combined.reserv_dim_employee_key,
              #combined.res_inst_dim_employee_key,
              #combined.instructor_type,
              #combined.dv_load_date_time,
              #combined.dv_batch_id
         from #combined
        where #combined.secondary_rank = 1
          and #combined.instructor_type = 'A')
select isnull(primary_combined.fact_boss_participation_key, secondary_combined.fact_boss_participation_key) fact_boss_participation_key,
       case when primary_combined.fact_boss_participation_key is not null
            then primary_combined.participation_id
            else secondary_combined.participation_id
        end participation_id,
       case when primary_combined.fact_boss_participation_key is not null
            then primary_combined.dim_boss_reservation_key
            else secondary_combined.dim_boss_reservation_key
        end dim_boss_reservation_key,
       case when primary_combined.fact_boss_participation_key is not null
            then primary_combined.mod_count
            else secondary_combined.mod_count
        end mod_count,
       case when primary_combined.fact_boss_participation_key is not null
            then primary_combined.number_of_participants
            else secondary_combined.number_of_participants
        end number_of_participants,
       case when primary_combined.fact_boss_participation_key is not null
            then primary_combined.participation_dim_date_key
            else secondary_combined.participation_dim_date_key
        end participation_dim_date_key,
       case when primary_combined.fact_boss_participation_key is not null
            then isnull(primary_combined.res_inst_dim_employee_key, primary_combined.reserv_dim_employee_key)
            else secondary_combined.res_inst_dim_employee_key
        end primary_dim_employee_key,
       case when primary_combined.fact_boss_participation_key is not null
            then isnull(primary_combined.instructor_type, 'P')
            else secondary_combined.instructor_type
        end instructor_type,
       case when primary_combined.fact_boss_participation_key is not null
            then isnull(secondary_combined.res_inst_dim_employee_key, '-997')
            else '-997'
        end secondary_dim_employee_key,
       case when primary_combined.dv_load_date_time >= isnull(secondary_combined.dv_load_date_time,'Jan 1, 1753')
            then primary_combined.dv_load_date_time
            else isnull(secondary_combined.dv_load_date_time,'Jan 1, 1753')
        end dv_load_date_time,
       '99991231' dv_load_end_date_time,
       case when primary_combined.dv_batch_id >= isnull(secondary_combined.dv_batch_id,-1)
            then primary_combined.dv_batch_id
            else isnull(secondary_combined.dv_batch_id,-1)
        end dv_batch_id
  from primary_combined
  full join secondary_combined
    on primary_combined.fact_boss_participation_key = secondary_combined.fact_boss_participation_key
   and primary_combined.dim_boss_reservation_key = secondary_combined.dim_boss_reservation_key

/* Delete and re-insert as a single transaction*/
/*   Delete records from the table that exist*/
/*   Insert incremental records*/

begin tran

 /*--- delete dbo.fact_boss_participation where fact_boss_participation_key in (select fact_boss_participation_key from #fact_boss_participation)*/
  /*---UDW-9140 defect fix------*/
delete dbo.fact_boss_participation 
   where dim_boss_reservation_key in (select dim_boss_reservation_key from #fact_boss_participation)
   and participation_dim_date_key in (select participation_dim_date_key from #fact_boss_participation)
  
  insert into dbo.fact_boss_participation
        (fact_boss_participation_key,
         participation_id,
         dim_boss_reservation_key,
         mod_count,
         number_of_participants,
         participation_dim_date_key,
         primary_dim_employee_key,
         instructor_type,
         secondary_dim_employee_key,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user)
  select fact_boss_participation_key,
         participation_id,
         dim_boss_reservation_key,
         mod_count,
         number_of_participants,
         participation_dim_date_key,
         primary_dim_employee_key,
         instructor_type,
         secondary_dim_employee_key,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate() ,
         suser_sname()
    from #fact_boss_participation
 
commit tran

end
