CREATE PROC [dbo].[proc_dv_validation_error_daily] AS
begin

set xact_abort on
set nocount on
set ansi_nulls on
set quoted_identifier on

if object_id('tempdb..#o') is not null drop table #o
create table dbo.#o
        (table_name varchar(500),
         validation_error_message varchar(500),
         error_count bigint
         )
  with (heap)

declare @dv_batch_id varchar(14) = (select cast(dv_batch_id as varchar(14)) from dv_job_status where job_name= 'wf_dv_main_azure_master_begin')

if object_id('tempdb..#pits') is not null drop table #pits
create table dbo.#pits with(distribution=round_robin, location=user_db, heap) as
select dv_table,rank() over (order by dv_table) r
  from dv_etl_map
 where dv_table like 'p[_]%' /*pit tables*/
group by dv_table

/*
/*Get all the BK columns*/
if object_id('tempdb..#dv_bk') is not null drop table #dv_bk
create table dbo.#dv_bk with(distribution=round_robin, location=user_db, heap) as
select dv_table, dv_column, business_key_sort_order
from dv_etl_map
where business_key_sort_order is not null
and dv_table not like 'stage%'
and object_id(dv_table) is not null
and 0=1
group by dv_table, dv_column, business_key_sort_order

/*this table will hold the table name and it's related comma-separated-list of BKs*/
if object_id('tempdb..#table_bk_comma_list') is not null drop table #table_bk_comma_list
create table dbo.#table_bk_comma_list with(distribution=round_robin, location=user_db, heap) as
select dv_table, substring(dv_table,1,1) table_type, cast('' as varchar(max)) bk_comma_list, rank() over (order by dv_table) r
from #dv_bk
group by dv_table, substring(dv_table,1,1)

/*this loop calculated the comma-separated-list of BKs*/
declare @start int = 1
declare @end int = (select max(business_key_sort_order) from #dv_bk)

while @start<=@end
begin

    update #table_bk_comma_list
       set bk_comma_list = bk_comma_list+#dv_bk.dv_column+','
      from #dv_bk
     where #table_bk_comma_list.dv_table = #dv_bk.dv_table
       and #dv_bk.business_key_sort_order = @start

set @start = @start + 1

end

/*this is necessary because we don't know which column is the last business key, so we always append a comma...and remove the last one after above calculation*/
update #table_bk_comma_list 
   set bk_comma_list = substring(bk_comma_list,1,len(bk_comma_list)-1)
   */
declare @sql varchar(max)
declare @start int = 1
declare @end int = (select max(r) from #pits)


/*loop through DV objects*/
while @start <= @end
begin


    /*check for duplicates bk_hashes*/
    set @sql = (select distinct 'insert into #o (table_name,validation_error_message,error_count)
                                    select '''+dv_table+''' table_name,
                                        ''duplicate bk_hash, dv_load_date_time'' failed_validation_message ,
                                        count(*)
                                    from (select bk_hash, dv_load_date_time
                                            from '+dv_table+'
                                            where bk_hash in (select bk_hash from '+dv_table+' where dv_batch_id = '+@dv_batch_id+')
                                            group by bk_hash, dv_load_date_time
                                            having count(*) > 1) x
                                    having count(*) > 0'
                    from #pits
                    where r = @start)
    exec(@sql)


/*
    /*hubs*/
    if exists(select 1 from #table_bk_comma_list where r = @start and table_type = 'h')
        begin

            /*check for duplicates bk_hashes*/
            set @sql = (select distinct 'insert into #o (table_name,validation_error_message,error_count)
                                         select '''+dv_table+''' table_name,
                                                ''duplicate bk_hash'' failed_validation_message,
                                                count(*)
                                           from (select bk_hash
                                                   from '+dv_table+'
                                                  where bk_hash not in (''-997'',''-998'',''-999'')
                                                    and bk_hash in (select bk_hash from '+dv_table+' where dv_batch_id = '+@dv_batch_id+')
                                                  group by bk_hash
                                                 having count(*) > 1) x
                                         having count(*) > 0'
                          from #table_bk_comma_list
                         where r = @start)
            exec(@sql)
    
            /*check for duplicate bks*/
            set @sql = (select distinct 'insert into #o (table_name,validation_error_message,error_count) 
                                         select '''+dv_table+''' table_name,
                                                ''duplicate bk'' failed_validation_message,
                                                count(*)
                                           from (select '+bk_comma_list+'
                                                   from '+dv_table+'
                                                  where bk_hash not in (''-997'',''-998'',''-999'')
                                                    and bk_hash in (select bk_hash from '+dv_table+' where dv_batch_id = '+@dv_batch_id+')
                                                  group by '+bk_comma_list+'
                                                 having count(*) > 1) x
                                         having count(*) > 0'
                          from #table_bk_comma_list
                         where r = @start)
            exec(@sql)

        end

    /*links/satellites*/
    else if exists(select 1 from #table_bk_comma_list where r = @start and table_type in ('s','l'))
        begin

            /*check for duplicates bk_hashes*/
            set @sql = (select distinct 'insert into #o (table_name,validation_error_message,error_count)
                                         select '''+dv_table+''' table_name,
                                                ''duplicate bk_hash, dv_load_date_time'' failed_validation_message ,
                                                count(*)
                                           from (select bk_hash, dv_load_date_time
                                                   from '+dv_table+'
                                                  where bk_hash not in (''-997'',''-998'',''-999'')
                                                    and bk_hash in (select bk_hash from '+dv_table+' where dv_batch_id = '+@dv_batch_id+')
                                                  group by bk_hash, dv_load_date_time
                                                 having count(*) > 1) x
                                         having count(*) > 0'
                          from #table_bk_comma_list
                         where r = @start)
            exec(@sql)

            /*check for duplicate bk/dv_load_date_time combos*/
            set @sql = (select distinct 'insert into #o (table_name,validation_error_message,error_count)
                                         select '''+dv_table+''' table_name,
                                                ''duplicate bk, dv_load_date_time'' failed_validation_message,
                                                count(*)
                                           from (select '+bk_comma_list+',dv_load_date_time
                                                   from '+dv_table+'
                                                  where bk_hash not in (''-997'',''-998'',''-999'')
                                                    and bk_hash in (select bk_hash from '+dv_table+' where dv_batch_id = '+@dv_batch_id+')
                                                  group by '+bk_comma_list+',dv_load_date_time
                                                 having count(*) > 1) x
                                         having count(*) > 0'
                          from #table_bk_comma_list
                         where r = @start)
            exec(@sql)

        end

    /*pits*/
    else if exists(select 1 from #table_bk_comma_list where r = @start and table_type in ('p'))
        begin

            /*check the last record in a key series has a NULL dv_next_greatest_satellite_date_time*/
            set @sql = (select distinct 'insert into #o (table_name,validation_error_message,error_count)
                                         select '''+dv_table+''' table_name,
                                                ''Active PIT record has non-null next_greatest_saellite_date_time'' failed_validation_message ,
                                                count(*) c
                                           from '+dv_table+'
                                          where bk_hash not in (''-997'',''-998'',''-999'')
                                            and bk_hash in (select bk_hash from '+dv_table+' where dv_batch_id = '+@dv_batch_id+')
                                            and dv_load_end_date_time = ''Dec 31, 9999''
                                            and (dv_next_greatest_satellite_date_time is not null
                                                 or dv_next_greatest_satellite_date_time <> ''dec 31, 9999'')
                                         having count(*) > 0'
                          from #table_bk_comma_list
                         where r = @start)
            exec(@sql)
        end
*/

    set @start = @start + 1

end


/*check for valid dependencies*/
insert into #o (table_name,validation_error_message,error_count) 
select 'dv_job_dependency' table_name,
       'Job dependency missing or invalid' failed_validation_message,
       count(*)
  from dv_job_dependency
  left join dv_job_status on dv_job_dependency.dependent_on_dv_job_status_id = dv_job_status.dv_job_status_id
 where dv_job_status.dv_job_status_id is null
having count(*) > 0


/*Start- Added as part of sprint UDW - 7423 */
insert into #o (table_name,validation_error_message,error_count) 
/*all tables exist*/
select dv_table as table_name, 'Table(s) does not exist in the database' failed_validation_message , count(*)
from dv_etl_map 
left join sys.tables t on dv_etl_map.dv_table = t.name
where t.object_id is null
group by dv_table, t.object_id, t.name
having count(*) > 0

insert into #o (table_name,validation_error_message,error_count) 
/*etl procs*/
select right(dv_table,len(dv_table)-2) table_name, 'etl proc(s) does not exist in the database' failed_validation_message , count(*)
from dv_etl_map 
left join sys.procedures t on t.name like 'proc_etl_%'+right(dv_table,len(dv_table)-2)+'%' 
where t.object_id is null /*DNE*/
  and dv_table like 'h[_]%'
group by right(dv_table,len(dv_table)-2), t.object_id,t.name
having count(*) > 0

insert into #o (table_name,validation_error_message,error_count) 
/*pit procs*/
select right(dv_table,len(dv_table)-2) table_name, 'pit proc(s) does not exist in the database' failed_validation_message , count(*)
from dv_etl_map 
left join sys.procedures t on t.name like 'proc_p[_]%'+right(dv_table,len(dv_table)-2)+'%' 
where t.object_id is null /*DNE*/
  and dv_table like 'h[_]%'
group by right(dv_table,len(dv_table)-2), t.object_id,t.name
having count(*) > 0

insert into #o (table_name,validation_error_message,error_count) 
/*d tables*/
select target_object as table_name, 'd table(s) does not exist in the database' failed_validation_message , count(*)
from dv_d_etl_map
left join sys.tables t on t.name = dv_d_etl_map.target_object
where t.object_id is null /*DNE*/
  and target_object like 'd[_]%'
group by target_object, t.object_id, t.name
having count(*) > 0

insert into #o (table_name,validation_error_message,error_count) 
/*d procs*/
select target_object as table_name, 'd proc(s) does not exist in the database' failed_validation_message , count(*)
from dv_d_etl_map
left join sys.procedures t on t.name = 'proc_'+dv_d_etl_map.target_object
where t.object_id is null /*DNE*/
  and target_object like 'd[_]%'
group by target_object, t.object_id, t.name
having count(*) > 0

/*End- Added as part of sprint UDW - 7423*/

select distinct 
       table_name,
       validation_error_message,
       error_count,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user,
       @dv_batch_id dv_batch_id
  from #o


 end

