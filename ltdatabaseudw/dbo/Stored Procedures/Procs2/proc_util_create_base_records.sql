CREATE PROC [dbo].[proc_util_create_base_records] @table_name [varchar](256) AS
BEGIN

  set xact_abort on
  set nocount on

declare @stmt nvarchar(4000), @start int, @end int, @checksql nvarchar(4000)

set @stmt = 'set identity_insert '+@table_name+' on'+char(13)+char(10)+
'insert into dbo.['+@table_name+'](dv_load_date_time'+
',dv_inserted_date_time'+
',dv_insert_user'+
case when SUBSTRING(@table_name, 1, 2) in ('r_','p_') then ',dv_load_end_date_time' else '' end+
',dv_batch_id'+
case when SUBSTRING(@table_name, 1, 2) in ('h_','l_','r_','s_') then ',dv_r_load_source_id' else '' end+
case when SUBSTRING(@table_name, 1, 2) in ('r_','s_','l_') then ',dv_hash' else '' end

if object_id('tempdb..#t') is not null drop table #t
create table dbo.#t with(distribution=round_robin, location=user_db, heap) as
select @table_name+'_id' dv_column, 1 r
union
select 'bk_hash' dv_column, 2 r
union
select dv_column, 2+dense_rank() over (order by sort_order) r
from dv_etl_map
where dv_table =  @table_name
  and (business_key_sort_order is not null
       or dv_column like 'r[_]%')

set @start = 1
set @end = (select max(r) from #t)


--insert sql
while @start <= @end
begin

set @stmt = (select distinct @stmt+','+dv_column from #t where r = @start)

set @start = @start + 1
end

set @stmt = @stmt+')'+char(13)

-- -999
set @start = 0
set @stmt = @stmt+'select ''jan 1, 1900'''+
           ','''+convert(varchar(30),getdate())+''''+
	   ','''+suser_sname()+''''+
	   case when SUBSTRING(@table_name, 1, 2) in ('r_','p_') then ',''dec 31, 9999''' else '' end+
	   ',-1'+
	   case when SUBSTRING(@table_name, 1, 2) in ('h_','l_','r_','s_') then ',6' else '' end+
	   case when SUBSTRING(@table_name, 1, 2) in ('r_','s_','l_') then ',-999' else '' end
	   
while @start <= @end
begin

if @start <> 0 
begin
set @stmt =  @stmt+case when @start > 2 then ',null' else ',-999' end
end

set @start = @start + 1
end

-- -998
set @start = 0
set @stmt = @stmt+char(13)+'union'+char(13)+'select ''jan 1, 1900'''+
           ','''+convert(varchar(30),getdate())+''''+
	   ','''+suser_sname()+''''+
	   case when SUBSTRING(@table_name, 1, 2) in ('r_','p_') then ',''dec 31, 9999''' else '' end+
	   ',-1'+
	   case when SUBSTRING(@table_name, 1, 2) in ('h_','l_','r_','s_') then ',6' else '' end+
	   case when SUBSTRING(@table_name, 1, 2) in ('r_','s_','l_') then ',-998' else '' end
	   	   
while @start <= @end
begin

if @start <> 0 
begin
set @stmt =  @stmt+case when @start > 2 then ',null' else ',-998' end
end

set @start = @start + 1
end

-- -997
set @start = 0
set @stmt = @stmt+char(13)+'union'+char(13)+'select ''jan 1, 1900'''+
           ','''+convert(varchar(30),getdate())+''''+
	   ','''+suser_sname()+''''+
	   case when SUBSTRING(@table_name, 1, 2) in ('r_','p_') then ',''dec 31, 9999''' else '' end+
	   ',-1'+
	   case when SUBSTRING(@table_name, 1, 2) in ('h_','l_','r_','s_') then ',6' else '' end+
	   case when SUBSTRING(@table_name, 1, 2) in ('r_','s_','l_') then ',-997' else '' end
	   	 
while @start <= @end
begin

if @start <> 0 
begin
set @stmt =  @stmt+case when @start > 2 then ',null' else ',-997' end
end

set @start = @start + 1
end

set @stmt = @stmt+char(13)+char(10)+'set identity_insert '+@table_name+' off'+char(13)+char(10)

if object_id('tempdb..#tablecount') is not null drop table #tablecount
create table dbo.#tablecount (c int) with(distribution=round_robin, location=user_db)

--only insert if the records don't already exist
set @checksql = 'insert into #tablecount select count(*) from '+@table_name+' where bk_hash in (''-999'',''-998'',''-997'')'	    
exec sp_executesql @checksql

if((select c from #tablecount) = 0) exec sp_executesql @stmt

--reseed identity only if the base records are the only records
truncate table #tablecount

set @stmt = 'dbcc checkident ('+@table_name+', reseed, 1)'
set @checksql = 'insert into #tablecount select count(*) from '+@table_name+' where bk_hash not in (''-999'',''-998'',''-997'')'
exec sp_executesql @checksql

if((select c from #tablecount) = 0) exec sp_executesql @stmt


--select @stmt

drop table #tablecount
drop table #t

end
