CREATE PROC [dbo].[proc_util_d_bk_hash] @parse_string [varchar](8000) AS
begin

set nocount on
set xact_abort on

--declare @parse_string varchar(8000) = 'util_bk_hash[l_udwcloudsync_club_master_roster.spabiz_store_num,h_spabiz_store.store_number]'
--remove new lines (13,10) and tabs (9)
set @parse_string = ltrim(rtrim(REPLACE(REPLACE(REPLACE(replace(@parse_string,char(160),''), CHAR(13), ''), CHAR(10), ''),char(9), '')))
--remove "util_bk_hash[" and the matching "]"
set @parse_string = right(@parse_string,len(@parse_string)-13)
set @parse_string = left(@parse_string,len(@parse_string)-1)

if object_id('tempdb..#parsed') is not null drop table #parsed
create table dbo.#parsed
       (from_qualified varchar(500),
        to_data_type varchar(100),
        r int)
  with (heap)

if object_id('tempdb..#proc_util_d_bk_hash') is not null drop table #proc_util_d_bk_hash
create table dbo.#proc_util_d_bk_hash
       (d_bk_hash varchar(max))
  with (heap)

declare @start int = 2
declare @end int = (select len(@parse_string) - len(replace(@parse_string,',',''))+1)
declare @from varchar(500), @next_from varchar(500)
declare @to varchar(500), @next_to varchar(500)

--parse out parameter pairs of froms/tos
while @start <= @end
begin
    
    set @from = (select ltrim(rtrim(substring(@parse_string,1,charindex(',',@parse_string)-1))))
    set @parse_string = ltrim(rtrim(substring(@parse_string,charindex(',',@parse_string)+1,len(@parse_string))))
    if @start = @end set @to = @parse_string else set @to = ltrim(rtrim((select substring(@parse_string,1,charindex(',',@parse_string)-1))))
    set @parse_string = ltrim(rtrim(substring(@parse_string,charindex(',',@parse_string)+1,len(@parse_string))))

    insert into #parsed
    select @from,
           data_type,
           @start/2
     from dv_etl_map
    where dv_table = substring(@to,1,charindex('.',@to)-1)
      and dv_column = substring(@to,charindex('.',@to)+1,len(@to))
    union all
    select @from,
           data_type,
           @start/2
     from dv_d_etl_map
    where target_object = substring(@to,1,charindex('.',@to)-1)
      and target_column = substring(@to,charindex('.',@to)+1,len(@to))

    set @start = @start + 2

end

--select * from #parsed

set @start = 1
set @end = (select max(r) from #parsed)
declare @bk_sql varchar(max) = 'convert(char(32),hashbytes(''md5'',('

--assemble the bk_hash calculation string
while @start <= @end
begin
    
    set @bk_sql = isnull(@bk_sql,'')
                  +(select case when @start > 1 then '+' else '' end
                           +'''P%#&z$@k''+'
                           +case when to_data_type = 'bigint' then 'isnull(cast(cast('+from_qualified+' as '+to_data_type+') as varchar(500)),''z#@$k%&P'')'
                                 when to_data_type = 'bit' then 'isnull(cast(cast('+from_qualified+' as '+to_data_type+') as varchar(42)),''z#@$k%&P'')'
                                 when to_data_type like 'char%' then 'isnull(cast('+from_qualified+' as '+to_data_type+'),''z#@$k%&P'')'
                                 when to_data_type like 'datetime%' then 'isnull(convert(varchar,cast('+from_qualified+' as '+to_data_type+'),120),''z#@$k%&P'')'
                                 when to_data_type like 'decimal%' then 'isnull(cast(cast('+from_qualified+' as '+to_data_type+') as varchar(500)),''z#@$k%&P'')'
                                 when to_data_type like 'float' then 'isnull(cast(cast('+from_qualified+' as '+to_data_type+') as varchar(500)),''z#@$k%&P'')'
                                 when to_data_type = 'int' then 'isnull(cast(cast('+from_qualified+' as '+to_data_type+') as varchar(500)),''z#@$k%&P'')'
                                 when to_data_type like 'nchar%' then 'isnull(cast('+from_qualified+' as '+to_data_type+'),''z#@$k%&P'')'
                                 when to_data_type like 'numeric%' then 'isnull(cast(cast('+from_qualified+' as '+to_data_type+') as varchar(500)),''z#@$k%&P'')'
                                 when to_data_type like 'nvarchar%' then 'isnull(cast('+from_qualified+' as '+to_data_type+'),''z#@$k%&P'')'
                                 when to_data_type like 'smalldatetime%' then 'isnull(convert(varchar,cast('+from_qualified+' as '+to_data_type+'),120),''z#@$k%&P'')'
                                 when to_data_type = 'smallint' then 'isnull(cast(cast('+from_qualified+' as '+to_data_type+') as varchar(500)),''z#@$k%&P'')'
                                 when to_data_type like 'uniqueidentifier%' then 'isnull(cast(cast('+from_qualified+' as '+to_data_type+') as char(36)),''z#@$k%&P'')'
                                 when to_data_type like 'varchar%' then 'isnull(cast('+from_qualified+' as '+to_data_type+'),''z#@$k%&P'')'
                                 when to_data_type like '%binary%' then 'isnull(convert(varchar(500), cast('+from_qualified+' as '+to_data_type+'), 2),''z#@$k%&P'')'
                                 else 'isnull(cast(cast('+from_qualified+' as '+to_data_type+') as varchar(500)),''z#@$k%&P'')' end
                      from #parsed
                     where r = @start)

    set @start = @start + 1

end

set @bk_sql = @bk_sql+')),2)'

insert into #proc_util_d_bk_hash
select @bk_sql

--select @bk_sql


drop table #parsed
--drop table #proc_util_d_bk_hash

end
