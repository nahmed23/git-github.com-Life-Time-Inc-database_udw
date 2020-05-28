CREATE PROC [dbo].[proc_util_sequence_number_insert] @table_name [varchar](256) AS
begin

  set nocount on
  set xact_abort on

-- If the table_name already exists then do not insert a second one.
-- If the actual table already exists then get the maximum from there (or zero if no records or only base records)

declare @max_sequence_number bigint
declare @stmt varchar(max)

if object_id('tempdb..#max_sequence_number') is not null
  drop table #max_sequence_number

create table #max_sequence_number(max_id bigint) with (heap)

set @stmt = 'if exists (select 1 from sys.tables where name = ''' + @table_name + ''')' +
            '  insert #max_sequence_number(max_id) select max(' + @table_name + '_id) from dbo.' + @table_name + ' ' +
            'else' +
            '  insert #max_sequence_number(max_id) select null'
exec (@stmt)

set @max_sequence_number = (select case when max_id is null then 0
                                        when max_id < 0 then 0
                                        else max_id
                                    end
                              from #max_sequence_number)

--drop table #max_sequence_number

begin tran

if exists (select 1 from dbo.dv_sequence_number where table_name = @table_name)
  update dbo.dv_sequence_number
     set max_sequence_number = @max_sequence_number
   where table_name = @table_name
else
  insert dbo.dv_sequence_number (
           dv_sequence_number_id,
           table_name,
           max_sequence_number)
  select isnull(max(dv_sequence_number_id), 0) + 1,
         @table_name,
         @max_sequence_number
    from dbo.dv_sequence_number

commit tran

end
