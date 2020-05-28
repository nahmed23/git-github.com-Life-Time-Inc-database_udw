CREATE PROC [marketing].[proc_parse_pipe_list] @item [varchar](8000),@list_table [varchar](500) AS
begin
set xact_abort on
set nocount on

declare @table_Name varchar(500),
        @sql varchar(max),
		@NewLineChar as char(2) = char(13) + char(10)
set @table_name = '#'+@list_table

set @sql ='DECLARE @list varchar(8000)'+@NewLineChar+
          'SET @list ='+''''+@item+''''+@NewLineChar+
          'if object_id(''tempdb..'+@table_name+''') is not null
          drop table ' + @table_name + @NewLineChar+' create table dbo.'+@table_name + ' (item varchar(8000)  null ) with (location=user_db,heap)' +@NewLineChar+
          'SET @list = REPLACE(@list+ ''|'', ''||'', ''|'')' +@NewLineChar+
          'DECLARE @SP2 INT' + @NewLineChar+
          'DECLARE @VALUE2 VARCHAR(1000)' + @NewLineChar+
          'WHILE PATINDEX(''%|%'', @list ) <> 0 '+ @NewLineChar+
          'BEGIN' +@NewLineChar+
          'SELECT  @SP2 = PATINDEX(''%|%'',@list)' +@NewLineChar+ 
          'SELECT  @VALUE2 = LEFT(@list, @SP2 - 1)' +@NewLineChar+
          'SET  @list = STUFF(@list, 1, @SP2, '''')' +@NewLineChar+
          'INSERT '+ @table_name+'(item) VALUES (@VALUE2)' +@NewLineChar+
          'END' +@NewLineChar
           exec (@sql)

end
