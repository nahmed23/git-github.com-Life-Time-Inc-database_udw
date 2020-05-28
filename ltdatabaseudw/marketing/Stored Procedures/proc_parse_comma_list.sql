CREATE PROC [marketing].[proc_parse_comma_list] @Item [varchar](500),@ListTable [varchar](500) AS
begin
set xact_abort on
set nocount on

declare @tableName varchar(500),
        @sql varchar(max),
		@NewLineChar as char(2) = char(13) + char(10)
set @tablename = '#'+@ListTable

set @sql ='DECLARE @list varchar(500)'+@NewLineChar+
          'SET @list ='+''''+@item+''''+@NewLineChar+
          'if object_id(''tempdb..'+@tablename+''') is not null
          drop table ' + @tablename + @NewLineChar+' create table dbo.'+@tablename + ' (id int  null ) with (location=user_db,heap)' +@NewLineChar+
          'SET @list = REPLACE(@list+ '','', '',,'', '','')' +@NewLineChar+
          'DECLARE @SP2 INT' + @NewLineChar+
          'DECLARE @VALUE2 VARCHAR(1000)' + @NewLineChar+
          'WHILE PATINDEX(''%,%'', @list ) <> 0 '+ @NewLineChar+
          'BEGIN' +@NewLineChar+
          'SELECT  @SP2 = PATINDEX(''%,%'',@list)' +@NewLineChar+ 
          'SELECT  @VALUE2 = LEFT(@list, @SP2 - 1)' +@NewLineChar+
          'SET  @list = STUFF(@list, 1, @SP2, '''')' +@NewLineChar+
          'INSERT '+ @tablename+'(id) VALUES (@VALUE2)' +@NewLineChar+
          'END' +@NewLineChar
           exec (@sql)
end

            ---------------------------Procedure ends-------------------------------



