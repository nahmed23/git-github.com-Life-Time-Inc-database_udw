CREATE PROC [dbo].[proc_Update_DeleteData_BOSS] @dv_table_name [varchar](50) AS
begin

set nocount on

declare @flag char(2)
declare @sql varchar (4000)
set @sql = 'declare @flag char(2)'+ char(13)+char(10)+
'set @flag = '+ 'case when exists( select 1 from dv_etl_map where dv_table = '+''''+''+@dv_table_name+''+''''+' and dv_column =  ''reservation'''+')'+
char(13)+char(10)+  'then ''H''  '+
'when exists (select 1 from dv_etl_map where dv_table = '+''''+''+replace(@dv_table_name,'h_','l_')+''+''''+' and dv_column = ''reservation''' +' and business_key_sort_order IS NULL)'+
+char(13)+char(10)+
'then ''L''  '+
'end'+
char(13)+char(10)+
'if @flag = ''H'''
+char(13)+char(10)+
'begin
 update '+''+@dv_table_name+''+
char(13)+char(10)+'set dv_deleted = 1
,dv_updated_date_time = convert(datetime,getdate(),120)
,dv_update_user = ''InformaticaUser''
where reservation in (select reservation from l_boss_audit_reserve)
and dv_deleted = 0'+
char(13)+char(10)+'end'+
char(13)+char(10)+
'else if  @flag = ''L'''+
char(13)+char(10)+
'begin
update '+''+@dv_table_name+''+
char(13)+char(10)+'set dv_deleted = 1
,dv_updated_date_time = convert(datetime,getdate(),120)
,dv_update_user = ''InformaticaUser''
where bk_hash in ( select link.bk_hash from '+''+replace(@dv_table_name,'h_','l_')+''+' link'+ 
+ char(13)+char(10)+
'join l_boss_audit_reserve auditDelete
on auditDelete.reservation = link.reservation)' +' and dv_deleted = 0'
+ char(13)+char(10)+
'end'
--print @sql
exec (@sql)



end
--GO

--exec proc_Update_DeleteData_BOSS 'h_boss_asi_player'












