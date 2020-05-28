CREATE PROC [dbo].[proc_util_sequence_number_get_next] @table_name [varchar](256),@id_count [bigint],@start_id [bigint] OUT AS
begin

  set nocount on
  set xact_abort on

begin tran

update dbo.dv_sequence_number
   set max_sequence_number = isnull(max_sequence_number, 0) + @id_count
 where table_name = @table_name

set @start_id = (select max_sequence_number - @id_count + 1 from dbo.dv_sequence_number where table_name = @table_name)

commit tran

end
