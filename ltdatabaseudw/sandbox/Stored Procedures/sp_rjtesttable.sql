CREATE PROC [sandbox].[sp_rjtesttable] AS
begin
select 'sp' as OT, * from sandbox.rjtesttable
end
