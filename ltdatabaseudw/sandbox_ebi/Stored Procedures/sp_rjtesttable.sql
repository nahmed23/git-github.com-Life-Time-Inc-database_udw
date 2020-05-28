CREATE PROC [sandbox_ebi].[sp_rjtesttable] AS
begin
select 'sp' as OT, * from sandbox_ebi.rjtesttable
end
