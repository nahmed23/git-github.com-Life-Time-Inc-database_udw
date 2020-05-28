CREATE PROC [sandbox].[sp_mytesttable] AS
begin
select 'sp' as OT, * from sandbox.mytesttable
end
