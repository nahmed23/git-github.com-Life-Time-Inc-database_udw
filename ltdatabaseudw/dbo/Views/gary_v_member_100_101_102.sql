CREATE VIEW [dbo].[gary_v_member_100_101_102] AS select * from gary_member_100
union all
select * from gary_member_101
union all
select * from gary_member_102;