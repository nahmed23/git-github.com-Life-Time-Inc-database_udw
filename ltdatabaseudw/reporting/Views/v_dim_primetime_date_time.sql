CREATE VIEW [reporting].[v_dim_primetime_date_time]
AS select distinct b.day_number_in_week,a.display_24_hour_group,
	case 
	/*Sunday*/
	when b.day_number_in_week = 1 and a.display_24_hour_group in (
	'08:00 - 09:00',
	'09:00 - 10:00',
	'10:00 - 11:00',
	'11:00 - 12:00',
	'12:00 - 13:00',
	'13:00 - 14:00',
	'14:00 - 15:00',
	'15:00 - 16:00',
	'16:00 - 17:00') then 'Y'
	/*Monday, Tuesday, Wednesday,Thursday*/
	when b.day_number_in_week in (2,3,4,5) and a.display_24_hour_group in (
	'08:00 - 09:00',
	'09:00 - 10:00',
	'16:00 - 17:00',
	'17:00 - 18:00',
    '18:00 - 19:00'
	) then 'Y'
	--/*Friday*/
	when b.day_number_in_week = 6 and a.display_24_hour_group in (
	'08:00 - 09:00',
	'09:00 - 10:00',
	'10:00 - 11:00',
	'11:00 - 12:00',
	'16:00 - 17:00',
	'17:00 - 18:00'
   ) then 'Y'
 --  /*Saturday*/
	when b.day_number_in_week = 7 and a.display_24_hour_group in (
	'08:00 - 09:00',
	'09:00 - 10:00',
	'10:00 - 11:00',
	'11:00 - 12:00',
	'12:00 - 13:00',
	'13:00 - 14:00'
   ) then 'Y'
	else 'N' end is_primetime

	from marketing.v_dim_time a
	CROSS JOIN (select distinct day_number_in_week from marketing.v_dim_date where day_number_in_week >0) b
	where a.display_24_hour_group <>'N/A';