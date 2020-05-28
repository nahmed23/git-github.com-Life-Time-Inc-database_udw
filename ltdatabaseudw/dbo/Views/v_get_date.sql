CREATE VIEW [dbo].[v_get_date] AS select dateadd(hh,-1 * offset ,getdate()) get_datetime,
       convert(datetime,convert(varchar,dateadd(hh,-1 * offset ,getdate()),110),110) get_date
  from map_utc_time_zone_conversion
 where getdate() between utc_start_date_time and utc_end_date_time
   and description = 'central time';