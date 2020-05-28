CREATE VIEW [reporting].[v_exerp_boss_booking_resource] AS select distinct
'EXERP' data_source,
mb.dim_exerp_booking_recurrence_key as reservation_key, 
ebres.resource_id,
ebres.resource_name,
ebres.resource_type 
from marketing.v_dim_exerp_booking_resource_usage ebres
INNER JOIN marketing.v_dim_exerp_booking mb on mb.dim_exerp_booking_key = ebres.dim_exerp_booking_key;