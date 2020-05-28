CREATE VIEW [marketing].[v_dim_chronotrack_location]
AS select d_chronotrack_location.location_id location_id,
       d_chronotrack_location.city city,
       d_chronotrack_location.county county,
       d_chronotrack_location.create_time create_time,
       d_chronotrack_location.latitude latitude,
       d_chronotrack_location.longitude longitude,
       d_chronotrack_location.modified_time modified_time,
       d_chronotrack_location.name name,
       d_chronotrack_location.postal_code postal_code,
       d_chronotrack_location.region_id region_id,
       d_chronotrack_location.street street,
       d_chronotrack_location.street_2 street_2,
       d_chronotrack_location.time_zone time_zone
  from dbo.d_chronotrack_location;