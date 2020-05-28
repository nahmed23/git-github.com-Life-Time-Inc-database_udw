CREATE VIEW [marketing].[v_dim_chronotrack_event_location]
AS select d_chronotrack_event_location.event_location_id event_location_id,
       d_chronotrack_event_location.create_time create_time,
       d_chronotrack_event_location.d_chronotrack_event_bk_hash d_chronotrack_event_bk_hash,
       d_chronotrack_event_location.d_chronotrack_location_bk_hash d_chronotrack_location_bk_hash,
       d_chronotrack_event_location.event_id event_id,
       d_chronotrack_event_location.location_id location_id,
       d_chronotrack_event_location.modified_time modified_time
  from dbo.d_chronotrack_event_location;