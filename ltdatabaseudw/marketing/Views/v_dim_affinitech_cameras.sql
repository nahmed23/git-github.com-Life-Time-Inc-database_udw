CREATE VIEW [marketing].[v_dim_affinitech_cameras]
AS select d_affinitech_cameras.cam_id cam_id,
       d_affinitech_cameras.cam_club_it cam_club_it,
       d_affinitech_cameras.cam_dim_club_key cam_dim_club_key,
       d_affinitech_cameras.cam_inverted cam_inverted,
       d_affinitech_cameras.cam_ip cam_ip,
       d_affinitech_cameras.cam_name cam_name,
       d_affinitech_cameras.studio studio
  from dbo.d_affinitech_cameras;