﻿CREATE VIEW [marketing].[v_dim_athlinks_api_vw_athlete_non_member]
AS select d_athlinks_api_vw_athlete_non_member.racer_id racer_id,
       d_athlinks_api_vw_athlete_non_member.age age,
       d_athlinks_api_vw_athlete_non_member.city city,
       d_athlinks_api_vw_athlete_non_member.country_id country_id,
       d_athlinks_api_vw_athlete_non_member.country_id_3 country_id_3,
       d_athlinks_api_vw_athlete_non_member.country_name country_name,
       d_athlinks_api_vw_athlete_non_member.create_date create_date,
       d_athlinks_api_vw_athlete_non_member.create_dim_date_key create_dim_date_key,
       d_athlinks_api_vw_athlete_non_member.create_dim_time_key create_dim_time_key,
       d_athlinks_api_vw_athlete_non_member.display_name display_name,
       d_athlinks_api_vw_athlete_non_member.full_name full_name,
       d_athlinks_api_vw_athlete_non_member.gender gender,
       d_athlinks_api_vw_athlete_non_member.is_member is_member,
       d_athlinks_api_vw_athlete_non_member.join_date join_date,
       d_athlinks_api_vw_athlete_non_member.join_dim_date_key join_dim_date_key,
       d_athlinks_api_vw_athlete_non_member.join_dim_time_key join_dim_time_key,
       d_athlinks_api_vw_athlete_non_member.last_name last_name,
       d_athlinks_api_vw_athlete_non_member.notes notes,
       d_athlinks_api_vw_athlete_non_member.owner_id owner_id,
       d_athlinks_api_vw_athlete_non_member.photo_path photo_path,
       d_athlinks_api_vw_athlete_non_member.result_count result_count,
       d_athlinks_api_vw_athlete_non_member.state_prov_abbrev state_prov_abbrev,
       d_athlinks_api_vw_athlete_non_member.state_prov_id state_prov_id,
       d_athlinks_api_vw_athlete_non_member.state_prov_name state_prov_name
from d_athlinks_api_vw_athlete_non_member where country_id in ('US','CA');