﻿CREATE VIEW [marketing].[v_dim_athlinks_api_vw_race_ltf_data]
AS select d_athlinks_api_vw_race_ltf_data.race_id race_id,
       d_athlinks_api_vw_race_ltf_data.city city,
       d_athlinks_api_vw_race_ltf_data.company_d_athlinks_api_vw_race_ltf_data_bk_hash company_d_athlinks_api_vw_race_ltf_data_bk_hash,
       d_athlinks_api_vw_race_ltf_data.country_id country_id,
       d_athlinks_api_vw_race_ltf_data.country_id_3 country_id_3,
       d_athlinks_api_vw_race_ltf_data.country_name country_name,
       d_athlinks_api_vw_race_ltf_data.create_date create_date,
       d_athlinks_api_vw_race_ltf_data.create_dim_date_key create_dim_date_key,
       d_athlinks_api_vw_race_ltf_data.create_dim_time_key create_dim_time_key,
       d_athlinks_api_vw_race_ltf_data.d_athlinks_api_vw_master_event_bk_hash d_athlinks_api_vw_master_event_bk_hash,
       d_athlinks_api_vw_race_ltf_data.date_sort date_sort,
       d_athlinks_api_vw_race_ltf_data.elevation elevation,
       d_athlinks_api_vw_race_ltf_data.latitude latitude,
       d_athlinks_api_vw_race_ltf_data.longitude longitude,
       d_athlinks_api_vw_race_ltf_data.master_id master_id,
       d_athlinks_api_vw_race_ltf_data.race_company_id race_company_id,
       d_athlinks_api_vw_race_ltf_data.race_date race_date,
       d_athlinks_api_vw_race_ltf_data.race_dim_date_key race_dim_date_key,
       d_athlinks_api_vw_race_ltf_data.race_dim_time_key race_dim_time_key,
       d_athlinks_api_vw_race_ltf_data.race_end_date race_end_date,
       d_athlinks_api_vw_race_ltf_data.race_end_dim_date_key race_end_dim_date_key,
       d_athlinks_api_vw_race_ltf_data.race_end_dim_time_key race_end_dim_time_key,
       d_athlinks_api_vw_race_ltf_data.race_name race_name,
       d_athlinks_api_vw_race_ltf_data.result_count result_count,
       d_athlinks_api_vw_race_ltf_data.state_prov_abbrev state_prov_abbrev,
       d_athlinks_api_vw_race_ltf_data.state_prov_id state_prov_id,
       d_athlinks_api_vw_race_ltf_data.state_prov_name state_prov_name,
       d_athlinks_api_vw_race_ltf_data.status status,
       d_athlinks_api_vw_race_ltf_data.temperature temperature,
       d_athlinks_api_vw_race_ltf_data.weather_notes weather_notes,
       d_athlinks_api_vw_race_ltf_data.website website
from d_athlinks_api_vw_race_ltf_data where country_id in ('US','CA');