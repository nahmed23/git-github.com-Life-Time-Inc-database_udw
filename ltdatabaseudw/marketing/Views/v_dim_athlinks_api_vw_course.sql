CREATE VIEW [marketing].[v_dim_athlinks_api_vw_course]
AS select d_athlinks_api_vw_course.course_id course_id,
       d_athlinks_api_vw_course.course_name course_name,
       d_athlinks_api_vw_course.course_pattern course_pattern,
       d_athlinks_api_vw_course.course_pattern_d_athlinks_api_vw_course_bk_hash course_pattern_d_athlinks_api_vw_course_bk_hash,
       d_athlinks_api_vw_course.course_pattern_id course_pattern_id,
       d_athlinks_api_vw_course.course_pattern_outer_d_athlinks_api_vw_course_bk_hash course_pattern_outer_d_athlinks_api_vw_course_bk_hash,
       d_athlinks_api_vw_course.course_pattern_outer_id course_pattern_outer_id,
       d_athlinks_api_vw_course.course_pattern_outer_name course_pattern_outer_name,
       d_athlinks_api_vw_course.create_date create_date,
       d_athlinks_api_vw_course.create_dim_date_key create_dim_date_key,
       d_athlinks_api_vw_course.create_dim_time_key create_dim_time_key,
       d_athlinks_api_vw_course.d_athlinks_api_vw_race_ltf_data_bk_hash d_athlinks_api_vw_race_ltf_data_bk_hash,
       d_athlinks_api_vw_course.dist_type_id dist_type_id,
       d_athlinks_api_vw_course.dist_unit dist_unit,
       d_athlinks_api_vw_course.event_course_d_athlinks_api_vw_course_bk_hash event_course_d_athlinks_api_vw_course_bk_hash,
       d_athlinks_api_vw_course.event_course_id event_course_id,
       d_athlinks_api_vw_course.gallery_id gallery_id,
       d_athlinks_api_vw_course.overall_count overall_count,
       d_athlinks_api_vw_course.race_cat_d_athlinks_api_vw_race_ltf_data_bk_hash race_cat_d_athlinks_api_vw_race_ltf_data_bk_hash,
       d_athlinks_api_vw_course.race_cat_desc race_cat_desc,
       d_athlinks_api_vw_course.race_cat_id race_cat_id,
       d_athlinks_api_vw_course.race_id race_id,
       d_athlinks_api_vw_course.results_date results_date,
       d_athlinks_api_vw_course.results_dim_date_key results_dim_date_key,
       d_athlinks_api_vw_course.results_dim_time_key results_dim_time_key,
       d_athlinks_api_vw_course.results_user results_user,
       d_athlinks_api_vw_course.settings settings
 from d_athlinks_api_vw_course 
   left join 
   d_athlinks_api_vw_race_ltf_data
   on d_athlinks_api_vw_course.d_athlinks_api_vw_race_ltf_data_bk_hash =d_athlinks_api_vw_race_ltf_data.bk_hash 
  where d_athlinks_api_vw_race_ltf_data.country_id in ('US','CA');