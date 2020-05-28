CREATE VIEW [marketing].[v_dim_lifetime_workout]
AS select dim_trainerize_workout.created_dim_date_key created_dim_date_key,
       dim_trainerize_workout.dim_mms_member_key dim_mms_member_key,
       dim_trainerize_workout.dim_trainerize_workout_key dim_trainerize_workout_key,
       dim_trainerize_workout.discriminator discriminator,
       dim_trainerize_workout.inactive_dim_date_key inactive_dim_date_key,
       dim_trainerize_workout.modified_dim_date_key modified_dim_date_key,
       dim_trainerize_workout.tags tags,
       dim_trainerize_workout.workout_description workout_description,
       dim_trainerize_workout.workout_name workout_name,
       dim_trainerize_workout.workout_type workout_type,
       dim_trainerize_workout.workouts_id workouts_id
  from dbo.dim_trainerize_workout;