CREATE TABLE [dbo].[dim_trainerize_workout] (
    [dim_trainerize_workout_id]  BIGINT         IDENTITY (1, 1) NOT NULL,
    [created_dim_date_key]       VARCHAR (8)    NULL,
    [dim_mms_member_key]         VARCHAR (32)   NULL,
    [dim_trainerize_workout_key] VARCHAR (32)   NULL,
    [discriminator]              VARCHAR (128)  NULL,
    [inactive_dim_date_key]      VARCHAR (8)    NULL,
    [modified_dim_date_key]      VARCHAR (8)    NULL,
    [tags]                       VARCHAR (4000) NULL,
    [workout_description]        VARCHAR (4000) NULL,
    [workout_name]               VARCHAR (100)  NULL,
    [workout_type]               INT            NULL,
    [workouts_id]                INT            NULL,
    [dv_load_date_time]          DATETIME       NULL,
    [dv_load_end_date_time]      DATETIME       NULL,
    [dv_batch_id]                BIGINT         NOT NULL,
    [dv_inserted_date_time]      DATETIME       NOT NULL,
    [dv_insert_user]             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]       DATETIME       NULL,
    [dv_update_user]             VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_trainerize_workout_key]));

