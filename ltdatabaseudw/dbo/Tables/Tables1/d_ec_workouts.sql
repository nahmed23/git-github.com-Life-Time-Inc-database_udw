CREATE TABLE [dbo].[d_ec_workouts] (
    [d_ec_workouts_id]            BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)      NOT NULL,
    [dim_trainerize_workout_key]  VARCHAR (32)   NULL,
    [workouts_id]                 INT            NULL,
    [created_dim_date_key]        VARCHAR (8)    NULL,
    [d_ec_workouts_party_bk_hash] CHAR (32)      NULL,
    [description]                 VARCHAR (8000) NULL,
    [discriminator]               VARCHAR (128)  NULL,
    [ec_workouts_party_bk_hash]   VARCHAR (32)   NULL,
    [inactive_dim_date_key]       VARCHAR (8)    NULL,
    [modified_dim_date_key]       VARCHAR (8)    NULL,
    [name]                        VARCHAR (100)  NULL,
    [party_id]                    INT            NULL,
    [tags]                        VARCHAR (8000) NULL,
    [type]                        INT            NULL,
    [p_ec_workouts_id]            BIGINT         NOT NULL,
    [deleted_flag]                INT            NULL,
    [dv_load_date_time]           DATETIME       NULL,
    [dv_load_end_date_time]       DATETIME       NULL,
    [dv_batch_id]                 BIGINT         NOT NULL,
    [dv_inserted_date_time]       DATETIME       NOT NULL,
    [dv_insert_user]              VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]        DATETIME       NULL,
    [dv_update_user]              VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

