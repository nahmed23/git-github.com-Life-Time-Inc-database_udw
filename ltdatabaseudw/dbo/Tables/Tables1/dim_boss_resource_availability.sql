CREATE TABLE [dbo].[dim_boss_resource_availability] (
    [dim_boss_resource_availability_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [capacity]                           INT          NULL,
    [d_boss_asi_resource_id]             BIGINT       NULL,
    [dim_boss_resource_availability_key] CHAR (32)    NULL,
    [dim_club_key]                       CHAR (32)    NULL,
    [dim_employee_key]                   CHAR (32)    NULL,
    [employee_id]                        INT          NULL,
    [end_dim_date_key]                   CHAR (8)     NULL,
    [end_dim_time_key]                   CHAR (8)     NULL,
    [resource]                           CHAR (25)    NULL,
    [resource_type]                      CHAR (25)    NULL,
    [start_dim_date_key]                 CHAR (8)     NULL,
    [start_dim_time_key]                 CHAR (8)     NULL,
    [status]                             CHAR (1)     NULL,
    [dv_load_date_time]                  DATETIME     NULL,
    [dv_load_end_date_time]              DATETIME     NULL,
    [dv_batch_id]                        BIGINT       NOT NULL,
    [dv_inserted_date_time]              DATETIME     NOT NULL,
    [dv_insert_user]                     VARCHAR (50) NOT NULL,
    [dv_updated_date_time]               DATETIME     NULL,
    [dv_update_user]                     VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_boss_resource_availability_key]));

