CREATE TABLE [dbo].[fact_boss_participation] (
    [fact_boss_participation_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [dim_boss_reservation_key]    CHAR (32)    NULL,
    [fact_boss_participation_key] CHAR (32)    NULL,
    [instructor_type]             CHAR (1)     NULL,
    [mod_count]                   INT          NULL,
    [number_of_participants]      INT          NULL,
    [participation_dim_date_key]  CHAR (8)     NULL,
    [participation_id]            INT          NULL,
    [primary_dim_employee_key]    CHAR (32)    NULL,
    [secondary_dim_employee_key]  CHAR (32)    NULL,
    [dv_load_date_time]           DATETIME     NULL,
    [dv_load_end_date_time]       DATETIME     NULL,
    [dv_batch_id]                 BIGINT       NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_boss_participation_key]));

