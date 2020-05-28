CREATE TABLE [dbo].[s_exerp_booking_1] (
    [s_exerp_booking_1_id]  BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)      NOT NULL,
    [booking_id]            VARCHAR (4000) NULL,
    [single_cancellation]   BIT            NULL,
    [strict_age_limit]      INT            NULL,
    [minimum_age]           INT            NULL,
    [maximum_age]           INT            NULL,
    [minimum_age_unit]      VARCHAR (4000) NULL,
    [maximum_age_unit]      VARCHAR (4000) NULL,
    [age_text]              VARCHAR (4000) NULL,
    [dv_load_date_time]     DATETIME       NOT NULL,
    [dv_r_load_source_id]   BIGINT         NOT NULL,
    [dv_inserted_date_time] DATETIME       NOT NULL,
    [dv_insert_user]        VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]  DATETIME       NULL,
    [dv_update_user]        VARCHAR (50)   NULL,
    [dv_hash]               CHAR (32)      NOT NULL,
    [dv_deleted]            BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]           BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

