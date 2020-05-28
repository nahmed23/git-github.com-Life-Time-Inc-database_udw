CREATE TABLE [dbo].[r_time_zone] (
    [r_time_zone_id]        INT           NOT NULL,
    [abbreviation]          VARCHAR (5)   NULL,
    [time_zone_name]        VARCHAR (255) NULL,
    [utc_offset_hour]       INT           NULL,
    [utc_offset_minute]     INT           NULL,
    [dv_load_date_time]     DATETIME      NOT NULL,
    [dv_load_end_date_time] DATETIME      NOT NULL,
    [dv_batch_id]           BIGINT        NOT NULL,
    [dv_r_load_source_id]   BIGINT        NOT NULL,
    [dv_inserted_date_time] DATETIME      NOT NULL,
    [dv_insert_user]        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]  DATETIME      NULL,
    [dv_update_user]        VARCHAR (50)  NULL,
    [dv_hash]               CHAR (32)     NOT NULL,
    [dv_deleted]            BIT           NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([r_time_zone_id]));

