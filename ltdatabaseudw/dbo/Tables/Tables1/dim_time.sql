CREATE TABLE [dbo].[dim_time] (
    [dim_time_key]                         INT          NOT NULL,
    [hour]                                 INT          NULL,
    [half_hour]                            INT          NULL,
    [hour_quarter]                         INT          NULL,
    [minute]                               INT          NULL,
    [minutes_after_midnight]               INT          NULL,
    [display_12_hour_time]                 CHAR (8)     NULL,
    [display_12_hour_group]                CHAR (19)    NULL,
    [display_12_hour_half_group]           CHAR (19)    NULL,
    [display_12_hour_quarter_group]        CHAR (19)    NULL,
    [display_24_hour_time]                 CHAR (5)     NULL,
    [display_24_hour_group]                CHAR (13)    NULL,
    [display_24_hour_half_group]           CHAR (13)    NULL,
    [display_24_hour_quarter_group]        CHAR (13)    NULL,
    [member_usage_targeted_segment_period] VARCHAR (13) NOT NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_batch_id]                          BIGINT       NOT NULL,
    [dv_r_load_source_id]                  BIGINT       NOT NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_hash]                              CHAR (32)    NOT NULL,
    [dv_deleted]                           BIT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);

