CREATE TABLE [dbo].[stage_loc_val_hour_type] (
    [stage_loc_val_hour_type_id] BIGINT         NOT NULL,
    [val_hour_type_id]           BIGINT         NULL,
    [val_hour_type_name]         VARCHAR (100)  NULL,
    [display_name]               VARCHAR (4000) NULL,
    [val_hour_type_group_id]     BIGINT         NULL,
    [created_date_time]          DATETIME       NULL,
    [created_by]                 VARCHAR (100)  NULL,
    [last_updated_date_time]     DATETIME       NULL,
    [last_updated_by]            VARCHAR (100)  NULL,
    [deleted_date_time]          DATETIME       NULL,
    [deleted_by]                 VARCHAR (100)  NULL,
    [managed_by_udw]             BIT            NULL,
    [dv_batch_id]                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

