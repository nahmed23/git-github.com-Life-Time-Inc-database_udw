CREATE TABLE [dbo].[stage_lt_bucks_CategoryItems] (
    [stage_lt_bucks_CategoryItems_id] BIGINT          NOT NULL,
    [citem_id]                        INT             NULL,
    [citem_product]                   INT             NULL,
    [citem_category]                  INT             NULL,
    [citem_active]                    BIT             NULL,
    [citem_show_inventory]            BIT             NULL,
    [citem_conversion]                NVARCHAR (50)   NULL,
    [citem_order]                     INT             NULL,
    [citem_frt]                       DECIMAL (26, 6) NULL,
    [citem_conversion_points]         NVARCHAR (50)   NULL,
    [citem_date_created]              SMALLDATETIME   NULL,
    [citem_date_modified]             SMALLDATETIME   NULL,
    [citem_needs_approval]            BIT             NULL,
    [citem_display_start_date]        DATETIME2 (7)   NULL,
    [citem_display_end_date]          DATETIME2 (7)   NULL,
    [LastModifiedTimestamp]           DATETIME        NULL,
    [dv_batch_id]                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

