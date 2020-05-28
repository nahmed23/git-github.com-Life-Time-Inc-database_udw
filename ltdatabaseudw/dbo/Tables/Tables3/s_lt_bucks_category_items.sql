﻿CREATE TABLE [dbo].[s_lt_bucks_category_items] (
    [s_lt_bucks_category_items_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)       NOT NULL,
    [citem_id]                     INT             NULL,
    [citem_active]                 BIT             NULL,
    [citem_show_inventory]         BIT             NULL,
    [citem_conversion]             NVARCHAR (50)   NULL,
    [citem_order]                  INT             NULL,
    [citem_frt]                    DECIMAL (26, 6) NULL,
    [citem_conversion_points]      NVARCHAR (50)   NULL,
    [citem_date_created]           SMALLDATETIME   NULL,
    [citem_date_modified]          SMALLDATETIME   NULL,
    [citem_needs_approval]         BIT             NULL,
    [citem_display_start_date]     DATETIME2 (7)   NULL,
    [citem_display_end_date]       DATETIME2 (7)   NULL,
    [last_modified_timestamp]      DATETIME        NULL,
    [dv_load_date_time]            DATETIME        NOT NULL,
    [dv_batch_id]                  BIGINT          NOT NULL,
    [dv_r_load_source_id]          BIGINT          NOT NULL,
    [dv_inserted_date_time]        DATETIME        NOT NULL,
    [dv_insert_user]               VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]         DATETIME        NULL,
    [dv_update_user]               VARCHAR (50)    NULL,
    [dv_hash]                      CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_lt_bucks_category_items]
    ON [dbo].[s_lt_bucks_category_items]([bk_hash] ASC, [s_lt_bucks_category_items_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_lt_bucks_category_items]([dv_batch_id] ASC);

