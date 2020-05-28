CREATE TABLE [dbo].[s_mms_web_order] (
    [s_mms_web_order_id]    BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [web_order_id]          INT             NULL,
    [placed_order_total]    DECIMAL (26, 6) NULL,
    [revised_order_total]   DECIMAL (26, 6) NULL,
    [balance_due]           DECIMAL (26, 6) NULL,
    [placed_date_time]      DATETIME        NULL,
    [revised_date_time]     DATETIME        NULL,
    [ip_address]            VARCHAR (16)    NULL,
    [expiration_date_time]  DATETIME        NULL,
    [inserted_date_time]    DATETIME        NULL,
    [updated_date_time]     DATETIME        NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_web_order]
    ON [dbo].[s_mms_web_order]([bk_hash] ASC, [s_mms_web_order_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_web_order]([dv_batch_id] ASC);

