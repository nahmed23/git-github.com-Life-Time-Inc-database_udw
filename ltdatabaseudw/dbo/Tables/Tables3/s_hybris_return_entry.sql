CREATE TABLE [dbo].[s_hybris_return_entry] (
    [s_hybris_return_entry_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)       NOT NULL,
    [hjmp_ts]                  BIGINT          NULL,
    [created_ts]               DATETIME        NULL,
    [modified_ts]              DATETIME        NULL,
    [return_entry_pk]          BIGINT          NULL,
    [p_order_entry]            BIGINT          NULL,
    [p_expected_quantity]      BIGINT          NULL,
    [p_received_quantity]      BIGINT          NULL,
    [p_reached_date]           DATETIME        NULL,
    [p_status]                 BIGINT          NULL,
    [p_action]                 BIGINT          NULL,
    [p_notes]                  NVARCHAR (255)  NULL,
    [p_return_request_pos]     INT             NULL,
    [p_return_request]         BIGINT          NULL,
    [acl_ts]                   BIGINT          NULL,
    [prop_ts]                  BIGINT          NULL,
    [p_reason]                 BIGINT          NULL,
    [p_amount]                 DECIMAL (30, 8) NULL,
    [p_refunded_date]          DATETIME        NULL,
    [dv_load_date_time]        DATETIME        NOT NULL,
    [dv_r_load_source_id]      BIGINT          NOT NULL,
    [dv_inserted_date_time]    DATETIME        NOT NULL,
    [dv_insert_user]           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]     DATETIME        NULL,
    [dv_update_user]           VARCHAR (50)    NULL,
    [dv_hash]                  CHAR (32)       NOT NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_return_entry]
    ON [dbo].[s_hybris_return_entry]([bk_hash] ASC, [s_hybris_return_entry_id] ASC);

