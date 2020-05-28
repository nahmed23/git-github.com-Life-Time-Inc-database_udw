CREATE TABLE [dbo].[s_hybris_ltf_refund_entry] (
    [s_hybris_ltf_refund_entry_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)       NOT NULL,
    [hjmpts]                       BIGINT          NULL,
    [ltf_refund_entry_pk]          BIGINT          NULL,
    [created_ts]                   DATETIME        NULL,
    [modified_ts]                  DATETIME        NULL,
    [acl_ts]                       INT             NULL,
    [prop_ts]                      INT             NULL,
    [p_refunded_date]              DATETIME        NULL,
    [p_amount]                     DECIMAL (30, 8) NULL,
    [p_refund_note]                NVARCHAR (4000) NULL,
    [dv_load_date_time]            DATETIME        NOT NULL,
    [dv_r_load_source_id]          BIGINT          NOT NULL,
    [dv_inserted_date_time]        DATETIME        NOT NULL,
    [dv_insert_user]               VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]         DATETIME        NULL,
    [dv_update_user]               VARCHAR (50)    NULL,
    [dv_hash]                      CHAR (32)       NOT NULL,
    [dv_batch_id]                  BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_ltf_refund_entry]
    ON [dbo].[s_hybris_ltf_refund_entry]([bk_hash] ASC, [s_hybris_ltf_refund_entry_id] ASC);

