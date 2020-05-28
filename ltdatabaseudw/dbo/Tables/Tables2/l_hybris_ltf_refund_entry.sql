CREATE TABLE [dbo].[l_hybris_ltf_refund_entry] (
    [l_hybris_ltf_refund_entry_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [type_pk_string]               BIGINT       NULL,
    [ltf_refund_entry_pk]          BIGINT       NULL,
    [owner_pk_string]              BIGINT       NULL,
    [p_reason]                     BIGINT       NULL,
    [p_refund_status]              BIGINT       NULL,
    [p_order_entries]              BIGINT       NULL,
    [p_refund_pay_type]            BIGINT       NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_r_load_source_id]          BIGINT       NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_hash]                      CHAR (32)    NOT NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_ltf_refund_entry]
    ON [dbo].[l_hybris_ltf_refund_entry]([bk_hash] ASC, [l_hybris_ltf_refund_entry_id] ASC);

