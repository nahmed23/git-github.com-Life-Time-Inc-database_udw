CREATE TABLE [dbo].[l_hybris_paymnt_trnsct_entries] (
    [l_hybris_paymnt_trnsct_entries_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)      NOT NULL,
    [type_pk_string]                    BIGINT         NULL,
    [owner_pk_string]                   BIGINT         NULL,
    [paymnt_trnsct_entries_pk]          BIGINT         NULL,
    [p_type]                            BIGINT         NULL,
    [p_currency]                        BIGINT         NULL,
    [p_request_token]                   NVARCHAR (255) NULL,
    [p_request_id]                      NVARCHAR (255) NULL,
    [p_subscription_id]                 NVARCHAR (255) NULL,
    [p_version_id]                      NVARCHAR (255) NULL,
    [p_payment_transaction]             BIGINT         NULL,
    [dv_load_date_time]                 DATETIME       NOT NULL,
    [dv_batch_id]                       BIGINT         NOT NULL,
    [dv_r_load_source_id]               BIGINT         NOT NULL,
    [dv_inserted_date_time]             DATETIME       NOT NULL,
    [dv_insert_user]                    VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]              DATETIME       NULL,
    [dv_update_user]                    VARCHAR (50)   NULL,
    [dv_hash]                           CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_paymnt_trnsct_entries]
    ON [dbo].[l_hybris_paymnt_trnsct_entries]([bk_hash] ASC, [l_hybris_paymnt_trnsct_entries_id] ASC);

