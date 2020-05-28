CREATE TABLE [dbo].[s_hybris_payment_infos] (
    [s_hybris_payment_infos_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [hjmpts]                    BIGINT         NULL,
    [payment_infos_pk]          BIGINT         NULL,
    [created_ts]                DATETIME       NULL,
    [modified_ts]               DATETIME       NULL,
    [acl_ts]                    INT            NULL,
    [prop_ts]                   INT            NULL,
    [user_pk]                   BIGINT         NULL,
    [p_issue_number]            INT            NULL,
    [p_bank]                    NVARCHAR (255) NULL,
    [p_ccowner]                 NVARCHAR (255) NULL,
    [p_subscription_id]         NVARCHAR (255) NULL,
    [p_bank_id_number]          NVARCHAR (255) NULL,
    [p_valid_from_month]        NVARCHAR (255) NULL,
    [p_saved]                   TINYINT        NULL,
    [p_ba_owner]                NVARCHAR (255) NULL,
    [p_valid_from_year]         NVARCHAR (255) NULL,
    [p_valid_to_year]           NVARCHAR (255) NULL,
    [p_account_number]          NVARCHAR (255) NULL,
    [p_valid_to_month]          NVARCHAR (255) NULL,
    [p_mocked_flag]             TINYINT        NULL,
    [p_nick_name]               NVARCHAR (255) NULL,
    [original_pk]               BIGINT         NULL,
    [duplicate]                 TINYINT        NULL,
    [p_ltf_criteria]            NVARCHAR (255) NULL,
    [p_credit_card_token]       NVARCHAR (255) NULL,
    [p_subscription_validated]  TINYINT        NULL,
    [p_payment_id]              NVARCHAR (255) NULL,
    [p_payer_id]                NVARCHAR (255) NULL,
    [p_token]                   NVARCHAR (255) NULL,
    [p_payer]                   NVARCHAR (255) NULL,
    [dv_load_date_time]         DATETIME       NOT NULL,
    [dv_r_load_source_id]       BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL,
    [dv_hash]                   CHAR (32)      NOT NULL,
    [dv_batch_id]               BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_payment_infos]
    ON [dbo].[s_hybris_payment_infos]([bk_hash] ASC, [s_hybris_payment_infos_id] ASC);

