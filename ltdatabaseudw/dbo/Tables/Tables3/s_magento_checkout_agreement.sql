CREATE TABLE [dbo].[s_magento_checkout_agreement] (
    [s_magento_checkout_agreement_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)      NOT NULL,
    [agreement_id]                    INT            NULL,
    [name]                            VARCHAR (255)  NULL,
    [content]                         VARCHAR (8000) NULL,
    [content_height]                  VARCHAR (25)   NULL,
    [checkbox_text]                   VARCHAR (8000) NULL,
    [is_active]                       INT            NULL,
    [is_html]                         INT            NULL,
    [mode]                            INT            NULL,
    [dummy_modified_date_time]        DATETIME       NULL,
    [dv_load_date_time]               DATETIME       NOT NULL,
    [dv_r_load_source_id]             BIGINT         NOT NULL,
    [dv_inserted_date_time]           DATETIME       NOT NULL,
    [dv_insert_user]                  VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]            DATETIME       NULL,
    [dv_update_user]                  VARCHAR (50)   NULL,
    [dv_hash]                         CHAR (32)      NOT NULL,
    [dv_deleted]                      BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                     BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

