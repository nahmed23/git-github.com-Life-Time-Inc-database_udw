CREATE TABLE [dbo].[stage_magento_checkout_agreement] (
    [stage_magento_checkout_agreement_id] BIGINT         NOT NULL,
    [agreement_id]                        INT            NULL,
    [name]                                VARCHAR (255)  NULL,
    [content]                             VARCHAR (8000) NULL,
    [content_height]                      VARCHAR (25)   NULL,
    [checkbox_text]                       VARCHAR (8000) NULL,
    [is_active]                           INT            NULL,
    [is_html]                             INT            NULL,
    [mode]                                INT            NULL,
    [dummy_modified_date_time]            DATETIME       NULL,
    [dv_batch_id]                         BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

