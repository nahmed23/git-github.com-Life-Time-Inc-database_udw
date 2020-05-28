CREATE TABLE [dbo].[l_hybris_fulfillment_partner] (
    [l_hybris_fulfillment_partner_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)      NOT NULL,
    [type_pk_string]                  BIGINT         NULL,
    [fulfillment_partner_pk]          BIGINT         NULL,
    [owner_pk_string]                 BIGINT         NULL,
    [p_export_file_format]            BIGINT         NULL,
    [p_import_file_format]            BIGINT         NULL,
    [p_receiver_code_id]              NVARCHAR (255) NULL,
    [p_receiver_id]                   NVARCHAR (255) NULL,
    [p_sender_id]                     NVARCHAR (255) NULL,
    [dv_load_date_time]               DATETIME       NOT NULL,
    [dv_r_load_source_id]             BIGINT         NOT NULL,
    [dv_inserted_date_time]           DATETIME       NOT NULL,
    [dv_insert_user]                  VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]            DATETIME       NULL,
    [dv_update_user]                  VARCHAR (50)   NULL,
    [dv_hash]                         CHAR (32)      NOT NULL,
    [dv_batch_id]                     BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_fulfillment_partner]
    ON [dbo].[l_hybris_fulfillment_partner]([bk_hash] ASC, [l_hybris_fulfillment_partner_id] ASC);

