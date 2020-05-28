CREATE TABLE [dbo].[l_hybris_affiliate_entry_details] (
    [l_hybris_affiliate_entry_details_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)      NOT NULL,
    [type_pk_string]                      BIGINT         NULL,
    [affiliate_entry_details_pk]          BIGINT         NULL,
    [owner_pk_string]                     BIGINT         NULL,
    [ltf_employee_id]                     NVARCHAR (255) NULL,
    [ltf_party_id]                        NVARCHAR (255) NULL,
    [ltf_affiliate_id]                    NVARCHAR (255) NULL,
    [dv_load_date_time]                   DATETIME       NOT NULL,
    [dv_r_load_source_id]                 BIGINT         NOT NULL,
    [dv_inserted_date_time]               DATETIME       NOT NULL,
    [dv_insert_user]                      VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                DATETIME       NULL,
    [dv_update_user]                      VARCHAR (50)   NULL,
    [dv_hash]                             CHAR (32)      NOT NULL,
    [dv_batch_id]                         BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_affiliate_entry_details]
    ON [dbo].[l_hybris_affiliate_entry_details]([bk_hash] ASC, [l_hybris_affiliate_entry_details_id] ASC);

