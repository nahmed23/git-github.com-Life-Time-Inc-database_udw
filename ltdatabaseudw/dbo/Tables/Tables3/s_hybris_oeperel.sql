CREATE TABLE [dbo].[s_hybris_oeperel] (
    [s_hybris_oeperel_id]   BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)      NOT NULL,
    [hjmpts]                BIGINT         NULL,
    [modified_ts]           DATETIME       NULL,
    [created_ts]            DATETIME       NULL,
    [oeperel_pk]            BIGINT         NULL,
    [r_sequence_number]     INT            NULL,
    [sequence_number]       INT            NULL,
    [qualifier]             NVARCHAR (255) NULL,
    [acl_ts]                BIGINT         NULL,
    [prop_ts]               BIGINT         NULL,
    [dv_load_date_time]     DATETIME       NOT NULL,
    [dv_r_load_source_id]   BIGINT         NOT NULL,
    [dv_inserted_date_time] DATETIME       NOT NULL,
    [dv_insert_user]        VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]  DATETIME       NULL,
    [dv_update_user]        VARCHAR (50)   NULL,
    [dv_hash]               CHAR (32)      NOT NULL,
    [dv_batch_id]           BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_oeperel]
    ON [dbo].[s_hybris_oeperel]([bk_hash] ASC, [s_hybris_oeperel_id] ASC);

