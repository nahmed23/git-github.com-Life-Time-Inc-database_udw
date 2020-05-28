CREATE TABLE [dbo].[s_hybris_pgrels] (
    [s_hybris_pgrels_id]    BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)      NOT NULL,
    [hjmpts]                BIGINT         NULL,
    [created_ts]            DATETIME       NULL,
    [modified_ts]           DATETIME       NULL,
    [pgrels_pk]             BIGINT         NULL,
    [language_pk]           BIGINT         NULL,
    [qualifier]             NVARCHAR (255) NULL,
    [source_pk]             BIGINT         NULL,
    [target_pk]             BIGINT         NULL,
    [sequence_number]       INT            NULL,
    [r_sequence_number]     INT            NULL,
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
CREATE CLUSTERED INDEX [ci_s_hybris_pgrels]
    ON [dbo].[s_hybris_pgrels]([bk_hash] ASC, [s_hybris_pgrels_id] ASC);

