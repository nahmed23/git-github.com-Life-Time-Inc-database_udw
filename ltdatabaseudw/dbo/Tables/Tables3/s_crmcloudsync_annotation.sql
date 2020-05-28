CREATE TABLE [dbo].[s_crmcloudsync_annotation] (
    [s_crmcloudsync_annotation_id]    BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)      NOT NULL,
    [annotation_id]                   VARCHAR (36)   NULL,
    [created_by_name]                 NVARCHAR (200) NULL,
    [created_by_yomi_name]            NVARCHAR (200) NULL,
    [created_on]                      DATETIME       NULL,
    [created_on_behalf_by_name]       NVARCHAR (200) NULL,
    [created_on_behalf_by_yomi_name]  NVARCHAR (200) NULL,
    [document_body]                   VARCHAR (8000) NULL,
    [file_name]                       NVARCHAR (255) NULL,
    [file_size]                       INT            NULL,
    [import_sequence_number]          INT            NULL,
    [is_document]                     INT            NULL,
    [is_document_name]                NVARCHAR (255) NULL,
    [is_private_name]                 NVARCHAR (255) NULL,
    [lang_id]                         NVARCHAR (2)   NULL,
    [mime_type]                       NVARCHAR (256) NULL,
    [modified_by_name]                NVARCHAR (200) NULL,
    [modified_by_yomi_name]           NVARCHAR (200) NULL,
    [modified_on]                     DATETIME       NULL,
    [modified_on_behalf_by_name]      NVARCHAR (200) NULL,
    [modified_on_behalf_by_yomi_name] NVARCHAR (200) NULL,
    [note_text]                       VARCHAR (8000) NULL,
    [object_id_type_code]             NVARCHAR (64)  NULL,
    [object_type_code]                NVARCHAR (64)  NULL,
    [object_type_code_name]           NVARCHAR (255) NULL,
    [over_ridden_created_on]          DATETIME       NULL,
    [owner_id_name]                   NVARCHAR (200) NULL,
    [owner_id_type]                   NVARCHAR (64)  NULL,
    [owner_id_yomi_name]              NVARCHAR (200) NULL,
    [step_id]                         NVARCHAR (32)  NULL,
    [subject]                         NVARCHAR (500) NULL,
    [version_number]                  BIGINT         NULL,
    [inserted_date_time]              DATETIME       NULL,
    [insert_user]                     VARCHAR (100)  NULL,
    [updated_date_time]               DATETIME       NULL,
    [update_user]                     VARCHAR (50)   NULL,
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
CREATE CLUSTERED INDEX [ci_s_crmcloudsync_annotation]
    ON [dbo].[s_crmcloudsync_annotation]([bk_hash] ASC, [s_crmcloudsync_annotation_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_crmcloudsync_annotation]([dv_batch_id] ASC);

