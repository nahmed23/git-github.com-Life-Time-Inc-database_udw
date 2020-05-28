CREATE TABLE [dbo].[s_exerp_document] (
    [s_exerp_document_id]    BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)      NOT NULL,
    [document_id]            INT            NULL,
    [creation_datetime]      DATETIME       NULL,
    [type]                   VARCHAR (4000) NULL,
    [subject]                VARCHAR (4000) NULL,
    [details]                VARCHAR (4000) NULL,
    [require_signature]      BIT            NULL,
    [signatures_signed]      INT            NULL,
    [signatures_missing]     INT            NULL,
    [latest_signed_datetime] DATETIME       NULL,
    [attached_file_name]     VARCHAR (4000) NULL,
    [ets]                    BIGINT         NULL,
    [dv_load_date_time]      DATETIME       NOT NULL,
    [dv_r_load_source_id]    BIGINT         NOT NULL,
    [dv_inserted_date_time]  DATETIME       NOT NULL,
    [dv_insert_user]         VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]   DATETIME       NULL,
    [dv_update_user]         VARCHAR (50)   NULL,
    [dv_hash]                CHAR (32)      NOT NULL,
    [dv_deleted]             BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]            BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exerp_document]([dv_batch_id] ASC);

