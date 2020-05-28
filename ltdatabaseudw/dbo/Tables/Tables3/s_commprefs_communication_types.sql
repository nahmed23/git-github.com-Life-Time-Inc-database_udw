CREATE TABLE [dbo].[s_commprefs_communication_types] (
    [s_commprefs_communication_types_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)      NOT NULL,
    [communication_types_id]             INT            NULL,
    [slug]                               VARCHAR (8000) NULL,
    [name]                               VARCHAR (8000) NULL,
    [description]                        VARCHAR (8000) NULL,
    [sequence]                           TINYINT        NULL,
    [active_on]                          DATETIME       NULL,
    [active_until]                       DATETIME       NULL,
    [created_time]                       DATETIME       NULL,
    [updated_time]                       DATETIME       NULL,
    [opt_in_required]                    BIT            NULL,
    [sample_image_url]                   VARCHAR (8000) NULL,
    [dv_load_date_time]                  DATETIME       NOT NULL,
    [dv_r_load_source_id]                BIGINT         NOT NULL,
    [dv_inserted_date_time]              DATETIME       NOT NULL,
    [dv_insert_user]                     VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]               DATETIME       NULL,
    [dv_update_user]                     VARCHAR (50)   NULL,
    [dv_hash]                            CHAR (32)      NOT NULL,
    [dv_batch_id]                        BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_commprefs_communication_types]
    ON [dbo].[s_commprefs_communication_types]([bk_hash] ASC, [s_commprefs_communication_types_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_commprefs_communication_types]([dv_batch_id] ASC);

