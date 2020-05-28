CREATE TABLE [dbo].[s_commprefs_communication_values] (
    [s_commprefs_communication_values_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)      NOT NULL,
    [communication_values_id]             INT            NULL,
    [value]                               NVARCHAR (255) NULL,
    [token]                               VARCHAR (8000) NULL,
    [token_created_time]                  DATETIME       NULL,
    [created_time]                        DATETIME       NULL,
    [updated_time]                        DATETIME       NULL,
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
CREATE CLUSTERED INDEX [ci_s_commprefs_communication_values]
    ON [dbo].[s_commprefs_communication_values]([bk_hash] ASC, [s_commprefs_communication_values_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_commprefs_communication_values]([dv_batch_id] ASC);

