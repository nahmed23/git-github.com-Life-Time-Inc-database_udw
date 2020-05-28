CREATE TABLE [dbo].[d_commprefs_communication_types] (
    [d_commprefs_communication_types_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)      NOT NULL,
    [communication_types_id]             INT            NULL,
    [active_on_dim_date_key]             CHAR (8)       NULL,
    [active_until_dim_date_key]          CHAR (8)       NULL,
    [created_dim_date_key]               CHAR (8)       NULL,
    [name]                               VARCHAR (8000) NULL,
    [opt_in_flag]                        CHAR (1)       NULL,
    [slug]                               VARCHAR (8000) NULL,
    [p_commprefs_communication_types_id] BIGINT         NOT NULL,
    [dv_load_date_time]                  DATETIME       NULL,
    [dv_load_end_date_time]              DATETIME       NULL,
    [dv_batch_id]                        BIGINT         NOT NULL,
    [dv_inserted_date_time]              DATETIME       NOT NULL,
    [dv_insert_user]                     VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]               DATETIME       NULL,
    [dv_update_user]                     VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_commprefs_communication_types]([dv_batch_id] ASC);

