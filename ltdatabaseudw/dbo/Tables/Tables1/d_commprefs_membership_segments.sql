CREATE TABLE [dbo].[d_commprefs_membership_segments] (
    [d_commprefs_membership_segments_id]    BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)     NOT NULL,
    [dim_commprefs_membership_segments_key] CHAR (32)     NULL,
    [commprefs_membership_segments_id]      INT           NULL,
    [created_time]                          DATETIME      NULL,
    [membership_segments_key_value]         NVARCHAR (50) NULL,
    [updated_time]                          DATETIME      NULL,
    [p_commprefs_membership_segments_id]    BIGINT        NOT NULL,
    [dv_load_date_time]                     DATETIME      NULL,
    [dv_load_end_date_time]                 DATETIME      NULL,
    [dv_batch_id]                           BIGINT        NOT NULL,
    [dv_inserted_date_time]                 DATETIME      NOT NULL,
    [dv_insert_user]                        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                  DATETIME      NULL,
    [dv_update_user]                        VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_commprefs_membership_segments]([dv_batch_id] ASC);

