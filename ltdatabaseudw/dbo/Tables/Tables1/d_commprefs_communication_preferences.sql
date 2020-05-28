CREATE TABLE [dbo].[d_commprefs_communication_preferences] (
    [d_commprefs_communication_preferences_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                         CHAR (32)    NOT NULL,
    [d_commprefs_communication_preferences_key]       CHAR (32)    NULL,
    [communication_preferences_id]                    INT          NULL,
    [created_date_key]                                CHAR (8)     NULL,
    [d_commprefs_communication_type_channels_bk_hash] CHAR (32)    NULL,
    [d_commprefs_communication_values_bk_hash]        CHAR (32)    NULL,
    [effective_date_key]                              CHAR (8)     NULL,
    [opt_in_flag]                                     CHAR (1)     NULL,
    [updated_date_key]                                CHAR (8)     NULL,
    [p_commprefs_communication_preferences_id]        BIGINT       NOT NULL,
    [dv_load_date_time]                               DATETIME     NULL,
    [dv_load_end_date_time]                           DATETIME     NULL,
    [dv_batch_id]                                     BIGINT       NOT NULL,
    [dv_inserted_date_time]                           DATETIME     NOT NULL,
    [dv_insert_user]                                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                            DATETIME     NULL,
    [dv_update_user]                                  VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_commprefs_communication_preferences]([dv_batch_id] ASC);

