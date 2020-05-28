CREATE TABLE [dbo].[stage_hash_commprefs_CommunicationPreferences] (
    [stage_hash_commprefs_CommunicationPreferences_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)      NOT NULL,
    [Id]                                               INT            NULL,
    [OptIn]                                            BIT            NULL,
    [EffectiveTime]                                    DATETIME       NULL,
    [CreatedTime]                                      DATETIME       NULL,
    [UpdatedTime]                                      DATETIME       NULL,
    [UpdatedBy]                                        VARCHAR (8000) NULL,
    [CommunicationTypeChannelId]                       INT            NULL,
    [CommunicationValueId]                             INT            NULL,
    [dv_load_date_time]                                DATETIME       NOT NULL,
    [dv_inserted_date_time]                            DATETIME       NOT NULL,
    [dv_insert_user]                                   VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                             DATETIME       NULL,
    [dv_update_user]                                   VARCHAR (50)   NULL,
    [dv_batch_id]                                      BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

