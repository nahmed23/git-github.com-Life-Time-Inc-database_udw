CREATE TABLE [dbo].[stage_hash_commprefs_CommunicationValues] (
    [stage_hash_commprefs_CommunicationValues_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)      NOT NULL,
    [Id]                                          INT            NULL,
    [Value]                                       NVARCHAR (255) NULL,
    [Token]                                       VARCHAR (8000) NULL,
    [TokenCreatedTime]                            DATETIME       NULL,
    [CreatedTime]                                 DATETIME       NULL,
    [UpdatedTime]                                 DATETIME       NULL,
    [ChannelKey]                                  NVARCHAR (128) NULL,
    [dv_load_date_time]                           DATETIME       NOT NULL,
    [dv_inserted_date_time]                       DATETIME       NOT NULL,
    [dv_insert_user]                              VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                        DATETIME       NULL,
    [dv_update_user]                              VARCHAR (50)   NULL,
    [dv_batch_id]                                 BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

