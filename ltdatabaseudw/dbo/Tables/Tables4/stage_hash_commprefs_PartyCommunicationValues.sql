CREATE TABLE [dbo].[stage_hash_commprefs_PartyCommunicationValues] (
    [stage_hash_commprefs_PartyCommunicationValues_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)       NOT NULL,
    [Id]                                               INT             NULL,
    [CreatedTime]                                      DATETIME        NULL,
    [CreatedBy]                                        NVARCHAR (4000) NULL,
    [DeletedTime]                                      DATETIME        NULL,
    [DeletedBy]                                        NVARCHAR (4000) NULL,
    [CommunicationValueId]                             INT             NULL,
    [PartyId]                                          INT             NULL,
    [jan_one]                                          DATETIME        NULL,
    [dv_load_date_time]                                DATETIME        NOT NULL,
    [dv_inserted_date_time]                            DATETIME        NOT NULL,
    [dv_insert_user]                                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                             DATETIME        NULL,
    [dv_update_user]                                   VARCHAR (50)    NULL,
    [dv_batch_id]                                      BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

