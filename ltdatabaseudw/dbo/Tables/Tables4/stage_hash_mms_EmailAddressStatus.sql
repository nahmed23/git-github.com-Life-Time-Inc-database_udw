CREATE TABLE [dbo].[stage_hash_mms_EmailAddressStatus] (
    [stage_hash_mms_EmailAddressStatus_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)     NOT NULL,
    [EmailAddressStatusID]                 INT           NULL,
    [EmailAddress]                         VARCHAR (140) NULL,
    [StatusFromDate]                       DATETIME      NULL,
    [StatusThruDate]                       DATETIME      NULL,
    [InsertedDateTime]                     DATETIME      NULL,
    [UpdatedDateTime]                      DATETIME      NULL,
    [ValCommunicationPreferenceSourceID]   TINYINT       NULL,
    [ValCommunicationPreferenceStatusID]   TINYINT       NULL,
    [EmailAddressSearch]                   VARCHAR (10)  NULL,
    [dv_load_date_time]                    DATETIME      NOT NULL,
    [dv_inserted_date_time]                DATETIME      NOT NULL,
    [dv_insert_user]                       VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                 DATETIME      NULL,
    [dv_update_user]                       VARCHAR (50)  NULL,
    [dv_batch_id]                          BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

