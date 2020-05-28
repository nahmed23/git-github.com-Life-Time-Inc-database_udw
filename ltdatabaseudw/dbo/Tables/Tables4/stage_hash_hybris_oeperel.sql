CREATE TABLE [dbo].[stage_hash_hybris_oeperel] (
    [stage_hash_hybris_oeperel_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [hjmpTS]                       BIGINT         NULL,
    [TypePkString]                 BIGINT         NULL,
    [OwnerPkString]                BIGINT         NULL,
    [modifiedTS]                   DATETIME       NULL,
    [createdTS]                    DATETIME       NULL,
    [PK]                           BIGINT         NULL,
    [RSequenceNumber]              INT            NULL,
    [TargetPK]                     BIGINT         NULL,
    [SequenceNumber]               INT            NULL,
    [SourcePK]                     BIGINT         NULL,
    [Qualifier]                    NVARCHAR (255) NULL,
    [languagepk]                   BIGINT         NULL,
    [aCLTS]                        BIGINT         NULL,
    [propTS]                       BIGINT         NULL,
    [dv_load_date_time]            DATETIME       NOT NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

