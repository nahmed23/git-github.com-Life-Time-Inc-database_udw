CREATE TABLE [dbo].[stage_hash_hybris_catalogs4basestores] (
    [stage_hash_hybris_catalogs4basestores_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)      NOT NULL,
    [hjmpTS]                                   BIGINT         NULL,
    [createdTS]                                DATETIME       NULL,
    [modifiedTS]                               DATETIME       NULL,
    [TypePkString]                             BIGINT         NULL,
    [OwnerPkString]                            BIGINT         NULL,
    [PK]                                       BIGINT         NULL,
    [languagepk]                               BIGINT         NULL,
    [Qualifier]                                NVARCHAR (255) NULL,
    [SourcePK]                                 BIGINT         NULL,
    [TargetPK]                                 BIGINT         NULL,
    [SequenceNumber]                           INT            NULL,
    [RSequenceNumber]                          INT            NULL,
    [aCLTS]                                    BIGINT         NULL,
    [propTS]                                   BIGINT         NULL,
    [dv_load_date_time]                        DATETIME       NOT NULL,
    [dv_inserted_date_time]                    DATETIME       NOT NULL,
    [dv_insert_user]                           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                     DATETIME       NULL,
    [dv_update_user]                           VARCHAR (50)   NULL,
    [dv_batch_id]                              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

