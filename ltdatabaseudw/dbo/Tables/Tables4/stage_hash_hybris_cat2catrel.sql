CREATE TABLE [dbo].[stage_hash_hybris_cat2catrel] (
    [stage_hash_hybris_cat2catrel_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)      NOT NULL,
    [hjmpTS]                          BIGINT         NULL,
    [TypePkString]                    BIGINT         NULL,
    [PK]                              BIGINT         NULL,
    [createdTS]                       DATETIME       NULL,
    [modifiedTS]                      DATETIME       NULL,
    [OwnerPkString]                   BIGINT         NULL,
    [aCLTS]                           INT            NULL,
    [propTS]                          INT            NULL,
    [Qualifier]                       NVARCHAR (255) NULL,
    [SourcePK]                        BIGINT         NULL,
    [TargetPK]                        BIGINT         NULL,
    [RSequenceNumber]                 INT            NULL,
    [SequenceNumber]                  INT            NULL,
    [languagepk]                      BIGINT         NULL,
    [dv_load_date_time]               DATETIME       NOT NULL,
    [dv_inserted_date_time]           DATETIME       NOT NULL,
    [dv_insert_user]                  VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]            DATETIME       NULL,
    [dv_update_user]                  VARCHAR (50)   NULL,
    [dv_batch_id]                     BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

