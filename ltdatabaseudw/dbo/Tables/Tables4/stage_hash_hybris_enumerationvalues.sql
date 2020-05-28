CREATE TABLE [dbo].[stage_hash_hybris_enumerationvalues] (
    [stage_hash_hybris_enumerationvalues_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)      NOT NULL,
    [hjmpTS]                                 BIGINT         NULL,
    [createdTS]                              DATETIME       NULL,
    [modifiedTS]                             DATETIME       NULL,
    [TypePkString]                           BIGINT         NULL,
    [OwnerPkString]                          BIGINT         NULL,
    [PK]                                     BIGINT         NULL,
    [Code]                                   NVARCHAR (255) NULL,
    [codeLowerCase]                          NVARCHAR (255) NULL,
    [SequenceNumber]                         INT            NULL,
    [p_extensionname]                        NVARCHAR (255) NULL,
    [p_icon]                                 BIGINT         NULL,
    [aCLTS]                                  BIGINT         NULL,
    [propTS]                                 BIGINT         NULL,
    [Editable]                               TINYINT        NULL,
    [dv_load_date_time]                      DATETIME       NOT NULL,
    [dv_inserted_date_time]                  DATETIME       NOT NULL,
    [dv_insert_user]                         VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                   DATETIME       NULL,
    [dv_update_user]                         VARCHAR (50)   NULL,
    [dv_batch_id]                            BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

