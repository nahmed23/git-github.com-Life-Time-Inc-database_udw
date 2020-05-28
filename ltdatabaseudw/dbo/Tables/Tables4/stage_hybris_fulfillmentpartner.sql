CREATE TABLE [dbo].[stage_hybris_fulfillmentpartner] (
    [stage_hybris_fulfillmentpartner_id] BIGINT         NOT NULL,
    [hjmpTS]                             BIGINT         NULL,
    [TypePkString]                       BIGINT         NULL,
    [PK]                                 BIGINT         NULL,
    [createdTS]                          DATETIME       NULL,
    [modifiedTS]                         DATETIME       NULL,
    [OwnerPkString]                      BIGINT         NULL,
    [aCLTS]                              INT            NULL,
    [propTS]                             INT            NULL,
    [p_displayname]                      NVARCHAR (255) NULL,
    [p_code]                             NVARCHAR (255) NULL,
    [p_ftpto]                            NVARCHAR (255) NULL,
    [p_exportfileformat]                 BIGINT         NULL,
    [p_ftpfrom]                          NVARCHAR (255) NULL,
    [p_importfileformat]                 BIGINT         NULL,
    [p_workdaysupplierid]                NVARCHAR (255) NULL,
    [p_inventoryto]                      NVARCHAR (255) NULL,
    [p_inventoryfileformat]              NVARCHAR (255) NULL,
    [p_receivercodeid]                   NVARCHAR (255) NULL,
    [p_receiverid]                       NVARCHAR (255) NULL,
    [p_senderqualifier]                  NVARCHAR (255) NULL,
    [p_receiverqualifier]                NVARCHAR (255) NULL,
    [p_senderid]                         NVARCHAR (255) NULL,
    [dv_batch_id]                        BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

