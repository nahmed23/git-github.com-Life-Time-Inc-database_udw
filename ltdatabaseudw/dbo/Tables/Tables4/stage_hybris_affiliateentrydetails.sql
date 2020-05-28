CREATE TABLE [dbo].[stage_hybris_affiliateentrydetails] (
    [stage_hybris_affiliateentrydetails_id] BIGINT         NOT NULL,
    [hjmpTS]                                BIGINT         NULL,
    [TypePkString]                          BIGINT         NULL,
    [PK]                                    BIGINT         NULL,
    [createdTS]                             DATETIME       NULL,
    [modifiedTS]                            DATETIME       NULL,
    [OwnerPkString]                         BIGINT         NULL,
    [aCLTS]                                 INT            NULL,
    [propTS]                                INT            NULL,
    [ltfemployeeid]                         NVARCHAR (255) NULL,
    [ltfaffvalendtime]                      DATETIME       NULL,
    [ltfpartyid]                            NVARCHAR (255) NULL,
    [ltfaffiliateid]                        NVARCHAR (255) NULL,
    [ltfpurchaseflag]                       TINYINT        NULL,
    [ltfaffvalstarttime]                    DATETIME       NULL,
    [dv_batch_id]                           BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

