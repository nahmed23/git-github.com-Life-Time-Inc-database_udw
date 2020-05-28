CREATE TABLE [dbo].[stage_spabiz_SERVICECHARGE] (
    [stage_spabiz_SERVICECHARGE_id] BIGINT          NOT NULL,
    [ID]                            DECIMAL (26, 6) NULL,
    [COUNTERID]                     DECIMAL (26, 6) NULL,
    [STOREID]                       DECIMAL (26, 6) NULL,
    [EDITTIME]                      DATETIME        NULL,
    [Delete]                        DECIMAL (26, 6) NULL,
    [DELETEDATE]                    DATETIME        NULL,
    [NAME]                          VARCHAR (300)   NULL,
    [QUICKID]                       VARCHAR (150)   NULL,
    [DISPLAYNAME]                   VARCHAR (150)   NULL,
    [STORE_NUMBER]                  DECIMAL (26, 6) NULL,
    [PAYCOMMISSION]                 DECIMAL (26, 6) NULL,
    [ENABLED]                       DECIMAL (26, 6) NULL,
    [ENABLEDTEXT]                   VARCHAR (30)    NULL,
    [TAXABLE]                       DECIMAL (26, 6) NULL,
    [AMOUNT]                        DECIMAL (26, 6) NULL,
    [DOLLARPERCENT]                 DECIMAL (26, 6) NULL,
    [STAFFID]                       DECIMAL (26, 6) NULL,
    [GLACCT]                        VARCHAR (150)   NULL,
    [COMPUTEDON]                    DECIMAL (26, 6) NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

