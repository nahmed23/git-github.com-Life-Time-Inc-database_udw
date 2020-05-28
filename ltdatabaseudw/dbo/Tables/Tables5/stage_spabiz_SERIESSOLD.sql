CREATE TABLE [dbo].[stage_spabiz_SERIESSOLD] (
    [stage_spabiz_SERIESSOLD_id] BIGINT          NOT NULL,
    [ID]                         DECIMAL (26, 6) NULL,
    [COUNTERID]                  DECIMAL (26, 6) NULL,
    [STOREID]                    DECIMAL (26, 6) NULL,
    [EDITTIME]                   DATETIME        NULL,
    [TICKETID]                   DECIMAL (26, 6) NULL,
    [SERIALNUM]                  VARCHAR (150)   NULL,
    [Date]                       DATETIME        NULL,
    [SERIESID]                   DECIMAL (26, 6) NULL,
    [STAFFIDCREATE]              DECIMAL (26, 6) NULL,
    [STAFFID1]                   DECIMAL (26, 6) NULL,
    [STAFFID2]                   DECIMAL (26, 6) NULL,
    [BUY_CUSTID]                 DECIMAL (26, 6) NULL,
    [CUSTID]                     DECIMAL (26, 6) NULL,
    [STATUS]                     DECIMAL (26, 6) NULL,
    [LASTUSED]                   DATETIME        NULL,
    [RETAILPRICE]                DECIMAL (26, 6) NULL,
    [BALANCE]                    DECIMAL (26, 6) NULL,
    [TICKETNUM]                  VARCHAR (150)   NULL,
    [STORE_NUMBER]               DECIMAL (26, 6) NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

