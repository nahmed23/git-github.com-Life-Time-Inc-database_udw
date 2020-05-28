CREATE TABLE [dbo].[stage_spabiz_TAX] (
    [stage_spabiz_TAX_id] BIGINT          NOT NULL,
    [ID]                  DECIMAL (26, 6) NULL,
    [COUNTERID]           DECIMAL (26, 6) NULL,
    [STOREID]             DECIMAL (26, 6) NULL,
    [EDITTIME]            DATETIME        NULL,
    [Delete]              DECIMAL (26, 6) NULL,
    [DELETEDATE]          DATETIME        NULL,
    [NAME]                VARCHAR (150)   NULL,
    [QUICKID]             VARCHAR (150)   NULL,
    [TAXAUTHNAME]         VARCHAR (150)   NULL,
    [DEPT]                VARCHAR (150)   NULL,
    [ADDRESS1]            VARCHAR (150)   NULL,
    [ADDRESS2]            VARCHAR (150)   NULL,
    [CITY]                VARCHAR (150)   NULL,
    [STATE]               VARCHAR (150)   NULL,
    [ZIP]                 VARCHAR (150)   NULL,
    [PHONE]               VARCHAR (150)   NULL,
    [CONTACT]             VARCHAR (150)   NULL,
    [CONTACTTITLE]        VARCHAR (150)   NULL,
    [REPORTCYCLE]         DECIMAL (26, 6) NULL,
    [TAXTYPE]             DECIMAL (26, 6) NULL,
    [AMOUNT]              DECIMAL (26, 6) NULL,
    [NODEID]              DECIMAL (26, 6) NULL,
    [STORE_NUMBER]        DECIMAL (26, 6) NULL,
    [dv_batch_id]         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

