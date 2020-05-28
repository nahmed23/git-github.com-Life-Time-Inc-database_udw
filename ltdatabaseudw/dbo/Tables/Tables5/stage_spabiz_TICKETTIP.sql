CREATE TABLE [dbo].[stage_spabiz_TICKETTIP] (
    [stage_spabiz_TICKETTIP_id] BIGINT          NOT NULL,
    [ID]                        DECIMAL (26, 6) NULL,
    [COUNTERID]                 DECIMAL (26, 6) NULL,
    [STOREID]                   DECIMAL (26, 6) NULL,
    [EDITTIME]                  DATETIME        NULL,
    [TICKETID]                  DECIMAL (26, 6) NULL,
    [TICKETNUM]                 VARCHAR (150)   NULL,
    [Date]                      DATETIME        NULL,
    [CUSTID]                    DECIMAL (26, 6) NULL,
    [STATUS]                    DECIMAL (26, 6) NULL,
    [SHIFTID]                   DECIMAL (26, 6) NULL,
    [AMOUNT]                    DECIMAL (26, 6) NULL,
    [SPLIT]                     DECIMAL (26, 6) NULL,
    [STAFFID]                   DECIMAL (26, 6) NULL,
    [PAID]                      DECIMAL (26, 6) NULL,
    [LAYERID]                   DECIMAL (26, 6) NULL,
    [STORE_NUMBER]              DECIMAL (26, 6) NULL,
    [GLACCOUNT]                 VARCHAR (45)    NULL,
    [CREDITCARD]                DECIMAL (26, 6) NULL,
    [PAIDDRAWERID]              DECIMAL (26, 6) NULL,
    [ISAUTOGRATUITY]            DECIMAL (26, 6) NULL,
    [dv_batch_id]               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

