﻿CREATE TABLE [dbo].[stage_spabiz_TICKETTAX] (
    [stage_spabiz_TICKETTAX_id] BIGINT          NOT NULL,
    [ID]                        DECIMAL (26, 6) NULL,
    [COUNTERID]                 DECIMAL (26, 6) NULL,
    [STOREID]                   DECIMAL (26, 6) NULL,
    [EDITTIME]                  DATETIME        NULL,
    [TICKETID]                  DECIMAL (26, 6) NULL,
    [TAXID]                     DECIMAL (26, 6) NULL,
    [AMOUNT]                    DECIMAL (26, 6) NULL,
    [Date]                      DATETIME        NULL,
    [STATUS]                    DECIMAL (26, 6) NULL,
    [SHIFTID]                   DECIMAL (26, 6) NULL,
    [DAYID]                     DECIMAL (26, 6) NULL,
    [PERIODID]                  DECIMAL (26, 6) NULL,
    [COST]                      DECIMAL (26, 6) NULL,
    [STORE_NUMBER]              DECIMAL (26, 6) NULL,
    [GLACCOUNT]                 VARCHAR (45)    NULL,
    [dv_batch_id]               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

