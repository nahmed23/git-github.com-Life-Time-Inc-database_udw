CREATE TABLE [dbo].[stage_spabiz_DISCOUNTDATA] (
    [stage_spabiz_DISCOUNTDATA_id] BIGINT          NOT NULL,
    [ID]                           DECIMAL (26, 6) NULL,
    [COUNTERID]                    DECIMAL (26, 6) NULL,
    [STOREID]                      DECIMAL (26, 6) NULL,
    [EDITTIME]                     DATETIME        NULL,
    [DISCOUNTID]                   DECIMAL (26, 6) NULL,
    [ITEMID]                       DECIMAL (26, 6) NULL,
    [ITEMINDEX]                    VARCHAR (150)   NULL,
    [STORE_NUMBER]                 DECIMAL (26, 6) NULL,
    [dv_batch_id]                  BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

