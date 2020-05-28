CREATE TABLE [dbo].[stage_spabiz_GIFTRECHARGE] (
    [stage_spabiz_GIFTRECHARGE_id] BIGINT          NOT NULL,
    [STORE_NUMBER]                 DECIMAL (26, 6) NULL,
    [EDITTIME]                     DATETIME        NULL,
    [ID]                           DECIMAL (26, 6) NULL,
    [GIFTID]                       DECIMAL (26, 6) NULL,
    [TICKETDATAID]                 DECIMAL (26, 6) NULL,
    [STOREID]                      DECIMAL (26, 6) NULL,
    [TICKETID]                     DECIMAL (26, 6) NULL,
    [AMOUNT]                       DECIMAL (26, 6) NULL,
    [EXPDATE]                      DATETIME        NULL,
    [COUNTERID]                    DECIMAL (26, 6) NULL,
    [dv_batch_id]                  BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

