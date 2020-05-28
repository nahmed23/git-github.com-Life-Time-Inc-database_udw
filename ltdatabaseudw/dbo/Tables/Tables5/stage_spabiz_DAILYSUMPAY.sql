CREATE TABLE [dbo].[stage_spabiz_DAILYSUMPAY] (
    [stage_spabiz_DAILYSUMPAY_id] BIGINT          NOT NULL,
    [ID]                          DECIMAL (26, 6) NULL,
    [COUNTERID]                   DECIMAL (26, 6) NULL,
    [STOREID]                     DECIMAL (26, 6) NULL,
    [EDITTIME]                    DATETIME        NULL,
    [DAYID]                       DECIMAL (26, 6) NULL,
    [PAYID]                       DECIMAL (26, 6) NULL,
    [Date]                        DATETIME        NULL,
    [STARTAMOUNT]                 DECIMAL (26, 6) NULL,
    [TICKETNUM]                   DECIMAL (26, 6) NULL,
    [TICKETAMT]                   DECIMAL (26, 6) NULL,
    [CHANGEOUT]                   DECIMAL (26, 6) NULL,
    [DRAWERENTRIES]               DECIMAL (26, 6) NULL,
    [YOUHAVE]                     DECIMAL (26, 6) NULL,
    [ERROR]                       DECIMAL (26, 6) NULL,
    [DEPOSIT]                     DECIMAL (26, 6) NULL,
    [TOTAL]                       DECIMAL (26, 6) NULL,
    [DAY_PAYINDEX]                VARCHAR (150)   NULL,
    [STORE_NUMBER]                DECIMAL (26, 6) NULL,
    [dv_batch_id]                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

