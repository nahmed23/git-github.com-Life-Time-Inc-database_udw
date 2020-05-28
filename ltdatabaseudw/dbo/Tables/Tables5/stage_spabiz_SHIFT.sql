CREATE TABLE [dbo].[stage_spabiz_SHIFT] (
    [stage_spabiz_SHIFT_id] BIGINT          NOT NULL,
    [ID]                    DECIMAL (26, 6) NULL,
    [COUNTERID]             DECIMAL (26, 6) NULL,
    [STOREID]               DECIMAL (26, 6) NULL,
    [EDITTIME]              DATETIME        NULL,
    [OPENSTAFFID]           DECIMAL (26, 6) NULL,
    [CLOSESTAFFID]          DECIMAL (26, 6) NULL,
    [Date]                  DATETIME        NULL,
    [DAYID]                 DECIMAL (26, 6) NULL,
    [PERIODID]              DECIMAL (26, 6) NULL,
    [TIMEOPEN]              DATETIME        NULL,
    [TIMECLOSE]             DATETIME        NULL,
    [TIMEREC]               DATETIME        NULL,
    [STATUS]                DECIMAL (26, 6) NULL,
    [ERRORNOTE]             VARCHAR (3000)  NULL,
    [DRAWERID]              DECIMAL (26, 6) NULL,
    [VOIDERID]              DECIMAL (26, 6) NULL,
    [STORE_NUMBER]          DECIMAL (26, 6) NULL,
    [AMOUNTINDRAWER]        DECIMAL (26, 6) NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

