CREATE TABLE [dbo].[stage_spabiz_BLUEPRINTDATA] (
    [stage_spabiz_BLUEPRINTDATA_id] BIGINT          NOT NULL,
    [ID]                            DECIMAL (26, 6) NULL,
    [ANSWER]                        VARCHAR (3000)  NULL,
    [ANSWERTEXT]                    VARCHAR (3000)  NULL,
    [STORE_NUMBER]                  DECIMAL (26, 6) NULL,
    [COUNTERID]                     DECIMAL (26, 6) NULL,
    [STOREID]                       DECIMAL (26, 6) NULL,
    [EDITTIME]                      DATETIME        NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

