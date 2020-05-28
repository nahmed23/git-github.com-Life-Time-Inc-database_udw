CREATE TABLE [dbo].[stage_spabiz_PARTYDATA] (
    [stage_spabiz_PARTYDATA_id] BIGINT          NOT NULL,
    [ID]                        DECIMAL (26, 6) NULL,
    [COUNTERID]                 DECIMAL (26, 6) NULL,
    [STOREID]                   DECIMAL (26, 6) NULL,
    [EDITTIME]                  DATETIME        NULL,
    [Delete]                    DECIMAL (26, 6) NULL,
    [PARTYID]                   DECIMAL (26, 6) NULL,
    [MASTERTICKETID]            DECIMAL (26, 6) NULL,
    [MASTERTICKETDATAID]        DECIMAL (26, 6) NULL,
    [CHILDTICKETID]             DECIMAL (26, 6) NULL,
    [CHILDTICKETDATAID]         DECIMAL (26, 6) NULL,
    [STORE_NUMBER]              DECIMAL (26, 6) NULL,
    [dv_batch_id]               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

