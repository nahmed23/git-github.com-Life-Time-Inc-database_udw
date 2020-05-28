CREATE TABLE [dbo].[stage_spabiz_SERVICECHARGETAX] (
    [stage_spabiz_SERVICECHARGETAX_id] BIGINT          NOT NULL,
    [ID]                               DECIMAL (26, 6) NULL,
    [COUNTERID]                        DECIMAL (26, 6) NULL,
    [STOREID]                          DECIMAL (26, 6) NULL,
    [EDITTIME]                         DATETIME        NULL,
    [TAXID]                            DECIMAL (26, 6) NULL,
    [STORE_NUMBER]                     DECIMAL (26, 6) NULL,
    [SERVICECHARGEID]                  DECIMAL (26, 6) NULL,
    [dv_batch_id]                      BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

