﻿CREATE TABLE [dbo].[stage_hash_spabiz_BLUEPRINTDATA] (
    [stage_hash_spabiz_BLUEPRINTDATA_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)       NOT NULL,
    [ID]                                 DECIMAL (26, 6) NULL,
    [ANSWER]                             VARCHAR (3000)  NULL,
    [ANSWERTEXT]                         VARCHAR (3000)  NULL,
    [STORE_NUMBER]                       DECIMAL (26, 6) NULL,
    [COUNTERID]                          DECIMAL (26, 6) NULL,
    [STOREID]                            DECIMAL (26, 6) NULL,
    [EDITTIME]                           DATETIME        NULL,
    [dv_load_date_time]                  DATETIME        NOT NULL,
    [dv_inserted_date_time]              DATETIME        NOT NULL,
    [dv_insert_user]                     VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]               DATETIME        NULL,
    [dv_update_user]                     VARCHAR (50)    NULL,
    [dv_batch_id]                        BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

