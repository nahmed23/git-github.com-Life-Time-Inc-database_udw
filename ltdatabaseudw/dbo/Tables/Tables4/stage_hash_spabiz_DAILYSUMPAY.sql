﻿CREATE TABLE [dbo].[stage_hash_spabiz_DAILYSUMPAY] (
    [stage_hash_spabiz_DAILYSUMPAY_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [ID]                               DECIMAL (26, 6) NULL,
    [COUNTERID]                        DECIMAL (26, 6) NULL,
    [STOREID]                          DECIMAL (26, 6) NULL,
    [EDITTIME]                         DATETIME        NULL,
    [DAYID]                            DECIMAL (26, 6) NULL,
    [PAYID]                            DECIMAL (26, 6) NULL,
    [Date]                             DATETIME        NULL,
    [STARTAMOUNT]                      DECIMAL (26, 6) NULL,
    [TICKETNUM]                        DECIMAL (26, 6) NULL,
    [TICKETAMT]                        DECIMAL (26, 6) NULL,
    [CHANGEOUT]                        DECIMAL (26, 6) NULL,
    [DRAWERENTRIES]                    DECIMAL (26, 6) NULL,
    [YOUHAVE]                          DECIMAL (26, 6) NULL,
    [ERROR]                            DECIMAL (26, 6) NULL,
    [DEPOSIT]                          DECIMAL (26, 6) NULL,
    [TOTAL]                            DECIMAL (26, 6) NULL,
    [DAY_PAYINDEX]                     VARCHAR (150)   NULL,
    [STORE_NUMBER]                     DECIMAL (26, 6) NULL,
    [dv_load_date_time]                DATETIME        NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL,
    [dv_batch_id]                      BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

