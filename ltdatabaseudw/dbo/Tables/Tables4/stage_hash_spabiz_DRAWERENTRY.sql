﻿CREATE TABLE [dbo].[stage_hash_spabiz_DRAWERENTRY] (
    [stage_hash_spabiz_DRAWERENTRY_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [ID]                               DECIMAL (26, 6) NULL,
    [COUNTERID]                        DECIMAL (26, 6) NULL,
    [STOREID]                          DECIMAL (26, 6) NULL,
    [EDITTIME]                         DATETIME        NULL,
    [STATUS]                           DECIMAL (26, 6) NULL,
    [SHIFTID]                          DECIMAL (26, 6) NULL,
    [NUM]                              VARCHAR (60)    NULL,
    [INAMOUNT]                         DECIMAL (26, 6) NULL,
    [INTYPE]                           DECIMAL (26, 6) NULL,
    [INOK]                             DECIMAL (26, 6) NULL,
    [OUTAMOUNT]                        DECIMAL (26, 6) NULL,
    [OUTTYPE]                          DECIMAL (26, 6) NULL,
    [OUTOK]                            DECIMAL (26, 6) NULL,
    [STAFFID]                          DECIMAL (26, 6) NULL,
    [PERIODID]                         DECIMAL (26, 6) NULL,
    [DAYID]                            DECIMAL (26, 6) NULL,
    [Date]                             DATETIME        NULL,
    [TIME]                             DATETIME        NULL,
    [PAYEEID]                          DECIMAL (26, 6) NULL,
    [PAYEETYPE]                        DECIMAL (26, 6) NULL,
    [PAYEEINDEX]                       VARCHAR (150)   NULL,
    [REASONID]                         DECIMAL (26, 6) NULL,
    [NOTE]                             VARCHAR (150)   NULL,
    [OK]                               DECIMAL (26, 6) NULL,
    [CHECKNUM]                         VARCHAR (60)    NULL,
    [DRAWERNUM]                        VARCHAR (150)   NULL,
    [STORE_NUMBER]                     DECIMAL (26, 6) NULL,
    [dv_load_date_time]                DATETIME        NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL,
    [dv_batch_id]                      BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

