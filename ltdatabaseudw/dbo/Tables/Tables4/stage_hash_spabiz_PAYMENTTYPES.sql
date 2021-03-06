﻿CREATE TABLE [dbo].[stage_hash_spabiz_PAYMENTTYPES] (
    [stage_hash_spabiz_PAYMENTTYPES_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)       NOT NULL,
    [ID]                                DECIMAL (26, 6) NULL,
    [COUNTERID]                         DECIMAL (26, 6) NULL,
    [STOREID]                           DECIMAL (26, 6) NULL,
    [EDITTIME]                          DATETIME        NULL,
    [Delete]                            DECIMAL (26, 6) NULL,
    [DELETEDATE]                        DATETIME        NULL,
    [NAME]                              VARCHAR (150)   NULL,
    [QUICKID]                           VARCHAR (150)   NULL,
    [PAYTYPE]                           DECIMAL (26, 6) NULL,
    [ENABLED]                           DECIMAL (26, 6) NULL,
    [DEPOSITABLE]                       DECIMAL (26, 6) NULL,
    [SERVICECHARGE]                     DECIMAL (26, 6) NULL,
    [PROGRAMMED]                        DECIMAL (26, 6) NULL,
    [ICON]                              VARCHAR (150)   NULL,
    [ORDERNUM]                          DECIMAL (26, 6) NULL,
    [QUICKKEY]                          DECIMAL (26, 6) NULL,
    [NONREVENUE]                        DECIMAL (26, 6) NULL,
    [VERIFY]                            DECIMAL (26, 6) NULL,
    [DATETIME]                          DATETIME        NULL,
    [STORE_NUMBER]                      DECIMAL (26, 6) NULL,
    [GLACCOUNT]                         VARCHAR (60)    NULL,
    [POPDRAWER]                         DECIMAL (26, 6) NULL,
    [MULTICOPY]                         DECIMAL (26, 6) NULL,
    [SIGNATURELINE]                     DECIMAL (26, 6) NULL,
    [NEWID]                             DECIMAL (26, 6) NULL,
    [PAYMENTTYPESBACKUPID]              DECIMAL (26, 6) NULL,
    [DEFAULTROOMNUMBER]                 VARCHAR (150)   NULL,
    [HOTELPOST]                         DECIMAL (26, 6) NULL,
    [HOTELPAYCODE]                      VARCHAR (15)    NULL,
    [dv_load_date_time]                 DATETIME        NOT NULL,
    [dv_inserted_date_time]             DATETIME        NOT NULL,
    [dv_insert_user]                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]              DATETIME        NULL,
    [dv_update_user]                    VARCHAR (50)    NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

