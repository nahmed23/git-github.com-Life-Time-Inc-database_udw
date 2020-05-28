﻿CREATE TABLE [dbo].[stage_hash_spabiz_TAX] (
    [stage_hash_spabiz_TAX_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)       NOT NULL,
    [ID]                       DECIMAL (26, 6) NULL,
    [COUNTERID]                DECIMAL (26, 6) NULL,
    [STOREID]                  DECIMAL (26, 6) NULL,
    [EDITTIME]                 DATETIME        NULL,
    [Delete]                   DECIMAL (26, 6) NULL,
    [DELETEDATE]               DATETIME        NULL,
    [NAME]                     VARCHAR (150)   NULL,
    [QUICKID]                  VARCHAR (150)   NULL,
    [TAXAUTHNAME]              VARCHAR (150)   NULL,
    [DEPT]                     VARCHAR (150)   NULL,
    [ADDRESS1]                 VARCHAR (150)   NULL,
    [ADDRESS2]                 VARCHAR (150)   NULL,
    [CITY]                     VARCHAR (150)   NULL,
    [STATE]                    VARCHAR (150)   NULL,
    [ZIP]                      VARCHAR (150)   NULL,
    [PHONE]                    VARCHAR (150)   NULL,
    [CONTACT]                  VARCHAR (150)   NULL,
    [CONTACTTITLE]             VARCHAR (150)   NULL,
    [REPORTCYCLE]              DECIMAL (26, 6) NULL,
    [TAXTYPE]                  DECIMAL (26, 6) NULL,
    [AMOUNT]                   DECIMAL (26, 6) NULL,
    [NODEID]                   DECIMAL (26, 6) NULL,
    [STORE_NUMBER]             DECIMAL (26, 6) NULL,
    [dv_load_date_time]        DATETIME        NOT NULL,
    [dv_inserted_date_time]    DATETIME        NOT NULL,
    [dv_insert_user]           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]     DATETIME        NULL,
    [dv_update_user]           VARCHAR (50)    NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));
