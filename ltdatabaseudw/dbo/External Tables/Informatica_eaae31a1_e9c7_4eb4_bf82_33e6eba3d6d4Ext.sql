﻿CREATE EXTERNAL TABLE [dbo].[Informatica_eaae31a1_e9c7_4eb4_bf82_33e6eba3d6d4Ext] (
    [stage_spabiz_STAFF_id] BIGINT NULL,
    [ID] DECIMAL (26, 6) NULL,
    [COUNTERID] DECIMAL (26, 6) NULL,
    [STOREID] DECIMAL (26, 6) NULL,
    [EDITTIME] DATETIME NULL,
    [Delete] DECIMAL (26, 6) NULL,
    [DELETEDATE] DATETIME NULL,
    [FIRSTNAME] VARCHAR (150) NULL,
    [MI] VARCHAR (3) NULL,
    [LASTNAME] VARCHAR (150) NULL,
    [FLNAME] VARCHAR (150) NULL,
    [FNAME] VARCHAR (150) NULL,
    [QUICKID] VARCHAR (60) NULL,
    [BOOKNAME] VARCHAR (150) NULL,
    [ADDRESS1] VARCHAR (765) NULL,
    [ADDRESS2] VARCHAR (90) NULL,
    [CITY] VARCHAR (60) NULL,
    [STATE] VARCHAR (6) NULL,
    [ZIP] VARCHAR (30) NULL,
    [TEL_HOME] VARCHAR (60) NULL,
    [TEL_WORK] VARCHAR (60) NULL,
    [TEL_MOBIL] VARCHAR (60) NULL,
    [TEL_PAGER] VARCHAR (60) NULL,
    [BIRTHDAY] DATETIME NULL,
    [SEX] VARCHAR (150) NULL,
    [EMPLOYER] VARCHAR (150) NULL,
    [SSN] VARCHAR (150) NULL,
    [EMPSTARTDATE] DATETIME NULL,
    [EMPENDDATE] DATETIME NULL,
    [CANUSESYSTEM] DECIMAL (26, 6) NULL,
    [PASSWORD] VARCHAR (150) NULL,
    [BALANCE] DECIMAL (26, 6) NULL,
    [SERVICECOMMISHID] DECIMAL (26, 6) NULL,
    [ASSCOMMISHID] DECIMAL (26, 6) NULL,
    [PRODUCTCOMMISHID] DECIMAL (26, 6) NULL,
    [PRINTTRAVELER] DECIMAL (26, 6) NULL,
    [POPUPINFO] VARCHAR (3000) NULL,
    [NOTE] VARCHAR (765) NULL,
    [PRINT1] DECIMAL (26, 6) NULL,
    [PRINT2] DECIMAL (26, 6) NULL,
    [PRINT3] DECIMAL (26, 6) NULL,
    [PRINT4] DECIMAL (26, 6) NULL,
    [PRINT5] DECIMAL (26, 6) NULL,
    [PRINT6] DECIMAL (26, 6) NULL,
    [PRINT7] DECIMAL (26, 6) NULL,
    [STARTAPCYCLE] DATETIME NULL,
    [APCYCLECOUNT] DECIMAL (26, 6) NULL,
    [DEPTCAT] DECIMAL (26, 6) NULL,
    [SEARCHCAT] DECIMAL (26, 6) NULL,
    [STATUS] DECIMAL (26, 6) NULL,
    [BDAY] VARCHAR (30) NULL,
    [ANNIVERSARY] VARCHAR (30) NULL,
    [CLOCKINREQ] DECIMAL (26, 6) NULL,
    [WAGETYPE] DECIMAL (26, 6) NULL,
    [WAGE] DECIMAL (26, 6) NULL,
    [WAGEPERMIN] DECIMAL (26, 6) NULL,
    [TGLEVEL] DECIMAL (26, 6) NULL,
    [PRINTPOPUP] DECIMAL (26, 6) NULL,
    [BIO] VARCHAR (765) NULL,
    [TYPEOF] DECIMAL (26, 6) NULL,
    [NAME] VARCHAR (150) NULL,
    [PAGERNUM] DECIMAL (26, 6) NULL,
    [PAGERTYPE] DECIMAL (26, 6) NULL,
    [SALES_TOTAL] DECIMAL (26, 6) NULL,
    [STORE_NUMBER] DECIMAL (26, 6) NULL,
    [SERVICETEMPLATEID] DECIMAL (26, 6) NULL,
    [STAFFTEMPLATEID] DECIMAL (26, 6) NULL,
    [WEBBOOK] DECIMAL (26, 6) NULL,
    [NEILLID] VARCHAR (75) NULL,
    [SCATID] DECIMAL (26, 6) NULL,
    [LOUIS] DECIMAL (26, 6) NULL,
    [DONOTPRINTPROD] DECIMAL (26, 6) NULL,
    [PRINTTRAVLER] DECIMAL (26, 6) NULL,
    [FOREIGNID] VARCHAR (300) NULL,
    [NEWID] DECIMAL (26, 6) NULL,
    [STAFFBACKUPID] DECIMAL (26, 6) NULL,
    [PRIMARYLOCATION] DECIMAL (26, 6) NULL,
    [LEVELID] DECIMAL (26, 6) NULL,
    [USERNAME] VARCHAR (60) NULL,
    [ISADMIN] DECIMAL (26, 6) NULL,
    [HEADMAPSTAFF] DECIMAL (26, 6) NULL,
    [SERVICECOMMISSIONTYPEID] DECIMAL (26, 6) NULL,
    [ALLOWPOWERBOOKING] DECIMAL (26, 6) NULL,
    [EMAIL] VARCHAR (750) NULL,
    [ENCPWORD] VARCHAR (300) NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_eaae31a1_e9c7_4eb4_bf82_33e6eba3d6d4DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_eaae31a1_e9c7_4eb4_bf82_33e6eba3d6d4',
    FILE_FORMAT = [Informatica_eaae31a1_e9c7_4eb4_bf82_33e6eba3d6d4FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

