﻿CREATE TABLE [sandbox_ebi].[stage_spabiz_customer] (
    [stage_spabiz_CUSTOMER_id] BIGINT          NOT NULL,
    [ID]                       DECIMAL (26, 6) NULL,
    [COUNTERID]                DECIMAL (26, 6) NULL,
    [STOREID]                  DECIMAL (26, 6) NULL,
    [EDITTIME]                 DATETIME        NULL,
    [Delete]                   DECIMAL (26, 6) NULL,
    [DELETEDATE]               DATETIME        NULL,
    [FIRSTNAME]                VARCHAR (150)   NULL,
    [LASTNAME]                 VARCHAR (150)   NULL,
    [FLNAME]                   VARCHAR (150)   NULL,
    [FNAME]                    VARCHAR (150)   NULL,
    [LNAME]                    VARCHAR (150)   NULL,
    [QUICKID]                  VARCHAR (150)   NULL,
    [ADDRESS1]                 VARCHAR (765)   NULL,
    [ADDRESS2]                 VARCHAR (360)   NULL,
    [CITY]                     VARCHAR (150)   NULL,
    [STATE]                    VARCHAR (30)    NULL,
    [ZIP]                      VARCHAR (30)    NULL,
    [COUNTRY]                  VARCHAR (150)   NULL,
    [TEL_HOME]                 VARCHAR (42)    NULL,
    [TEL_WORK]                 VARCHAR (42)    NULL,
    [TEL_WORKEXT]              VARCHAR (12)    NULL,
    [TEL_WORKFAX]              VARCHAR (42)    NULL,
    [TEL_MOBIL]                VARCHAR (42)    NULL,
    [TEL_PAGER]                VARCHAR (42)    NULL,
    [TEL_WHICH]                DECIMAL (26, 6) NULL,
    [EMAIL]                    VARCHAR (150)   NULL,
    [BDAY]                     VARCHAR (30)    NULL,
    [SEX]                      DECIMAL (26, 6) NULL,
    [PAYERID]                  DECIMAL (26, 6) NULL,
    [ACTIVESTATUS]             DECIMAL (26, 6) NULL,
    [CREATEDDATE]              DATETIME        NULL,
    [FIRSTVISIT]               DATETIME        NULL,
    [LASTAPDATE]               DATETIME        NULL,
    [LASTDATE]                 DATETIME        NULL,
    [TOTALVISITS]              DECIMAL (26, 6) NULL,
    [SERVICEVISITS]            DECIMAL (26, 6) NULL,
    [RETAINED]                 DECIMAL (26, 6) NULL,
    [DRIVERSLICENSE]           VARCHAR (150)   NULL,
    [REFERRALID]               DECIMAL (26, 6) NULL,
    [APPCONFIRM]               DECIMAL (26, 6) NULL,
    [APPCONFIRMONDAY]          DECIMAL (26, 6) NULL,
    [TOTALLATE]                DECIMAL (26, 6) NULL,
    [TOTALNOSHOW]              DECIMAL (26, 6) NULL,
    [BALANCE]                  DECIMAL (26, 6) NULL,
    [CREDITLIMIT]              DECIMAL (26, 6) NULL,
    [ALLERGIES]                VARCHAR (300)   NULL,
    [MEDICATION]               VARCHAR (300)   NULL,
    [OCCUPATION]               VARCHAR (150)   NULL,
    [EMPLOYER]                 VARCHAR (150)   NULL,
    [CALLDAYS]                 DECIMAL (26, 6) NULL,
    [NOTE]                     VARCHAR (4000)  NULL,
    [SHOWNOTE]                 DECIMAL (26, 6) NULL,
    [DONOTCHARGETAX]           DECIMAL (26, 6) NULL,
    [CHARGECOST]               DECIMAL (26, 6) NULL,
    [TAXNUM]                   VARCHAR (150)   NULL,
    [PRIMARYSTAFFID]           DECIMAL (26, 6) NULL,
    [MAILOK]                   DECIMAL (26, 6) NULL,
    [TOTALSERVICE]             DECIMAL (26, 6) NULL,
    [TOTALPRODUCT]             DECIMAL (26, 6) NULL,
    [YTDSERVICE]               DECIMAL (26, 6) NULL,
    [YTDPRODUCT]               DECIMAL (26, 6) NULL,
    [LASTCALLED]               DATETIME        NULL,
    [MARITAL]                  VARCHAR (150)   NULL,
    [ALTID]                    VARCHAR (75)    NULL,
    [RID]                      VARCHAR (18)    NULL,
    [USERNAME]                 VARCHAR (150)   NULL,
    [store_number]             DECIMAL (26, 6) NULL,
    [CUSTOMERID]               DECIMAL (26, 6) NULL,
    [FN]                       VARCHAR (150)   NULL,
    [LN]                       VARCHAR (150)   NULL,
    [ISURGENT]                 DECIMAL (26, 6) NULL,
    [DONOTPRINTNOTE]           DECIMAL (26, 6) NULL,
    [URGENT]                   DECIMAL (26, 6) NULL,
    [FOREIGNID]                VARCHAR (300)   NULL,
    [MEMBERID]                 VARCHAR (300)   NULL,
    [NOTE1]                    VARCHAR (1500)  NULL,
    [CUSTOMERVID]              VARCHAR (150)   NULL,
    [MEMBERVID]                VARCHAR (150)   NULL,
    [MEMBERSTATUS]             VARCHAR (36)    NULL,
    [MEMBERCATEGORY]           VARCHAR (3)     NULL,
    [STATUS]                   VARCHAR (6)     NULL,
    [APTHOLDINFO]              VARCHAR (1500)  NULL,
    [UDFIELD1]                 VARCHAR (150)   NULL,
    [UDFIELD2]                 VARCHAR (150)   NULL,
    [UDFIELD3]                 VARCHAR (150)   NULL,
    [UDFIELD4]                 VARCHAR (150)   NULL,
    [ACCOUNTNUMBER]            VARCHAR (300)   NULL,
    [EMAILOK]                  DECIMAL (26, 6) NULL,
    [MIDDLENAME]               VARCHAR (150)   NULL,
    [MEMBERSHIPID]             DECIMAL (26, 6) NULL,
    [MEMBERACTIVE]             DECIMAL (26, 6) NULL,
    [TEXTMSGOK]                DECIMAL (26, 6) NULL,
    [EMAIL_RECURRING]          DECIMAL (26, 6) NULL,
    [MERCURY1]                 VARCHAR (600)   NULL,
    [MERCURY2]                 VARCHAR (600)   NULL,
    [MERCURY3]                 VARCHAR (600)   NULL,
    [MERCURY4]                 VARCHAR (600)   NULL,
    [TITLE]                    VARCHAR (150)   NULL,
    [GENDERPREF]               DECIMAL (26, 6) NULL,
    [PARENTID]                 DECIMAL (26, 6) NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

