﻿CREATE TABLE [sandbox_ebi].[d_cogipf_runreport] (
    [COGIPF_REQUESTID]       NVARCHAR (255)  NOT NULL,
    [COGIPF_SUBREQUESTID]    NVARCHAR (255)  NULL,
    [COGIPF_SESSIONID]       VARCHAR (255)   NULL,
    [COGIPF_STEPID]          NVARCHAR (255)  NULL,
    [COGIPF_PROC_ID]         INT             NULL,
    [COGIPF_THREADID]        VARCHAR (255)   NULL,
    [COGIPF_LOCALTIMESTAMP]  DATETIME        NULL,
    [COGIPF_TIMEZONE_OFFSET] INT             NULL,
    [COGIPF_REPORTPATH]      NVARCHAR (512)  NULL,
    [COGIPF_REPORTNAME]      NVARCHAR (255)  NULL,
    [COGIPF_PACKAGE]         NVARCHAR (1024) NULL,
    [COGIPF_MODEL]           NVARCHAR (512)  NULL,
    [COGIPF_STATUS]          VARCHAR (255)   NULL,
    [COGIPF_RUNTIME]         INT             NULL,
    [COGIPF_TARGET_TYPE]     VARCHAR (255)   NULL,
    [COGIPF_ERRORDETAILS]    NVARCHAR (2000) NULL,
    [COGIPF_USERID]          VARCHAR (255)   NULL,
    [COGIPF_USERNAME]        NVARCHAR (255)  NULL,
    [dv_batch_id]            BIGINT          NULL,
    [dv_insert_date_time]    DATETIME        NULL,
    [dv_insert_uer]          VARCHAR (50)    NULL,
    [dv_update_date_time]    DATETIME        NULL,
    [dv_update_user]         VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

