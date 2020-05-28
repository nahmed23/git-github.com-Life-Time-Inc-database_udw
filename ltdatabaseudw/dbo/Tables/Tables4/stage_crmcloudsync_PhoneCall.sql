﻿CREATE TABLE [dbo].[stage_crmcloudsync_PhoneCall] (
    [stage_crmcloudsync_PhoneCall_id] BIGINT          NOT NULL,
    [activityid]                      VARCHAR (36)    NULL,
    [activitytypecode]                NVARCHAR (64)   NULL,
    [activitytypecodename]            NVARCHAR (255)  NULL,
    [actualdurationminutes]           INT             NULL,
    [actualend]                       DATETIME        NULL,
    [actualstart]                     DATETIME        NULL,
    [category]                        NVARCHAR (250)  NULL,
    [createdby]                       VARCHAR (36)    NULL,
    [createdbyname]                   NVARCHAR (200)  NULL,
    [createdbyyominame]               NVARCHAR (200)  NULL,
    [createdon]                       DATETIME        NULL,
    [createdonbehalfby]               VARCHAR (36)    NULL,
    [createdonbehalfbyname]           NVARCHAR (200)  NULL,
    [createdonbehalfbyyominame]       NVARCHAR (200)  NULL,
    [description]                     VARCHAR (8000)  NULL,
    [directioncode]                   BIT             NULL,
    [directioncodename]               NVARCHAR (255)  NULL,
    [exchangerate]                    DECIMAL (28)    NULL,
    [from]                            VARCHAR (8000)  NULL,
    [importsequencenumber]            INT             NULL,
    [isbilled]                        BIT             NULL,
    [isbilledname]                    NVARCHAR (255)  NULL,
    [isregularactivity]               BIT             NULL,
    [isregularactivityname]           NVARCHAR (255)  NULL,
    [isworkflowcreated]               BIT             NULL,
    [isworkflowcreatedname]           NVARCHAR (255)  NULL,
    [leftvoicemail]                   BIT             NULL,
    [leftvoicemailname]               NVARCHAR (255)  NULL,
    [ltf_wrapupcode]                  INT             NULL,
    [ltf_wrapupcodename]              NVARCHAR (255)  NULL,
    [modifiedby]                      VARCHAR (36)    NULL,
    [modifiedbyname]                  NVARCHAR (200)  NULL,
    [modifiedbyyominame]              NVARCHAR (200)  NULL,
    [modifiedon]                      DATETIME        NULL,
    [modifiedonbehalfby]              VARCHAR (36)    NULL,
    [modifiedonbehalfbyname]          NVARCHAR (200)  NULL,
    [modifiedonbehalfbyyominame]      NVARCHAR (200)  NULL,
    [new_callid]                      NVARCHAR (72)   NULL,
    [overriddencreatedon]             DATETIME        NULL,
    [ownerid]                         VARCHAR (36)    NULL,
    [owneridname]                     NVARCHAR (200)  NULL,
    [owneridtype]                     NVARCHAR (64)   NULL,
    [owneridyominame]                 NVARCHAR (200)  NULL,
    [owningbusinessunit]              VARCHAR (36)    NULL,
    [owningteam]                      VARCHAR (36)    NULL,
    [owninguser]                      VARCHAR (36)    NULL,
    [phonenumber]                     NVARCHAR (200)  NULL,
    [prioritycode]                    INT             NULL,
    [prioritycodename]                NVARCHAR (255)  NULL,
    [processid]                       VARCHAR (36)    NULL,
    [regardingobjectid]               VARCHAR (36)    NULL,
    [regardingobjectidname]           NVARCHAR (4000) NULL,
    [regardingobjectidyominame]       NVARCHAR (4000) NULL,
    [regardingobjecttypecode]         NVARCHAR (64)   NULL,
    [scheduleddurationminutes]        INT             NULL,
    [scheduledend]                    DATETIME        NULL,
    [scheduledstart]                  DATETIME        NULL,
    [serviceid]                       VARCHAR (36)    NULL,
    [stageid]                         VARCHAR (36)    NULL,
    [statecode]                       INT             NULL,
    [statecodename]                   NVARCHAR (255)  NULL,
    [statuscode]                      INT             NULL,
    [statuscodename]                  NVARCHAR (255)  NULL,
    [subcategory]                     NVARCHAR (250)  NULL,
    [subject]                         NVARCHAR (200)  NULL,
    [timezoneruleversionnumber]       INT             NULL,
    [to]                              VARCHAR (8000)  NULL,
    [transactioncurrencyid]           VARCHAR (36)    NULL,
    [transactioncurrencyidname]       NVARCHAR (100)  NULL,
    [utcconversiontimezonecode]       INT             NULL,
    [versionnumber]                   BIGINT          NULL,
    [InsertedDateTime]                DATETIME        NULL,
    [InsertUser]                      VARCHAR (50)    NULL,
    [UpdatedDateTime]                 DATETIME        NULL,
    [UpdateUser]                      VARCHAR (50)    NULL,
    [ltf_program]                     INT             NULL,
    [ltf_programname]                 NVARCHAR (255)  NULL,
    [ltf_callername]                  NVARCHAR (100)  NULL,
    [ltf_callsubtype]                 INT             NULL,
    [ltf_callsubtypename]             NVARCHAR (255)  NULL,
    [ltf_calltype]                    INT             NULL,
    [ltf_calltypename]                NVARCHAR (255)  NULL,
    [ltf_club]                        VARCHAR (36)    NULL,
    [ltf_clubid]                      VARCHAR (36)    NULL,
    [ltf_clubidname]                  NVARCHAR (100)  NULL,
    [ltf_clubname]                    NVARCHAR (100)  NULL,
    [activityadditionalparams]        VARCHAR (8000)  NULL,
    [traversedpath]                   NVARCHAR (1250) NULL,
    [ltf_mostrecentcasl]              DATETIME        NULL,
    [dv_batch_id]                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

