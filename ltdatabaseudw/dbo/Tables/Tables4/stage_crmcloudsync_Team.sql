﻿CREATE TABLE [dbo].[stage_crmcloudsync_Team] (
    [stage_crmcloudsync_Team_id] BIGINT          NOT NULL,
    [administratorid]            VARCHAR (36)    NULL,
    [administratoridname]        NVARCHAR (200)  NULL,
    [administratoridyominame]    NVARCHAR (200)  NULL,
    [businessunitid]             VARCHAR (36)    NULL,
    [businessunitidname]         NVARCHAR (160)  NULL,
    [createdby]                  VARCHAR (36)    NULL,
    [createdbyname]              NVARCHAR (200)  NULL,
    [createdbyyominame]          NVARCHAR (200)  NULL,
    [createdon]                  DATETIME        NULL,
    [createdonbehalfby]          VARCHAR (36)    NULL,
    [createdonbehalfbyname]      NVARCHAR (200)  NULL,
    [createdonbehalfbyyominame]  NVARCHAR (200)  NULL,
    [description]                NVARCHAR (4000) NULL,
    [emailaddress]               NVARCHAR (100)  NULL,
    [exchangerate]               DECIMAL (28)    NULL,
    [importsequencenumber]       INT             NULL,
    [isdefault]                  BIT             NULL,
    [isdefaultname]              NVARCHAR (255)  NULL,
    [ltf_telephone1]             NVARCHAR (15)   NULL,
    [modifiedby]                 VARCHAR (36)    NULL,
    [modifiedbyname]             NVARCHAR (200)  NULL,
    [modifiedbyyominame]         NVARCHAR (200)  NULL,
    [modifiedon]                 DATETIME        NULL,
    [modifiedonbehalfby]         VARCHAR (36)    NULL,
    [modifiedonbehalfbyname]     NVARCHAR (200)  NULL,
    [modifiedonbehalfbyyominame] NVARCHAR (200)  NULL,
    [name]                       NVARCHAR (160)  NULL,
    [organizationid]             VARCHAR (36)    NULL,
    [organizationidname]         NVARCHAR (100)  NULL,
    [overriddencreatedon]        DATETIME        NULL,
    [processid]                  VARCHAR (36)    NULL,
    [queueid]                    VARCHAR (36)    NULL,
    [queueidname]                NVARCHAR (400)  NULL,
    [regardingobjectid]          VARCHAR (36)    NULL,
    [regardingobjecttypecode]    NVARCHAR (64)   NULL,
    [stageid]                    VARCHAR (36)    NULL,
    [systemmanaged]              BIT             NULL,
    [systemmanagedname]          NVARCHAR (255)  NULL,
    [teamid]                     VARCHAR (36)    NULL,
    [teamtemplateid]             VARCHAR (36)    NULL,
    [teamtype]                   INT             NULL,
    [teamtypename]               NVARCHAR (255)  NULL,
    [transactioncurrencyid]      VARCHAR (36)    NULL,
    [transactioncurrencyidname]  NVARCHAR (100)  NULL,
    [versionnumber]              BIGINT          NULL,
    [yominame]                   NVARCHAR (160)  NULL,
    [InsertedDateTime]           DATETIME        NULL,
    [InsertUser]                 VARCHAR (100)   NULL,
    [UpdatedDateTime]            DATETIME        NULL,
    [UpdateUser]                 VARCHAR (50)    NULL,
    [ltf_teamtype]               INT             NULL,
    [ltf_teamtypename]           NVARCHAR (255)  NULL,
    [traversedpath]              NVARCHAR (1250) NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
