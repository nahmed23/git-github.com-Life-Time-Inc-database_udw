﻿CREATE TABLE [dbo].[stage_crmcloudsync_ltf_campaigninstance] (
    [stage_crmcloudsync_ltf_campaigninstance_id] BIGINT         NOT NULL,
    [createdby]                                  VARCHAR (36)   NULL,
    [createdbyname]                              NVARCHAR (200) NULL,
    [createdbyyominame]                          NVARCHAR (200) NULL,
    [createdon]                                  DATETIME       NULL,
    [createdonbehalfby]                          VARCHAR (36)   NULL,
    [createdonbehalfbyname]                      NVARCHAR (200) NULL,
    [createdonbehalfbyyominame]                  NVARCHAR (200) NULL,
    [importsequencenumber]                       INT            NULL,
    [ltf_campaign]                               VARCHAR (36)   NULL,
    [ltf_campaigninstanceid]                     VARCHAR (36)   NULL,
    [ltf_campaignname]                           NVARCHAR (128) NULL,
    [ltf_club]                                   VARCHAR (36)   NULL,
    [ltf_clubname]                               NVARCHAR (100) NULL,
    [ltf_connectwitham]                          BIT            NULL,
    [ltf_connectwithamname]                      NVARCHAR (255) NULL,
    [ltf_expirationdate]                         DATETIME       NULL,
    [ltf_initialusedate]                         DATETIME       NULL,
    [ltf_issuedby]                               VARCHAR (36)   NULL,
    [ltf_issuedbyname]                           NVARCHAR (200) NULL,
    [ltf_issuedbyyominame]                       NVARCHAR (200) NULL,
    [ltf_issueddate]                             DATETIME       NULL,
    [ltf_issueddays]                             INT            NULL,
    [ltf_issuingcontact]                         VARCHAR (36)   NULL,
    [ltf_issuingcontactname]                     NVARCHAR (160) NULL,
    [ltf_issuingcontactyominame]                 NVARCHAR (160) NULL,
    [ltf_issuinglead]                            VARCHAR (36)   NULL,
    [ltf_issuingleadname]                        NVARCHAR (160) NULL,
    [ltf_issuingleadyominame]                    NVARCHAR (160) NULL,
    [ltf_issuingopportunity]                     VARCHAR (36)   NULL,
    [ltf_issuingopportunityname]                 NVARCHAR (300) NULL,
    [ltf_name]                                   NVARCHAR (100) NULL,
    [ltf_passbegindate]                          DATETIME       NULL,
    [ltf_qrcode]                                 VARCHAR (8000) NULL,
    [ltf_remainingdays]                          INT            NULL,
    [modifiedby]                                 VARCHAR (36)   NULL,
    [modifiedbyname]                             NVARCHAR (200) NULL,
    [modifiedbyyominame]                         NVARCHAR (200) NULL,
    [modifiedon]                                 DATETIME       NULL,
    [modifiedonbehalfby]                         VARCHAR (36)   NULL,
    [modifiedonbehalfbyname]                     NVARCHAR (200) NULL,
    [modifiedonbehalfbyyominame]                 NVARCHAR (200) NULL,
    [organizationid]                             VARCHAR (36)   NULL,
    [organizationidname]                         NVARCHAR (160) NULL,
    [overriddencreatedon]                        DATETIME       NULL,
    [statecode]                                  INT            NULL,
    [statecodename]                              NVARCHAR (255) NULL,
    [statuscode]                                 INT            NULL,
    [statuscodename]                             NVARCHAR (255) NULL,
    [timezoneruleversionnumber]                  INT            NULL,
    [utcconversiontimezonecode]                  INT            NULL,
    [versionnumber]                              BIGINT         NULL,
    [InsertedDateTime]                           DATETIME       NULL,
    [InsertUser]                                 VARCHAR (50)   NULL,
    [UpdatedDateTime]                            DATETIME       NULL,
    [UpdateUser]                                 VARCHAR (50)   NULL,
    [ltf_prospectid]                             NVARCHAR (100) NULL,
    [ltf_referringmember]                        VARCHAR (36)   NULL,
    [ltf_referringmemberid]                      NVARCHAR (100) NULL,
    [ltf_sendid]                                 NVARCHAR (100) NULL,
    [ltf_referringcorpacctid]                    NVARCHAR (100) NULL,
    [ltf_referringcorpacct]                      VARCHAR (36)   NULL,
    [dv_batch_id]                                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
