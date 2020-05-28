﻿CREATE TABLE [dbo].[stage_crmcloudsync_LTF_Inquiry] (
    [stage_crmcloudsync_LTF_Inquiry_id] BIGINT          NOT NULL,
    [activityid]                        VARCHAR (36)    NULL,
    [activitytypecode]                  NVARCHAR (64)   NULL,
    [activitytypecodename]              NVARCHAR (255)  NULL,
    [actualdurationminutes]             INT             NULL,
    [actualend]                         DATETIME        NULL,
    [actualstart]                       DATETIME        NULL,
    [bcc]                               VARCHAR (8000)  NULL,
    [cc]                                VARCHAR (8000)  NULL,
    [createdby]                         VARCHAR (36)    NULL,
    [createdbyname]                     NVARCHAR (200)  NULL,
    [createdbyyominame]                 NVARCHAR (200)  NULL,
    [createdon]                         DATETIME        NULL,
    [createdonbehalfby]                 VARCHAR (36)    NULL,
    [createdonbehalfbyname]             NVARCHAR (200)  NULL,
    [createdonbehalfbyyominame]         NVARCHAR (200)  NULL,
    [customers]                         VARCHAR (8000)  NULL,
    [deliverylastattemptedon]           DATETIME        NULL,
    [deliveryprioritycode]              INT             NULL,
    [deliveryprioritycodename]          NVARCHAR (255)  NULL,
    [description]                       VARCHAR (8000)  NULL,
    [exchangerate]                      DECIMAL (28)    NULL,
    [from]                              VARCHAR (8000)  NULL,
    [importsequencenumber]              INT             NULL,
    [instancetypecode]                  INT             NULL,
    [instancetypecodename]              NVARCHAR (255)  NULL,
    [isbilled]                          BIT             NULL,
    [isbilledname]                      NVARCHAR (255)  NULL,
    [ismapiprivate]                     BIT             NULL,
    [ismapiprivatename]                 NVARCHAR (255)  NULL,
    [isregularactivity]                 BIT             NULL,
    [isregularactivityname]             NVARCHAR (255)  NULL,
    [isworkflowcreated]                 BIT             NULL,
    [isworkflowcreatedname]             NVARCHAR (255)  NULL,
    [leftvoicemail]                     BIT             NULL,
    [leftvoicemailname]                 NVARCHAR (255)  NULL,
    [ltf_address1_city]                 NVARCHAR (50)   NULL,
    [ltf_address1_country]              NVARCHAR (50)   NULL,
    [ltf_address1_line1]                NVARCHAR (50)   NULL,
    [ltf_address1_line2]                NVARCHAR (50)   NULL,
    [ltf_address1_postalcode]           NVARCHAR (100)  NULL,
    [ltf_address1_stateorprovince]      NVARCHAR (25)   NULL,
    [ltf_age]                           INT             NULL,
    [ltf_besttimetocontact]             NVARCHAR (40)   NULL,
    [ltf_birtdate]                      DATETIME        NULL,
    [ltf_birthdate]                     DATETIME        NULL,
    [ltf_birthyear]                     NVARCHAR (4)    NULL,
    [ltf_cid]                           NVARCHAR (100)  NULL,
    [ltf_communicationconsent]          INT             NULL,
    [ltf_communicationconsentname]      NVARCHAR (255)  NULL,
    [ltf_consentdatetime]               DATETIME        NULL,
    [ltf_consentipaddress]              NVARCHAR (19)   NULL,
    [ltf_consenttext]                   NVARCHAR (300)  NULL,
    [ltf_currentmember]                 BIT             NULL,
    [ltf_currentmembername]             NVARCHAR (255)  NULL,
    [ltf_custservemail]                 NVARCHAR (100)  NULL,
    [ltf_dcmp]                          NVARCHAR (100)  NULL,
    [ltf_destinationqueue]              NVARCHAR (100)  NULL,
    [ltf_duplicatecontactfound]         BIT             NULL,
    [ltf_duplicatecontactfoundname]     NVARCHAR (255)  NULL,
    [ltf_duplicateleadfound]            BIT             NULL,
    [ltf_duplicateleadfoundname]        NVARCHAR (255)  NULL,
    [ltf_emailaddress1]                 NVARCHAR (100)  NULL,
    [ltf_emailtemplate]                 NVARCHAR (100)  NULL,
    [ltf_employeenumber]                NVARCHAR (10)   NULL,
    [ltf_employer]                      NVARCHAR (50)   NULL,
    [ltf_exacttargetemailsent]          BIT             NULL,
    [ltf_exacttargetemailsentname]      NVARCHAR (255)  NULL,
    [ltf_firstname]                     NVARCHAR (50)   NULL,
    [ltf_gcid]                          NVARCHAR (100)  NULL,
    [ltf_gclid]                         NVARCHAR (100)  NULL,
    [ltf_gendercode]                    INT             NULL,
    [ltf_gendercodename]                NVARCHAR (255)  NULL,
    [ltf_group]                         NVARCHAR (100)  NULL,
    [ltf_inquirysource]                 NVARCHAR (100)  NULL,
    [ltf_inquirytype]                   NVARCHAR (100)  NULL,
    [ltf_interests]                     NVARCHAR (300)  NULL,
    [ltf_keywords]                      NVARCHAR (100)  NULL,
    [ltf_landingpage]                   NVARCHAR (100)  NULL,
    [ltf_lastname]                      NVARCHAR (50)   NULL,
    [ltf_latitude]                      DECIMAL (26, 6) NULL,
    [ltf_leadtype]                      NVARCHAR (100)  NULL,
    [ltf_longitude]                     DECIMAL (26, 6) NULL,
    [ltf_memberid]                      NVARCHAR (25)   NULL,
    [ltf_membershipinforequested]       NVARCHAR (50)   NULL,
    [ltf_mmsclubid]                     NVARCHAR (10)   NULL,
    [ltf_primarygoal]                   NVARCHAR (100)  NULL,
    [ltf_referringcontactid]            VARCHAR (36)    NULL,
    [ltf_referringcontactidname]        NVARCHAR (160)  NULL,
    [ltf_referringcontactidyominame]    NVARCHAR (160)  NULL,
    [ltf_referringmemberid]             NVARCHAR (10)   NULL,
    [ltf_registrationcode]              NVARCHAR (100)  NULL,
    [ltf_requesttype]                   NVARCHAR (100)  NULL,
    [ltf_telephone1]                    NVARCHAR (50)   NULL,
    [ltf_telephone2]                    NVARCHAR (50)   NULL,
    [ltf_undereighteen]                 BIT             NULL,
    [ltf_undereighteenname]             NVARCHAR (255)  NULL,
    [ltf_utmcampaign]                   NVARCHAR (100)  NULL,
    [ltf_utmcontent]                    NVARCHAR (100)  NULL,
    [ltf_utmmedium]                     NVARCHAR (100)  NULL,
    [ltf_utmsource]                     NVARCHAR (100)  NULL,
    [ltf_utmterm]                       NVARCHAR (100)  NULL,
    [ltf_visitcount]                    INT             NULL,
    [modifiedby]                        VARCHAR (36)    NULL,
    [modifiedbyname]                    NVARCHAR (200)  NULL,
    [modifiedbyyominame]                NVARCHAR (200)  NULL,
    [modifiedon]                        DATETIME        NULL,
    [modifiedonbehalfby]                VARCHAR (36)    NULL,
    [modifiedonbehalfbyname]            NVARCHAR (200)  NULL,
    [modifiedonbehalfbyyominame]        NVARCHAR (200)  NULL,
    [optionalattendees]                 VARCHAR (8000)  NULL,
    [organizer]                         VARCHAR (8000)  NULL,
    [overriddencreatedon]               DATETIME        NULL,
    [ownerid]                           VARCHAR (36)    NULL,
    [owneridname]                       NVARCHAR (200)  NULL,
    [owneridtype]                       NVARCHAR (64)   NULL,
    [owneridyominame]                   NVARCHAR (200)  NULL,
    [owningbusinessunit]                VARCHAR (36)    NULL,
    [owningteam]                        VARCHAR (36)    NULL,
    [owninguser]                        VARCHAR (36)    NULL,
    [partners]                          VARCHAR (8000)  NULL,
    [postponeactivityprocessinguntil]   DATETIME        NULL,
    [prioritycode]                      INT             NULL,
    [prioritycodename]                  NVARCHAR (255)  NULL,
    [processid]                         VARCHAR (36)    NULL,
    [regardingobjectid]                 VARCHAR (36)    NULL,
    [regardingobjectidname]             VARCHAR (8000)  NULL,
    [regardingobjectidyominame]         VARCHAR (8000)  NULL,
    [regardingobjecttypecode]           NVARCHAR (64)   NULL,
    [requiredattendees]                 VARCHAR (8000)  NULL,
    [resources]                         VARCHAR (8000)  NULL,
    [scheduleddurationminutes]          INT             NULL,
    [scheduledend]                      DATETIME        NULL,
    [scheduledstart]                    DATETIME        NULL,
    [sendermailboxid]                   VARCHAR (36)    NULL,
    [sendermailboxidname]               NVARCHAR (200)  NULL,
    [senton]                            DATETIME        NULL,
    [seriesid]                          VARCHAR (36)    NULL,
    [serviceid]                         VARCHAR (36)    NULL,
    [serviceidname]                     NVARCHAR (160)  NULL,
    [stageid]                           VARCHAR (36)    NULL,
    [statecode]                         INT             NULL,
    [statecodename]                     NVARCHAR (255)  NULL,
    [statuscode]                        INT             NULL,
    [statuscodename]                    NVARCHAR (255)  NULL,
    [subject]                           NVARCHAR (200)  NULL,
    [timezoneruleversionnumber]         INT             NULL,
    [to]                                VARCHAR (8000)  NULL,
    [transactioncurrencyid]             VARCHAR (36)    NULL,
    [transactioncurrencyidname]         NVARCHAR (100)  NULL,
    [utcconversiontimezonecode]         INT             NULL,
    [versionnumber]                     BIGINT          NULL,
    [InsertedDateTime]                  DATETIME        NULL,
    [InsertUser]                        VARCHAR (100)   NULL,
    [UpdatedDateTime]                   DATETIME        NULL,
    [UpdateUser]                        VARCHAR (50)    NULL,
    [Community]                         INT             NULL,
    [CommunityName]                     NVARCHAR (255)  NULL,
    [ltf_visitorid]                     NVARCHAR (40)   NULL,
    [ltf_clubname]                      VARCHAR (36)    NULL,
    [ltf_clubnamename]                  NVARCHAR (100)  NULL,
    [ltf_device]                        NVARCHAR (100)  NULL,
    [ltf_operatingsystem]               NVARCHAR (100)  NULL,
    [ltf_referringdomain]               NVARCHAR (100)  NULL,
    [ltf_referringpage]                 NVARCHAR (500)  NULL,
    [ltf_useridleadid]                  NVARCHAR (100)  NULL,
    [traversedpath]                     NVARCHAR (1250) NULL,
    [activityadditionalparams]          VARCHAR (8000)  NULL,
    [ltf_utmaudience]                   NVARCHAR (100)  NULL,
    [ltf_utmimage]                      NVARCHAR (100)  NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
