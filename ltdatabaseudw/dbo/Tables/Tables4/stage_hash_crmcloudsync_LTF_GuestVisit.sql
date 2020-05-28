﻿CREATE TABLE [dbo].[stage_hash_crmcloudsync_LTF_GuestVisit] (
    [stage_hash_crmcloudsync_LTF_GuestVisit_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)       NOT NULL,
    [activityid]                                VARCHAR (36)    NULL,
    [activitytypecode]                          NVARCHAR (64)   NULL,
    [activitytypecodename]                      NVARCHAR (255)  NULL,
    [actualdurationminutes]                     INT             NULL,
    [actualend]                                 DATETIME        NULL,
    [actualstart]                               DATETIME        NULL,
    [bcc]                                       VARCHAR (8000)  NULL,
    [cc]                                        VARCHAR (8000)  NULL,
    [createdby]                                 VARCHAR (36)    NULL,
    [createdbyname]                             NVARCHAR (200)  NULL,
    [createdbyyominame]                         NVARCHAR (200)  NULL,
    [createdon]                                 DATETIME        NULL,
    [createdonbehalfby]                         VARCHAR (36)    NULL,
    [createdonbehalfbyname]                     NVARCHAR (200)  NULL,
    [createdonbehalfbyyominame]                 NVARCHAR (200)  NULL,
    [customers]                                 VARCHAR (8000)  NULL,
    [deliverylastattemptedon]                   DATETIME        NULL,
    [deliveryprioritycode]                      INT             NULL,
    [deliveryprioritycodename]                  NVARCHAR (255)  NULL,
    [description]                               VARCHAR (8000)  NULL,
    [exchangerate]                              DECIMAL (28)    NULL,
    [from]                                      NVARCHAR (2000) NULL,
    [importsequencenumber]                      INT             NULL,
    [instancetypecode]                          INT             NULL,
    [instancetypecodename]                      NVARCHAR (255)  NULL,
    [isbilled]                                  BIT             NULL,
    [isbilledname]                              NVARCHAR (255)  NULL,
    [ismapiprivate]                             BIT             NULL,
    [ismapiprivatename]                         NVARCHAR (255)  NULL,
    [isregularactivity]                         BIT             NULL,
    [isregularactivityname]                     NVARCHAR (255)  NULL,
    [isworkflowcreated]                         BIT             NULL,
    [isworkflowcreatedname]                     NVARCHAR (255)  NULL,
    [leftvoicemail]                             BIT             NULL,
    [leftvoicemailname]                         NVARCHAR (255)  NULL,
    [ltf_address1_addresstypecode]              INT             NULL,
    [ltf_address1_addresstypecodename]          NVARCHAR (255)  NULL,
    [ltf_address1_city]                         NVARCHAR (50)   NULL,
    [ltf_address1_country]                      NVARCHAR (50)   NULL,
    [ltf_address1_county]                       NVARCHAR (50)   NULL,
    [ltf_address1_line1]                        NVARCHAR (50)   NULL,
    [ltf_address1_line2]                        NVARCHAR (50)   NULL,
    [ltf_address1_line3]                        NVARCHAR (50)   NULL,
    [ltf_address1_postalcode]                   NVARCHAR (20)   NULL,
    [ltf_address1_postofficebox]                NVARCHAR (20)   NULL,
    [ltf_address1_stateorprovince]              NVARCHAR (50)   NULL,
    [ltf_businessunitguestvisitid]              VARCHAR (36)    NULL,
    [ltf_businessunitguestvisitidname]          NVARCHAR (160)  NULL,
    [ltf_clubid]                                VARCHAR (36)    NULL,
    [ltf_clubidname]                            NVARCHAR (100)  NULL,
    [ltf_dateofbirth]                           DATETIME        NULL,
    [ltf_emailaddress1]                         NVARCHAR (100)  NULL,
    [ltf_employer]                              NVARCHAR (200)  NULL,
    [ltf_firstname]                             NVARCHAR (50)   NULL,
    [ltf_gender]                                INT             NULL,
    [ltf_gendername]                            NVARCHAR (255)  NULL,
    [ltf_guesttype]                             INT             NULL,
    [ltf_guesttypename]                         NVARCHAR (255)  NULL,
    [ltf_lastname]                              NVARCHAR (50)   NULL,
    [ltf_middlename]                            NVARCHAR (50)   NULL,
    [ltf_mobilephone]                           NVARCHAR (50)   NULL,
    [ltf_pager]                                 NVARCHAR (50)   NULL,
    [ltf_referredby]                            VARCHAR (36)    NULL,
    [ltf_referredbyname]                        NVARCHAR (160)  NULL,
    [ltf_referredbyyominame]                    NVARCHAR (160)  NULL,
    [ltf_telephone1]                            NVARCHAR (50)   NULL,
    [ltf_telephone2]                            NVARCHAR (50)   NULL,
    [ltf_telephone3]                            NVARCHAR (50)   NULL,
    [ltf_websiteurl]                            NVARCHAR (100)  NULL,
    [modifiedby]                                VARCHAR (36)    NULL,
    [modifiedbyname]                            NVARCHAR (200)  NULL,
    [modifiedbyyominame]                        NVARCHAR (200)  NULL,
    [modifiedon]                                DATETIME        NULL,
    [modifiedonbehalfby]                        VARCHAR (36)    NULL,
    [modifiedonbehalfbyname]                    NVARCHAR (200)  NULL,
    [modifiedonbehalfbyyominame]                NVARCHAR (200)  NULL,
    [new_clubname]                              VARCHAR (36)    NULL,
    [new_clubnamename]                          NVARCHAR (100)  NULL,
    [optionalattendees]                         NVARCHAR (2000) NULL,
    [organizer]                                 NVARCHAR (2000) NULL,
    [overriddencreatedon]                       DATETIME        NULL,
    [ownerid]                                   VARCHAR (36)    NULL,
    [owneridname]                               NVARCHAR (200)  NULL,
    [owneridtype]                               NVARCHAR (64)   NULL,
    [owneridyominame]                           NVARCHAR (200)  NULL,
    [owningbusinessunit]                        VARCHAR (36)    NULL,
    [owningteam]                                VARCHAR (36)    NULL,
    [owninguser]                                VARCHAR (36)    NULL,
    [partners]                                  NVARCHAR (2000) NULL,
    [postponeactivityprocessinguntil]           DATETIME        NULL,
    [prioritycode]                              INT             NULL,
    [prioritycodename]                          NVARCHAR (255)  NULL,
    [processid]                                 VARCHAR (36)    NULL,
    [regardingobjectid]                         VARCHAR (36)    NULL,
    [regardingobjectidname]                     VARCHAR (8000)  NULL,
    [regardingobjectidyominame]                 VARCHAR (8000)  NULL,
    [regardingobjecttypecode]                   NVARCHAR (64)   NULL,
    [requiredattendees]                         NVARCHAR (2000) NULL,
    [resources]                                 NVARCHAR (2000) NULL,
    [scheduleddurationminutes]                  INT             NULL,
    [scheduledend]                              DATETIME        NULL,
    [scheduledstart]                            DATETIME        NULL,
    [sendermailboxid]                           VARCHAR (36)    NULL,
    [sendermailboxidname]                       NVARCHAR (200)  NULL,
    [senton]                                    DATETIME        NULL,
    [seriesid]                                  VARCHAR (36)    NULL,
    [serviceid]                                 VARCHAR (36)    NULL,
    [serviceidname]                             NVARCHAR (160)  NULL,
    [stageid]                                   VARCHAR (36)    NULL,
    [statecode]                                 INT             NULL,
    [statecodename]                             NVARCHAR (255)  NULL,
    [statuscode]                                INT             NULL,
    [statuscodename]                            NVARCHAR (255)  NULL,
    [subject]                                   NVARCHAR (200)  NULL,
    [timezoneruleversionnumber]                 INT             NULL,
    [to]                                        NVARCHAR (2000) NULL,
    [transactioncurrencyid]                     VARCHAR (36)    NULL,
    [transactioncurrencyidname]                 NVARCHAR (100)  NULL,
    [utcconversiontimezonecode]                 INT             NULL,
    [versionnumber]                             BIGINT          NULL,
    [InsertedDateTime]                          DATETIME        NULL,
    [InsertUser]                                VARCHAR (50)    NULL,
    [UpdatedDateTime]                           DATETIME        NULL,
    [UpdateUser]                                VARCHAR (50)    NULL,
    [activityadditionalparams]                  VARCHAR (8000)  NULL,
    [community]                                 INT             NULL,
    [communityname]                             NVARCHAR (255)  NULL,
    [ltf_assignedmea]                           VARCHAR (36)    NULL,
    [ltf_assignedmeaname]                       NVARCHAR (200)  NULL,
    [ltf_assignedmeayominame]                   NVARCHAR (200)  NULL,
    [traversedpath]                             NVARCHAR (1250) NULL,
    [ltf_appointmentid]                         VARCHAR (36)    NULL,
    [ltf_appointmentidname]                     NVARCHAR (200)  NULL,
    [ltf_clubcloseto]                           INT             NULL,
    [ltf_clubclosetoname]                       NVARCHAR (255)  NULL,
    [ltf_Interests]                             NVARCHAR (1000) NULL,
    [ltf_leadid]                                VARCHAR (36)    NULL,
    [ltf_leadidname]                            NVARCHAR (160)  NULL,
    [ltf_matchingcontactcount]                  NVARCHAR (100)  NULL,
    [ltf_matchingleadcount]                     NVARCHAR (100)  NULL,
    [ltf_membershipinterest]                    INT             NULL,
    [ltf_membershipinterestname]                NVARCHAR (255)  NULL,
    [ltf_online]                                INT             NULL,
    [ltf_onlinename]                            NVARCHAR (255)  NULL,
    [ltf_outofarea]                             INT             NULL,
    [ltf_outofareaname]                         NVARCHAR (255)  NULL,
    [ltf_partyid]                               NVARCHAR (100)  NULL,
    [ltf_referralsource]                        INT             NULL,
    [ltf_referralsourcename]                    NVARCHAR (255)  NULL,
    [ltf_referringmemberid]                     NVARCHAR (100)  NULL,
    [ltf_requestdate]                           DATETIME        NULL,
    [ltf_requestid]                             NVARCHAR (100)  NULL,
    [ltf_sameday]                               INT             NULL,
    [ltf_samedayname]                           NVARCHAR (255)  NULL,
    [ltf_agreementsignature]                    VARCHAR (8000)  NULL,
    [ltf_campaigninstance]                      VARCHAR (36)    NULL,
    [ltf_campaigninstancename]                  NVARCHAR (100)  NULL,
    [ltf_deductguestpriv]                       BIT             NULL,
    [ltf_qrcodeused]                            BIT             NULL,
    [ltf_source]                                NVARCHAR (100)  NULL,
    [ltf_timeout]                               BIT             NULL,
    [ltf_timeoutservice]                        NVARCHAR (100)  NULL,
    [ltf_prospectid]                            NVARCHAR (100)  NULL,
    [ltf_mostrecentcasl]                        DATETIME        NULL,
    [ltf_sendid]                                NVARCHAR (100)  NULL,
    [ltf_referringcorpacctid]                   NVARCHAR (100)  NULL,
    [ltf_gracevisit]                            BIT             NULL,
    [ltf_lineofbusiness]                        INT             NULL,
    [ltf_lineofbusinessname]                    NVARCHAR (255)  NULL,
    [dv_load_date_time]                         DATETIME        NOT NULL,
    [dv_batch_id]                               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));
