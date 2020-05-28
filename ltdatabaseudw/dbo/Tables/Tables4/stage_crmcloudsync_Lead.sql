﻿CREATE TABLE [dbo].[stage_crmcloudsync_Lead] (
    [stage_crmcloudsync_Lead_id]        BIGINT          NOT NULL,
    [accountid]                         VARCHAR (36)    NULL,
    [accountidname]                     NVARCHAR (160)  NULL,
    [accountidyominame]                 NVARCHAR (160)  NULL,
    [address1_addressid]                VARCHAR (36)    NULL,
    [address1_addresstypecode]          INT             NULL,
    [address1_addresstypecodename]      NVARCHAR (255)  NULL,
    [address1_city]                     NVARCHAR (50)   NULL,
    [address1_composite]                NVARCHAR (4000) NULL,
    [address1_country]                  NVARCHAR (80)   NULL,
    [address1_county]                   NVARCHAR (50)   NULL,
    [address1_fax]                      NVARCHAR (50)   NULL,
    [address1_latitude]                 DECIMAL (26, 6) NULL,
    [address1_line1]                    NVARCHAR (50)   NULL,
    [address1_line2]                    NVARCHAR (50)   NULL,
    [address1_line3]                    NVARCHAR (250)  NULL,
    [address1_longitude]                DECIMAL (26, 6) NULL,
    [address1_name]                     NVARCHAR (100)  NULL,
    [address1_postalcode]               NVARCHAR (20)   NULL,
    [address1_postofficebox]            NVARCHAR (20)   NULL,
    [address1_shippingmethodcode]       INT             NULL,
    [address1_shippingmethodcodename]   NVARCHAR (255)  NULL,
    [address1_stateorprovince]          NVARCHAR (3)    NULL,
    [address1_telephone1]               NVARCHAR (50)   NULL,
    [address1_telephone2]               NVARCHAR (50)   NULL,
    [address1_telephone3]               NVARCHAR (50)   NULL,
    [address1_upszone]                  NVARCHAR (4)    NULL,
    [address1_utcoffset]                INT             NULL,
    [address2_addressid]                VARCHAR (36)    NULL,
    [address2_addresstypecode]          INT             NULL,
    [address2_addresstypecodename]      NVARCHAR (255)  NULL,
    [address2_city]                     NVARCHAR (80)   NULL,
    [address2_composite]                NVARCHAR (4000) NULL,
    [address2_country]                  NVARCHAR (80)   NULL,
    [address2_county]                   NVARCHAR (50)   NULL,
    [address2_fax]                      NVARCHAR (50)   NULL,
    [address2_latitude]                 DECIMAL (26, 6) NULL,
    [address2_line1]                    NVARCHAR (250)  NULL,
    [address2_line2]                    NVARCHAR (250)  NULL,
    [address2_line3]                    NVARCHAR (250)  NULL,
    [address2_longitude]                DECIMAL (26, 6) NULL,
    [address2_name]                     NVARCHAR (100)  NULL,
    [address2_postalcode]               NVARCHAR (20)   NULL,
    [address2_postofficebox]            NVARCHAR (20)   NULL,
    [address2_shippingmethodcode]       INT             NULL,
    [address2_shippingmethodcodename]   NVARCHAR (255)  NULL,
    [address2_stateorprovince]          NVARCHAR (50)   NULL,
    [address2_telephone1]               NVARCHAR (50)   NULL,
    [address2_telephone2]               NVARCHAR (50)   NULL,
    [address2_telephone3]               NVARCHAR (50)   NULL,
    [address2_upszone]                  NVARCHAR (4)    NULL,
    [address2_utcoffset]                INT             NULL,
    [budgetamount]                      DECIMAL (26, 6) NULL,
    [budgetamount_base]                 DECIMAL (26, 6) NULL,
    [budgetstatus]                      INT             NULL,
    [budgetstatusname]                  NVARCHAR (255)  NULL,
    [campaignid]                        VARCHAR (36)    NULL,
    [campaignidname]                    NVARCHAR (128)  NULL,
    [companyname]                       NVARCHAR (100)  NULL,
    [confirminterest]                   BIT             NULL,
    [confirminterestname]               NVARCHAR (255)  NULL,
    [contactid]                         VARCHAR (36)    NULL,
    [contactidname]                     NVARCHAR (160)  NULL,
    [contactidyominame]                 NVARCHAR (160)  NULL,
    [createdby]                         VARCHAR (36)    NULL,
    [createdbyname]                     NVARCHAR (200)  NULL,
    [createdbyyominame]                 NVARCHAR (200)  NULL,
    [createdon]                         DATETIME        NULL,
    [createdonbehalfby]                 VARCHAR (36)    NULL,
    [createdonbehalfbyname]             NVARCHAR (200)  NULL,
    [createdonbehalfbyyominame]         NVARCHAR (200)  NULL,
    [customerid]                        VARCHAR (36)    NULL,
    [customeridname]                    NVARCHAR (160)  NULL,
    [customeridtype]                    NVARCHAR (64)   NULL,
    [customeridyominame]                NVARCHAR (450)  NULL,
    [decisionmaker]                     BIT             NULL,
    [decisionmakername]                 NVARCHAR (255)  NULL,
    [description]                       NVARCHAR (4000) NULL,
    [donotbulkemail]                    BIT             NULL,
    [donotbulkemailname]                NVARCHAR (255)  NULL,
    [donotemail]                        BIT             NULL,
    [donotemailname]                    NVARCHAR (255)  NULL,
    [donotfax]                          BIT             NULL,
    [donotfaxname]                      NVARCHAR (255)  NULL,
    [donotphone]                        BIT             NULL,
    [donotphonename]                    NVARCHAR (255)  NULL,
    [donotpostalmail]                   BIT             NULL,
    [donotpostalmailname]               NVARCHAR (255)  NULL,
    [donotsendmarketingmaterialname]    NVARCHAR (255)  NULL,
    [donotsendmm]                       BIT             NULL,
    [emailaddress1]                     NVARCHAR (100)  NULL,
    [emailaddress2]                     NVARCHAR (100)  NULL,
    [emailaddress3]                     NVARCHAR (100)  NULL,
    [entityimage_timestamp]             BIGINT          NULL,
    [entityimage_url]                   NVARCHAR (200)  NULL,
    [entityimageid]                     VARCHAR (36)    NULL,
    [estimatedamount]                   DECIMAL (26, 6) NULL,
    [estimatedamount_base]              DECIMAL (26, 6) NULL,
    [estimatedclosedate]                DATETIME        NULL,
    [estimatedvalue]                    DECIMAL (26, 6) NULL,
    [evaluatefit]                       BIT             NULL,
    [evaluatefitname]                   NVARCHAR (255)  NULL,
    [exchangerate]                      DECIMAL (28)    NULL,
    [fax]                               NVARCHAR (50)   NULL,
    [firstname]                         NVARCHAR (50)   NULL,
    [fullname]                          NVARCHAR (160)  NULL,
    [importsequencenumber]              INT             NULL,
    [industrycode]                      INT             NULL,
    [industrycodename]                  NVARCHAR (255)  NULL,
    [initialcommunication]              INT             NULL,
    [initialcommunicationname]          NVARCHAR (255)  NULL,
    [isprivatename]                     NVARCHAR (255)  NULL,
    [jobtitle]                          NVARCHAR (100)  NULL,
    [lastname]                          NVARCHAR (50)   NULL,
    [lastusedincampaign]                DATETIME        NULL,
    [leadid]                            VARCHAR (36)    NULL,
    [leadqualitycode]                   INT             NULL,
    [leadqualitycodename]               NVARCHAR (255)  NULL,
    [leadsourcecode]                    INT             NULL,
    [leadsourcecodename]                NVARCHAR (255)  NULL,
    [ltf_birthdate]                     DATETIME        NULL,
    [ltf_birthyear]                     NVARCHAR (4)    NULL,
    [ltf_businessunit]                  VARCHAR (36)    NULL,
    [ltf_businessunitname]              NVARCHAR (160)  NULL,
    [ltf_clubid]                        VARCHAR (36)    NULL,
    [ltf_clubidname]                    NVARCHAR (100)  NULL,
    [ltf_currentmember]                 BIT             NULL,
    [ltf_currentmembername]             NVARCHAR (255)  NULL,
    [ltf_donotemail_address1]           BIT             NULL,
    [ltf_donotemail_address1name]       NVARCHAR (255)  NULL,
    [ltf_donotemail_address2]           BIT             NULL,
    [ltf_donotemail_address2name]       NVARCHAR (255)  NULL,
    [ltf_donotphone_mobilephone]        BIT             NULL,
    [ltf_donotphone_mobilephonename]    NVARCHAR (255)  NULL,
    [ltf_donotphone_telephone1]         BIT             NULL,
    [ltf_donotphone_telephone1name]     NVARCHAR (255)  NULL,
    [ltf_donotphone_telephone2]         BIT             NULL,
    [ltf_donotphone_telephone2name]     NVARCHAR (255)  NULL,
    [ltf_employeenumber]                NVARCHAR (100)  NULL,
    [ltf_employerid]                    VARCHAR (36)    NULL,
    [ltf_employeridname]                NVARCHAR (160)  NULL,
    [ltf_employeridyominame]            NVARCHAR (160)  NULL,
    [ltf_gendercode]                    INT             NULL,
    [ltf_gendercodename]                NVARCHAR (255)  NULL,
    [ltf_guesttoleadid]                 VARCHAR (36)    NULL,
    [ltf_guesttoleadidname]             NVARCHAR (200)  NULL,
    [ltf_insertedbysystem]              BIT             NULL,
    [ltf_insertedbysystemname]          NVARCHAR (255)  NULL,
    [ltf_lastactivity]                  NVARCHAR (100)  NULL,
    [ltf_lastactivitylead]              DATETIME        NULL,
    [ltf_leadsource]                    INT             NULL,
    [ltf_leadsourcename]                NVARCHAR (255)  NULL,
    [ltf_leadtype]                      INT             NULL,
    [ltf_leadtypename]                  NVARCHAR (255)  NULL,
    [ltf_legacy]                        NVARCHAR (4000) NULL,
    [ltf_measurablegoalid]              VARCHAR (36)    NULL,
    [ltf_measurablegoalidname]          NVARCHAR (100)  NULL,
    [ltf_membershipinforequested]       NVARCHAR (50)   NULL,
    [ltf_membershiplevel]               INT             NULL,
    [ltf_membershiplevelname]           NVARCHAR (255)  NULL,
    [ltf_membershiptype]                INT             NULL,
    [ltf_membershiptypename]            NVARCHAR (255)  NULL,
    [ltf_mmsclubid]                     INT             NULL,
    [ltf_nugget]                        NVARCHAR (4000) NULL,
    [ltf_park]                          BIT             NULL,
    [ltf_parkcomments]                  NVARCHAR (400)  NULL,
    [ltf_parkname]                      NVARCHAR (255)  NULL,
    [ltf_parkreason]                    INT             NULL,
    [ltf_parkreasonname]                NVARCHAR (255)  NULL,
    [ltf_parkuntil]                     DATETIME        NULL,
    [ltf_primaryobjectiveid]            VARCHAR (36)    NULL,
    [ltf_primaryobjectiveidname]        NVARCHAR (100)  NULL,
    [ltf_referringcontactid]            VARCHAR (36)    NULL,
    [ltf_referringcontactidname]        NVARCHAR (160)  NULL,
    [ltf_referringcontactidyominame]    NVARCHAR (160)  NULL,
    [ltf_referringmemberid]             NVARCHAR (10)   NULL,
    [ltf_registrationcode]              NVARCHAR (100)  NULL,
    [ltf_specificgoalid]                VARCHAR (36)    NULL,
    [ltf_specificgoalidname]            NVARCHAR (100)  NULL,
    [ltf_suffix]                        NVARCHAR (3)    NULL,
    [ltf_taskid]                        VARCHAR (36)    NULL,
    [ltf_taskidname]                    NVARCHAR (200)  NULL,
    [ltf_timezonecode]                  INT             NULL,
    [ltf_udwid]                         BIGINT          NULL,
    [ltf_volatilecontact]               BIT             NULL,
    [ltf_volatilecontactname]           NVARCHAR (255)  NULL,
    [ltf_webteamid]                     VARCHAR (36)    NULL,
    [ltf_webteamidname]                 NVARCHAR (160)  NULL,
    [ltf_webteamidyominame]             NVARCHAR (160)  NULL,
    [masterid]                          VARCHAR (36)    NULL,
    [masterleadidname]                  NVARCHAR (160)  NULL,
    [masterleadidyominame]              NVARCHAR (160)  NULL,
    [merged]                            BIT             NULL,
    [mergedname]                        NVARCHAR (255)  NULL,
    [middlename]                        NVARCHAR (50)   NULL,
    [mobilephone]                       NVARCHAR (20)   NULL,
    [modifiedby]                        VARCHAR (36)    NULL,
    [modifiedbyname]                    NVARCHAR (200)  NULL,
    [modifiedbyyominame]                NVARCHAR (200)  NULL,
    [modifiedon]                        DATETIME        NULL,
    [modifiedonbehalfby]                VARCHAR (36)    NULL,
    [modifiedonbehalfbyname]            NVARCHAR (200)  NULL,
    [modifiedonbehalfbyyominame]        NVARCHAR (200)  NULL,
    [need]                              INT             NULL,
    [needname]                          NVARCHAR (255)  NULL,
    [numberofemployees]                 INT             NULL,
    [originatingcaseid]                 VARCHAR (36)    NULL,
    [originatingcaseidname]             NVARCHAR (200)  NULL,
    [overriddencreatedon]               DATETIME        NULL,
    [ownerid]                           VARCHAR (36)    NULL,
    [owneridname]                       NVARCHAR (200)  NULL,
    [owneridtype]                       NVARCHAR (64)   NULL,
    [owneridyominame]                   NVARCHAR (200)  NULL,
    [owningbusinessunit]                VARCHAR (36)    NULL,
    [owningteam]                        VARCHAR (36)    NULL,
    [owninguser]                        VARCHAR (36)    NULL,
    [pager]                             NVARCHAR (20)   NULL,
    [parentaccountid]                   VARCHAR (36)    NULL,
    [parentaccountidname]               NVARCHAR (160)  NULL,
    [parentaccountidyominame]           NVARCHAR (160)  NULL,
    [parentcontactid]                   VARCHAR (36)    NULL,
    [parentcontactidname]               NVARCHAR (160)  NULL,
    [parentcontactidyominame]           NVARCHAR (160)  NULL,
    [participatesinworkflow]            BIT             NULL,
    [participatesinworkflowname]        NVARCHAR (255)  NULL,
    [preferredcontactmethodcode]        INT             NULL,
    [preferredcontactmethodcodename]    NVARCHAR (255)  NULL,
    [prioritycode]                      INT             NULL,
    [prioritycodename]                  NVARCHAR (255)  NULL,
    [processid]                         VARCHAR (36)    NULL,
    [purchaseprocess]                   INT             NULL,
    [purchaseprocessname]               NVARCHAR (255)  NULL,
    [purchasetimeframe]                 INT             NULL,
    [purchasetimeframename]             NVARCHAR (255)  NULL,
    [qualificationcomments]             NVARCHAR (4000) NULL,
    [qualifyingopportunityid]           VARCHAR (36)    NULL,
    [qualifyingopportunityidname]       NVARCHAR (300)  NULL,
    [relatedobjectid]                   VARCHAR (36)    NULL,
    [revenue]                           DECIMAL (26, 6) NULL,
    [revenue_base]                      DECIMAL (26, 6) NULL,
    [salesstage]                        INT             NULL,
    [salesstagecode]                    INT             NULL,
    [salesstagecodename]                NVARCHAR (255)  NULL,
    [salesstagename]                    NVARCHAR (255)  NULL,
    [salutation]                        NVARCHAR (4)    NULL,
    [schedulefollowup_prospect]         DATETIME        NULL,
    [schedulefollowup_qualify]          DATETIME        NULL,
    [sic]                               NVARCHAR (20)   NULL,
    [stageid]                           VARCHAR (36)    NULL,
    [statecode]                         INT             NULL,
    [statecodename]                     NVARCHAR (255)  NULL,
    [statuscode]                        INT             NULL,
    [statuscodename]                    NVARCHAR (255)  NULL,
    [subject]                           NVARCHAR (300)  NULL,
    [telephone1]                        NVARCHAR (50)   NULL,
    [telephone2]                        NVARCHAR (50)   NULL,
    [telephone3]                        NVARCHAR (50)   NULL,
    [timezoneruleversionnumber]         INT             NULL,
    [transactioncurrencyid]             VARCHAR (36)    NULL,
    [transactioncurrencyidname]         NVARCHAR (100)  NULL,
    [utcconversiontimezonecode]         INT             NULL,
    [versionnumber]                     BIGINT          NULL,
    [websiteurl]                        NVARCHAR (200)  NULL,
    [yomicompanyname]                   NVARCHAR (100)  NULL,
    [yomifirstname]                     NVARCHAR (150)  NULL,
    [yomifullname]                      NVARCHAR (450)  NULL,
    [yomilastname]                      NVARCHAR (150)  NULL,
    [yomimiddlename]                    NVARCHAR (150)  NULL,
    [InsertedDateTime]                  DATETIME        NULL,
    [InsertUser]                        VARCHAR (100)   NULL,
    [ltf_dncoverride]                   BIT             NULL,
    [ltf_dncoverridename]               NVARCHAR (255)  NULL,
    [ltf_duplicateoverride]             BIT             NULL,
    [ltf_duplicateoverridename]         NVARCHAR (255)  NULL,
    [ltf_inquirytype]                   NVARCHAR (100)  NULL,
    [ltf_manageduntil]                  DATETIME        NULL,
    [UpdatedDateTime]                   DATETIME        NULL,
    [UpdateUser]                        VARCHAR (50)    NULL,
    [ltf_webtransfermethod]             INT             NULL,
    [ltf_webtransfermethodname]         NVARCHAR (255)  NULL,
    [ltf_actualappointmentscheduled]    DATETIME        NULL,
    [ltf_actualfirstresponse]           DATETIME        NULL,
    [ltf_actualinitialcontact]          DATETIME        NULL,
    [ltf_appointmentshowed]             BIT             NULL,
    [ltf_appointmentshowedname]         NVARCHAR (255)  NULL,
    [ltf_besttimetocontact]             INT             NULL,
    [ltf_besttimetocontactname]         NVARCHAR (255)  NULL,
    [ltf_communicationconsent]          INT             NULL,
    [ltf_communicationconsentname]      NVARCHAR (255)  NULL,
    [ltf_consentdatetime]               DATETIME        NULL,
    [ltf_consentipaddress]              NVARCHAR (19)   NULL,
    [ltf_consenttext]                   NVARCHAR (300)  NULL,
    [ltf_dcmp]                          NVARCHAR (250)  NULL,
    [ltf_device]                        NVARCHAR (100)  NULL,
    [ltf_dnctemporaryreleaseexpiration] DATETIME        NULL,
    [ltf_dndoverrideexpiration]         DATETIME        NULL,
    [ltf_duplicateleadcount]            NVARCHAR (10)   NULL,
    [ltf_emailaddress1_umsk]            NVARCHAR (100)  NULL,
    [ltf_emailaddress2_umsk]            NVARCHAR (100)  NULL,
    [ltf_emailtemplate]                 NVARCHAR (110)  NULL,
    [ltf_employer]                      NVARCHAR (50)   NULL,
    [ltf_exacttargetemailsent]          BIT             NULL,
    [ltf_exacttargetemailsentname]      NVARCHAR (255)  NULL,
    [ltf_excludefromfollowup]           BIT             NULL,
    [ltf_excludefromfollowupname]       NVARCHAR (255)  NULL,
    [ltf_firstresponseby]               DATETIME        NULL,
    [ltf_firstresponsesent]             INT             NULL,
    [ltf_firstresponsesentname]         NVARCHAR (255)  NULL,
    [ltf_firstresponsestatus]           INT             NULL,
    [ltf_firstresponsestatusname]       NVARCHAR (255)  NULL,
    [ltf_gcid]                          NVARCHAR (250)  NULL,
    [ltf_gclid]                         NVARCHAR (250)  NULL,
    [ltf_group]                         INT             NULL,
    [ltf_groupname]                     NVARCHAR (255)  NULL,
    [ltf_initialcontactby]              DATETIME        NULL,
    [ltf_initialcontactmade]            INT             NULL,
    [ltf_initialcontactmadename]        NVARCHAR (255)  NULL,
    [ltf_initialcontactstatus]          INT             NULL,
    [ltf_initialcontactstatusname]      NVARCHAR (255)  NULL,
    [ltf_keywords]                      NVARCHAR (100)  NULL,
    [ltf_landingpage]                   NVARCHAR (100)  NULL,
    [ltf_lastsubmissiondate]            DATETIME        NULL,
    [ltf_memberid]                      NVARCHAR (100)  NULL,
    [ltf_mobilephone_umsk]              NVARCHAR (100)  NULL,
    [ltf_operatingsystem]               NVARCHAR (100)  NULL,
    [ltf_primarygoal]                   NVARCHAR (100)  NULL,
    [ltf_primarylead]                   VARCHAR (36)    NULL,
    [ltf_primaryleadname]               NVARCHAR (160)  NULL,
    [ltf_primaryleadyominame]           NVARCHAR (160)  NULL,
    [ltf_reactivationreason]            INT             NULL,
    [ltf_reactivationreasonname]        NVARCHAR (255)  NULL,
    [ltf_recommendedmembership]         INT             NULL,
    [ltf_recommendedmembershipname]     NVARCHAR (255)  NULL,
    [ltf_referringdomain]               NVARCHAR (100)  NULL,
    [ltf_referringpage]                 NVARCHAR (500)  NULL,
    [ltf_requesttype]                   NVARCHAR (100)  NULL,
    [ltf_scheduleapptby]                DATETIME        NULL,
    [ltf_scheduleapptstatus]            INT             NULL,
    [ltf_scheduleapptstatusname]        NVARCHAR (255)  NULL,
    [ltf_scheduleapptsent]              INT             NULL,
    [ltf_scheduleapptsentname]          NVARCHAR (255)  NULL,
    [ltf_telephone1_umsk]               NVARCHAR (100)  NULL,
    [ltf_telephone2_umsk]               NVARCHAR (100)  NULL,
    [ltf_transferredtoclubon]           DATETIME        NULL,
    [ltf_undereighteen]                 BIT             NULL,
    [ltf_undereighteenname]             NVARCHAR (255)  NULL,
    [ltf_utmaudience]                   NVARCHAR (100)  NULL,
    [ltf_utmcampaign]                   NVARCHAR (100)  NULL,
    [ltf_utmcontent]                    NVARCHAR (100)  NULL,
    [ltf_utmimage]                      NVARCHAR (100)  NULL,
    [ltf_utmmedium]                     NVARCHAR (100)  NULL,
    [ltf_utmsource]                     NVARCHAR (100)  NULL,
    [ltf_utmterm]                       NVARCHAR (100)  NULL,
    [ltf_visitcount]                    INT             NULL,
    [ltf_visitorid]                     NVARCHAR (40)   NULL,
    [ltf_webteamemail]                  NVARCHAR (100)  NULL,
    [relatedobjectidname]               NVARCHAR (200)  NULL,
    [traversedpath]                     NVARCHAR (1250) NULL,
    [ltf_promocode]                     NVARCHAR (100)  NULL,
    [ltf_updatecontact]                 BIT             NULL,
    [ltf_updatecontactname]             NVARCHAR (255)  NULL,
    [ltf_assignedtoclubdate]            DATETIME        NULL,
    [ltf_dncdneupdatetriggeredby]       INT             NULL,
    [ltf_dncdneupdatetriggeredbyname]   NVARCHAR (255)  NULL,
    [ltf_imsjoinlink]                   NVARCHAR (300)  NULL,
    [ltf_initialemailsubject]           NVARCHAR (250)  NULL,
    [ltf_isimsjoin]                     BIT             NULL,
    [ltf_islifetimecloseto]             INT             NULL,
    [ltf_islifetimeclosetoname]         NVARCHAR (255)  NULL,
    [ltf_leadinitialload]               BIT             NULL,
    [ltf_leadinitialloadname]           NVARCHAR (255)  NULL,
    [ltf_selectedinterestids]           NVARCHAR (1000) NULL,
    [ltf_triggerleadqualify]            BIT             NULL,
    [ltf_triggerleadqualifyname]        NVARCHAR (255)  NULL,
    [ltf_webteamphone]                  NVARCHAR (100)  NULL,
    [ltf_originatingguestvisit]         VARCHAR (36)    NULL,
    [ltf_originatingguestvisitname]     NVARCHAR (200)  NULL,
    [ltf_originatingchat]               VARCHAR (36)    NULL,
    [ltf_originatingchatname]           NVARCHAR (2000) NULL,
    [ltf_mostrecentcasl]                DATETIME        NULL,
    [ltf_referringcorpacctid]           NVARCHAR (100)  NULL,
    [ltf_referringcorpacct]             VARCHAR (36)    NULL,
    [ltf_lineofbusiness]                INT             NULL,
    [ltf_channel]                       INT             NULL,
    [ltf_lineofbusinessname]            NVARCHAR (255)  NULL,
    [ltf_channelname]                   NVARCHAR (255)  NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
