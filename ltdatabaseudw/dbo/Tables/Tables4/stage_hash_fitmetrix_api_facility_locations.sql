﻿CREATE TABLE [dbo].[stage_hash_fitmetrix_api_facility_locations] (
    [stage_hash_fitmetrix_api_facility_locations_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                        CHAR (32)      NOT NULL,
    [FACILITYLOCATIONID]                             INT            NULL,
    [FACILITYID]                                     INT            NULL,
    [STREET1]                                        VARCHAR (255)  NULL,
    [CITY]                                           VARCHAR (255)  NULL,
    [STATE]                                          VARCHAR (255)  NULL,
    [ZIP]                                            VARCHAR (255)  NULL,
    [COUNTRY]                                        VARCHAR (255)  NULL,
    [PHONE]                                          VARCHAR (255)  NULL,
    [STREET2]                                        VARCHAR (255)  NULL,
    [HOURS]                                          VARCHAR (255)  NULL,
    [EMAIL]                                          VARCHAR (255)  NULL,
    [MANAGER]                                        VARCHAR (255)  NULL,
    [EXTERNALID]                                     VARCHAR (255)  NULL,
    [DESCRIPTION]                                    VARCHAR (255)  NULL,
    [LATITUDE]                                       VARCHAR (255)  NULL,
    [LONGITUDE]                                      VARCHAR (255)  NULL,
    [SERVERTIMEOFFSET]                               INT            NULL,
    [NAME]                                           VARCHAR (255)  NULL,
    [PHONEEXT]                                       VARCHAR (255)  NULL,
    [BOOKINGURL]                                     VARCHAR (255)  NULL,
    [CHECKOUTURL]                                    VARCHAR (255)  NULL,
    [RATINGURL]                                      VARCHAR (255)  NULL,
    [SOCIALURL]                                      VARCHAR (255)  NULL,
    [CLASSDETAILURL]                                 VARCHAR (255)  NULL,
    [LOCATIONURL]                                    VARCHAR (255)  NULL,
    [EMAILFROMNAME]                                  VARCHAR (255)  NULL,
    [HIDEINPORTAL]                                   VARCHAR (255)  NULL,
    [DATEFORMAT]                                     VARCHAR (255)  NULL,
    [LOCATIONBOOKINGWINDOW]                          INT            NULL,
    [DISPLAYORDER]                                   INT            NULL,
    [ICSENABLED]                                     VARCHAR (255)  NULL,
    [BOOKINGCONVERSION]                              VARCHAR (255)  NULL,
    [PURCHASECONVERSION]                             VARCHAR (255)  NULL,
    [MAILCHIMPAPIKEY]                                VARCHAR (255)  NULL,
    [MAILCHIMPLISTID]                                VARCHAR (255)  NULL,
    [MAILCHIMPENABLED]                               VARCHAR (255)  NULL,
    [PACKAGEHEADER]                                  VARCHAR (255)  NULL,
    [PACKAGEFOOTER]                                  VARCHAR (255)  NULL,
    [SUNDAYHOURS]                                    VARCHAR (255)  NULL,
    [MONDAYHOURS]                                    VARCHAR (255)  NULL,
    [TUESDAYHOURS]                                   VARCHAR (255)  NULL,
    [WEDNESDAYHOURS]                                 VARCHAR (255)  NULL,
    [THURSDAYHOURS]                                  VARCHAR (255)  NULL,
    [FRIDAYHOURS]                                    VARCHAR (255)  NULL,
    [SATURDAYHOURS]                                  VARCHAR (255)  NULL,
    [TWITTERURL]                                     VARCHAR (255)  NULL,
    [FACEBOOKURL]                                    VARCHAR (255)  NULL,
    [CHECKOUTHEADER]                                 VARCHAR (255)  NULL,
    [CHECKOUTFOOTER]                                 VARCHAR (255)  NULL,
    [CHECKOUTCALLOUT]                                VARCHAR (255)  NULL,
    [PICKASPOTHEADER]                                VARCHAR (4000) NULL,
    [PICKASPOTFOOTER]                                VARCHAR (4000) NULL,
    [PICKASPOTCALLOUT]                               VARCHAR (255)  NULL,
    [ANNOUNCEMENTTITLE]                              VARCHAR (255)  NULL,
    [ANNOUNCEMENTBODY]                               VARCHAR (255)  NULL,
    [ANNOUNCEMENTLINK]                               VARCHAR (255)  NULL,
    [ANNOUNCEMENTLINKTEXT]                           VARCHAR (255)  NULL,
    [ANNOUNCEMENT]                                   VARCHAR (255)  NULL,
    [BOOKINGNOTES]                                   VARCHAR (255)  NULL,
    [STRIPEAPIKEY]                                   VARCHAR (255)  NULL,
    [INSTAGRAMURL]                                   VARCHAR (255)  NULL,
    [ISAPPROVAL]                                     VARCHAR (255)  NULL,
    [SENDSUBTEXTMESSAGES]                            VARCHAR (255)  NULL,
    [NOTIFYPARTICIPANTS]                             VARCHAR (255)  NULL,
    [EXTERNALID2]                                    VARCHAR (255)  NULL,
    [CRMCLUBID]                                      VARCHAR (255)  NULL,
    [NOTIFYMEMBERSOFFAVINSTRUCTORSUB]                VARCHAR (255)  NULL,
    [FacilityLocationActivities]                     VARCHAR (255)  NULL,
    [APPSCHEDULEURL]                                 VARCHAR (255)  NULL,
    [REDIRECTAPPID]                                  VARCHAR (255)  NULL,
    [GUESTPASSLIMIT]                                 INT            NULL,
    [dummy_modified_date_time]                       DATETIME       NULL,
    [dv_load_date_time]                              DATETIME       NOT NULL,
    [dv_inserted_date_time]                          DATETIME       NOT NULL,
    [dv_insert_user]                                 VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                           DATETIME       NULL,
    [dv_update_user]                                 VARCHAR (50)   NULL,
    [dv_batch_id]                                    BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

