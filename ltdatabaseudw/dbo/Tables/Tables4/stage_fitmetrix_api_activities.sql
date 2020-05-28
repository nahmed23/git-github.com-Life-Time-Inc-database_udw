﻿CREATE TABLE [dbo].[stage_fitmetrix_api_activities] (
    [stage_fitmetrix_api_activities_id] BIGINT        NOT NULL,
    [ACTIVITYID]                        INT           NULL,
    [ACTIVITYNAME]                      VARCHAR (255) NULL,
    [ACTIVITYADDED]                     VARCHAR (255) NULL,
    [EXTERNALID]                        VARCHAR (255) NULL,
    [LEVEL]                             VARCHAR (255) NULL,
    [FACILITYID]                        INT           NULL,
    [ACTIVITYTYPEID]                    INT           NULL,
    [NEEDLOANERS]                       VARCHAR (255) NULL,
    [CHECKINTYPE]                       VARCHAR (255) NULL,
    [ALLOWRESERVATION]                  VARCHAR (255) NULL,
    [NOINTEGRATIONSYNC]                 VARCHAR (255) NULL,
    [ICON]                              VARCHAR (255) NULL,
    [ISASSESSMENT]                      VARCHAR (255) NULL,
    [IMAGE]                             VARCHAR (255) NULL,
    [ISDELETED]                         VARCHAR (255) NULL,
    [ISAPPOINTMENTACTIVITY]             VARCHAR (255) NULL,
    [APPNAME]                           VARCHAR (255) NULL,
    [POSITION]                          INT           NULL,
    [DELETE]                            VARCHAR (255) NULL,
    [APPIMAGE]                          VARCHAR (255) NULL,
    [APPICON]                           VARCHAR (255) NULL,
    [ISMANUALATTENDANCE]                VARCHAR (255) NULL,
    [DURATIONMINUTES]                   INT           NULL,
    [dummy_modified_date_time]          DATETIME      NULL,
    [dv_batch_id]                       BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

