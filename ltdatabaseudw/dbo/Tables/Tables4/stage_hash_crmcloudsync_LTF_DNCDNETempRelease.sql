﻿CREATE TABLE [dbo].[stage_hash_crmcloudsync_LTF_DNCDNETempRelease] (
    [stage_hash_crmcloudsync_LTF_DNCDNETempRelease_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)      NOT NULL,
    [createdby]                                        VARCHAR (36)   NULL,
    [createdbyname]                                    NVARCHAR (200) NULL,
    [createdbyyominame]                                NVARCHAR (200) NULL,
    [createdon]                                        DATETIME       NULL,
    [createdonbehalfby]                                VARCHAR (36)   NULL,
    [createdonbehalfbyname]                            NVARCHAR (200) NULL,
    [createdonbehalfbyyominame]                        NVARCHAR (200) NULL,
    [importsequencenumber]                             INT            NULL,
    [ltf_dncdnetempreleaseid]                          VARCHAR (36)   NULL,
    [ltf_expirationdate]                               DATETIME       NULL,
    [ltf_value]                                        NVARCHAR (100) NULL,
    [modifiedby]                                       VARCHAR (36)   NULL,
    [modifiedbyname]                                   NVARCHAR (200) NULL,
    [modifiedbyyominame]                               NVARCHAR (200) NULL,
    [modifiedon]                                       DATETIME       NULL,
    [modifiedonbehalfby]                               VARCHAR (36)   NULL,
    [modifiedonbehalfbyname]                           NVARCHAR (200) NULL,
    [modifiedonbehalfbyyominame]                       NVARCHAR (200) NULL,
    [organizationid]                                   VARCHAR (36)   NULL,
    [organizationidname]                               NVARCHAR (160) NULL,
    [overriddencreatedon]                              DATETIME       NULL,
    [statecode]                                        INT            NULL,
    [statecodename]                                    NVARCHAR (255) NULL,
    [statuscode]                                       INT            NULL,
    [statuscodename]                                   NVARCHAR (255) NULL,
    [timezoneruleversionnumber]                        INT            NULL,
    [utcconversiontimezonecode]                        INT            NULL,
    [versionnumber]                                    BIGINT         NULL,
    [InsertedDateTime]                                 DATETIME       NULL,
    [InsertUser]                                       VARCHAR (100)  NULL,
    [UpdatedDateTime]                                  DATETIME       NULL,
    [UpdateUser]                                       VARCHAR (50)   NULL,
    [dv_load_date_time]                                DATETIME       NOT NULL,
    [dv_inserted_date_time]                            DATETIME       NOT NULL,
    [dv_insert_user]                                   VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                             DATETIME       NULL,
    [dv_update_user]                                   VARCHAR (50)   NULL,
    [dv_batch_id]                                      BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

