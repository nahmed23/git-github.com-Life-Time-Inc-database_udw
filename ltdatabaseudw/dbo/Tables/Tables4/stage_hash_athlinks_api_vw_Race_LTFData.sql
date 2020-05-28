﻿CREATE TABLE [dbo].[stage_hash_athlinks_api_vw_Race_LTFData] (
    [stage_hash_athlinks_api_vw_Race_LTFData_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)       NOT NULL,
    [RaceID]                                     INT             NULL,
    [RaceName]                                   NVARCHAR (256)  NULL,
    [RaceDate]                                   DATETIME        NULL,
    [RaceEndDate]                                DATETIME        NULL,
    [City]                                       NVARCHAR (128)  NULL,
    [StateProvID]                                NVARCHAR (8)    NULL,
    [StateProvName]                              NVARCHAR (70)   NULL,
    [StateProvAbbrev]                            NVARCHAR (8)    NULL,
    [CountryID]                                  CHAR (2)        NULL,
    [CountryID3]                                 CHAR (3)        NULL,
    [CountryName]                                NVARCHAR (64)   NULL,
    [RaceCompanyID]                              INT             NULL,
    [DateSort]                                   VARCHAR (8)     NULL,
    [WebSite]                                    NVARCHAR (256)  NULL,
    [Status]                                     INT             NULL,
    [Elevation]                                  INT             NULL,
    [MasterID]                                   INT             NULL,
    [ResultCount]                                INT             NULL,
    [Latitude]                                   DECIMAL (26, 6) NULL,
    [Longitude]                                  DECIMAL (26, 6) NULL,
    [Temperature]                                INT             NULL,
    [WeatherNotes]                               NVARCHAR (512)  NULL,
    [CreateDate]                                 DATETIME        NULL,
    [dv_load_date_time]                          DATETIME        NOT NULL,
    [dv_batch_id]                                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

