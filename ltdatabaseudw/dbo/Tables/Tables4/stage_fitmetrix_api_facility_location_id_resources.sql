CREATE TABLE [dbo].[stage_fitmetrix_api_facility_location_id_resources] (
    [stage_fitmetrix_api_facility_location_id_resources_id] BIGINT          NOT NULL,
    [FACILITYLOCATIONRESOURCEID]                            INT             NULL,
    [FACILITYLOCATIONID]                                    INT             NULL,
    [MAXCAPACITY]                                           INT             NULL,
    [NAME]                                                  VARCHAR (255)   NULL,
    [EXTERNALID]                                            VARCHAR (255)   NULL,
    [CONFIGURATION]                                         VARCHAR (8000)  NULL,
    [USEINTERVALS]                                          VARCHAR (255)   NULL,
    [DEFAULTACTIVITYTYPEID]                                 INT             NULL,
    [ADDRESS]                                               VARCHAR (255)   NULL,
    [LAT]                                                   INT             NULL,
    [LONG]                                                  INT             NULL,
    [EXTERNALID_base64_decoded]                             VARBINARY (255) NULL,
    [dummy_modified_date_time]                              DATETIME        NULL,
    [dv_batch_id]                                           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

