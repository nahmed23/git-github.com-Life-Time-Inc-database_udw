CREATE TABLE [dbo].[stage_hash_fitmetrix_api_facility_location_id_resources] (
    [stage_hash_fitmetrix_api_facility_location_id_resources_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                                    CHAR (32)       NOT NULL,
    [FACILITYLOCATIONRESOURCEID]                                 INT             NULL,
    [FACILITYLOCATIONID]                                         INT             NULL,
    [MAXCAPACITY]                                                INT             NULL,
    [NAME]                                                       VARCHAR (255)   NULL,
    [EXTERNALID]                                                 VARCHAR (255)   NULL,
    [CONFIGURATION]                                              VARCHAR (8000)  NULL,
    [USEINTERVALS]                                               VARCHAR (255)   NULL,
    [DEFAULTACTIVITYTYPEID]                                      INT             NULL,
    [ADDRESS]                                                    VARCHAR (255)   NULL,
    [LAT]                                                        INT             NULL,
    [LONG]                                                       INT             NULL,
    [EXTERNALID_base64_decoded]                                  VARBINARY (255) NULL,
    [dummy_modified_date_time]                                   DATETIME        NULL,
    [dv_load_date_time]                                          DATETIME        NOT NULL,
    [dv_updated_date_time]                                       DATETIME        NULL,
    [dv_update_user]                                             VARCHAR (50)    NULL,
    [dv_batch_id]                                                BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

