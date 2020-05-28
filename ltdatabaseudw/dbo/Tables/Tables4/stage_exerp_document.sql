CREATE TABLE [dbo].[stage_exerp_document] (
    [stage_exerp_document_id] BIGINT         NOT NULL,
    [id]                      INT            NULL,
    [person_id]               VARCHAR (4000) NULL,
    [company_id]              VARCHAR (4000) NULL,
    [creation_datetime]       DATETIME       NULL,
    [type]                    VARCHAR (4000) NULL,
    [subject]                 VARCHAR (4000) NULL,
    [details]                 VARCHAR (4000) NULL,
    [creator_person_id]       VARCHAR (4000) NULL,
    [require_signature]       BIT            NULL,
    [signatures_signed]       INT            NULL,
    [signatures_missing]      INT            NULL,
    [latest_signed_datetime]  DATETIME       NULL,
    [attached_file_name]      VARCHAR (4000) NULL,
    [center_id]               INT            NULL,
    [ets]                     BIGINT         NULL,
    [dv_batch_id]             BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

