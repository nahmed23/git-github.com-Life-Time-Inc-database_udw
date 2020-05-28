CREATE TABLE [dbo].[s_humanity_overtime_hours] (
    [s_humanity_overtime_hours_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)     NOT NULL,
    [userid]                       BIGINT        NULL,
    [employee_id]                  VARCHAR (255) NULL,
    [date_formatted]               VARCHAR (255) NULL,
    [hours_regular]                VARCHAR (255) NULL,
    [hours_overtime]               VARCHAR (255) NULL,
    [hours_d_overtime]             VARCHAR (255) NULL,
    [hours_position_id]            VARCHAR (255) NULL,
    [hours_location_id]            VARCHAR (255) NULL,
    [company_id]                   VARCHAR (255) NULL,
    [start_time]                   VARCHAR (255) NULL,
    [end_time]                     VARCHAR (255) NULL,
    [ltf_file_name]                VARCHAR (255) NULL,
    [employee_name]                VARCHAR (255) NULL,
    [dummy_modified_date_time]     DATETIME      NULL,
    [dv_load_date_time]            DATETIME      NOT NULL,
    [dv_r_load_source_id]          BIGINT        NOT NULL,
    [dv_inserted_date_time]        DATETIME      NOT NULL,
    [dv_insert_user]               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]         DATETIME      NULL,
    [dv_update_user]               VARCHAR (50)  NULL,
    [dv_hash]                      CHAR (32)     NOT NULL,
    [dv_deleted]                   BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                  BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

