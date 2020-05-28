CREATE TABLE [dbo].[stage_hash_mms_Employee] (
    [stage_hash_mms_Employee_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)    NOT NULL,
    [EmployeeID]                 INT          NULL,
    [ClubID]                     INT          NULL,
    [ActiveStatusFlag]           BIT          NULL,
    [FirstName]                  VARCHAR (50) NULL,
    [LastName]                   VARCHAR (50) NULL,
    [MiddleInt]                  VARCHAR (3)  NULL,
    [InsertedDateTime]           DATETIME     NULL,
    [MemberID]                   INT          NULL,
    [UpdatedDateTime]            DATETIME     NULL,
    [HireDate]                   DATETIME     NULL,
    [TerminationDate]            DATETIME     NULL,
    [dv_load_date_time]          DATETIME     NOT NULL,
    [dv_inserted_date_time]      DATETIME     NOT NULL,
    [dv_insert_user]             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]       DATETIME     NULL,
    [dv_update_user]             VARCHAR (50) NULL,
    [dv_batch_id]                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

