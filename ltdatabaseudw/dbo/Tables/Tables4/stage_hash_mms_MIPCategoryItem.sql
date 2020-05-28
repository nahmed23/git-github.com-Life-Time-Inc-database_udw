CREATE TABLE [dbo].[stage_hash_mms_MIPCategoryItem] (
    [stage_hash_mms_MIPCategoryItem_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)    NOT NULL,
    [MIPCategoryItemID]                 INT          NULL,
    [ValMIPCategoryID]                  SMALLINT     NULL,
    [ValMIPSubCategoryID]               SMALLINT     NULL,
    [ValMIPItemID]                      SMALLINT     NULL,
    [ActiveFlag]                        BIT          NULL,
    [AllowCommentFlag]                  BIT          NULL,
    [SortOrder]                         SMALLINT     NULL,
    [InsertedDateTime]                  DATETIME     NULL,
    [UpdatedDateTime]                   DATETIME     NULL,
    [ValMIPInterestCategoryID]          SMALLINT     NULL,
    [ProspectEnabledFlag]               BIT          NULL,
    [dv_load_date_time]                 DATETIME     NOT NULL,
    [dv_inserted_date_time]             DATETIME     NOT NULL,
    [dv_insert_user]                    VARCHAR (50) NOT NULL,
    [dv_updated_date_time]              DATETIME     NULL,
    [dv_update_user]                    VARCHAR (50) NULL,
    [dv_batch_id]                       BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

