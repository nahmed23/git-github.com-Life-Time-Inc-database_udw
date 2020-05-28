CREATE TABLE [dbo].[stage_mms_MIPCategoryItem] (
    [stage_mms_MIPCategoryItem_id] BIGINT   NOT NULL,
    [MIPCategoryItemID]            INT      NULL,
    [ValMIPCategoryID]             SMALLINT NULL,
    [ValMIPSubCategoryID]          SMALLINT NULL,
    [ValMIPItemID]                 SMALLINT NULL,
    [ActiveFlag]                   BIT      NULL,
    [AllowCommentFlag]             BIT      NULL,
    [SortOrder]                    SMALLINT NULL,
    [InsertedDateTime]             DATETIME NULL,
    [UpdatedDateTime]              DATETIME NULL,
    [ValMIPInterestCategoryID]     SMALLINT NULL,
    [ProspectEnabledFlag]          BIT      NULL,
    [dv_batch_id]                  BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

