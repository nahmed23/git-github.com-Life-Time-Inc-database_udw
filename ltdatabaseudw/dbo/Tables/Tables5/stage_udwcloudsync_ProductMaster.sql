﻿CREATE TABLE [dbo].[stage_udwcloudsync_ProductMaster] (
    [stage_udwcloudsync_ProductMaster_id]         BIGINT          NOT NULL,
    [AppCreatedBy]                                NVARCHAR (4000) NULL,
    [AppModifiedBy]                               NVARCHAR (4000) NULL,
    [AssessJuniorDuesFlag]                        NVARCHAR (4000) NULL,
    [Attachments]                                 NVARCHAR (4000) NULL,
    [ConnectivityLeadGenerator]                   NVARCHAR (4000) NULL,
    [ConnectivityPrimaryLeadGeneratorFlag]        NVARCHAR (4000) NULL,
    [ContentType]                                 NVARCHAR (4000) NULL,
    [CorporateTransferFlag]                       NVARCHAR (4000) NULL,
    [CorporateTransferMultiplier]                 NVARCHAR (4000) NULL,
    [Created]                                     DATETIME        NULL,
    [CreatedBy]                                   NVARCHAR (4000) NULL,
    [DeferredRevenueFlag]                         NVARCHAR (4000) NULL,
    [DepartmentalDSSRFlag]                        NVARCHAR (4000) NULL,
    [Discount1Description]                        NVARCHAR (4000) NULL,
    [Discount1EffectiveFromDate]                  NVARCHAR (4000) NULL,
    [Discount1EffectiveThroughDate]               NVARCHAR (4000) NULL,
    [Discount1SalesCommissionPercent]             NVARCHAR (4000) NULL,
    [Discount1ServiceCommissionPercent]           NVARCHAR (4000) NULL,
    [Discount2Description]                        NVARCHAR (4000) NULL,
    [Discount2EffectiveFromDate]                  NVARCHAR (4000) NULL,
    [Discount2EffectiveThroughDate]               NVARCHAR (4000) NULL,
    [Discount2SalesCommissionPercent]             NVARCHAR (4000) NULL,
    [Discount2ServiceCommissionPercent]           NVARCHAR (4000) NULL,
    [Discount3Description]                        NVARCHAR (4000) NULL,
    [Discount3EffectiveFromDate]                  NVARCHAR (4000) NULL,
    [Discount3EffectiveThroughDate]               NVARCHAR (4000) NULL,
    [Discount3SalesCommissionPercent]             NVARCHAR (4000) NULL,
    [Discount3ServiceCommissionPercent]           NVARCHAR (4000) NULL,
    [Discount4Description]                        NVARCHAR (4000) NULL,
    [Discount4EffectiveFromDate]                  NVARCHAR (4000) NULL,
    [Discount4EffectiveThroughDate]               NVARCHAR (4000) NULL,
    [Discount4SalesCommissionPercent]             NVARCHAR (4000) NULL,
    [Discount4ServiceCommissionPercent]           NVARCHAR (4000) NULL,
    [Discount5Description]                        NVARCHAR (4000) NULL,
    [Discount5EffectiveFromDate]                  NVARCHAR (4000) NULL,
    [Discount5EffectiveThroughDate]               NVARCHAR (4000) NULL,
    [Discount5SalesCommissionPercent]             NVARCHAR (4000) NULL,
    [Discount5ServiceCommissionPercent]           NVARCHAR (4000) NULL,
    [Division]                                    NVARCHAR (4000) NULL,
    [DSSRDowngradeOtherEnrollmentFeeFlag]         NVARCHAR (4000) NULL,
    [DSSRIFAdminFeeFlag]                          NVARCHAR (4000) NULL,
    [ECommerceOfferFlag]                          NVARCHAR (4000) NULL,
    [Edit]                                        NVARCHAR (4000) NULL,
    [ExperienceLifeMagazineFlag]                  NVARCHAR (4000) NULL,
    [FolderChildCount]                            NVARCHAR (4000) NULL,
    [ID]                                          INT             NULL,
    [ItemChildCount]                              NVARCHAR (4000) NULL,
    [MMSDepartment]                               NVARCHAR (4000) NULL,
    [MMSPackageProductFlag]                       NVARCHAR (4000) NULL,
    [MMSProductDisplayUIFlag]                     NVARCHAR (4000) NULL,
    [MMSProductGLOverrideClubID]                  NVARCHAR (4000) NULL,
    [MMSProductTipAllowedFlag]                    NVARCHAR (4000) NULL,
    [MMSRecurrentProductTypeDescription]          NVARCHAR (4000) NULL,
    [Modified]                                    DATETIME        NULL,
    [ModifiedBy]                                  NVARCHAR (4000) NULL,
    [MTDAverageDeliveredSessionPrice]             NVARCHAR (4000) NULL,
    [MTDAverageSalePrice]                         NVARCHAR (4000) NULL,
    [NewBusinessOldBusiness]                      NVARCHAR (4000) NULL,
    [PackageProductCountasHalfSessionFlag]        NVARCHAR (4000) NULL,
    [PackageProductSessionType]                   NVARCHAR (4000) NULL,
    [PayrollExtractDescription]                   NVARCHAR (4000) NULL,
    [PayrollExtractRegionType]                    NVARCHAR (4000) NULL,
    [PayrollmyLTBucksProductGroupDescription]     NVARCHAR (4000) NULL,
    [PayrollmyLTBucksProductGroupFlag]            NVARCHAR (4000) NULL,
    [PayrollmyLTBucksProductGroupSortOrder]       NVARCHAR (4000) NULL,
    [PayrollmyLTBucksSalesAmountFlag]             NVARCHAR (4000) NULL,
    [PayrollmyLTBucksServiceAmountFlag]           NVARCHAR (4000) NULL,
    [PayrollmyLTBucksServiceQuantityFlag]         NVARCHAR (4000) NULL,
    [PayrollProductGroupDescription]              NVARCHAR (4000) NULL,
    [PayrollProductGroupSortOrder]                NVARCHAR (4000) NULL,
    [PayrollSalesAmountFlag]                      NVARCHAR (4000) NULL,
    [PayrollServiceAmountFlag]                    NVARCHAR (4000) NULL,
    [PayrollServiceQuantityFlag]                  NVARCHAR (4000) NULL,
    [PayrollStandardProductGroupFlag]             NVARCHAR (4000) NULL,
    [PayrollTrackSalesFlag]                       NVARCHAR (4000) NULL,
    [PayrollTrackServiceFlag]                     NVARCHAR (4000) NULL,
    [ProductDescription]                          NVARCHAR (4000) NULL,
    [ProductDiscountGLAccount]                    NVARCHAR (4000) NULL,
    [ProductGLAccount]                            NVARCHAR (4000) NULL,
    [ProductGLDepartmentCode]                     NVARCHAR (4000) NULL,
    [ProductGLProductCode]                        NVARCHAR (4000) NULL,
    [ProductID]                                   NVARCHAR (4000) NULL,
    [ProductRefundGLAccount]                      NVARCHAR (4000) NULL,
    [ProductSKU]                                  NVARCHAR (4000) NULL,
    [ProductStatus]                               NVARCHAR (4000) NULL,
    [ProductWorkdayAccount]                       NVARCHAR (4000) NULL,
    [ProductWorkdayCostCenter]                    NVARCHAR (4000) NULL,
    [ProductWorkdayDiscountGLAccount]             NVARCHAR (4000) NULL,
    [ProductWorkdayOffering]                      NVARCHAR (4000) NULL,
    [ProductWorkdayOverRideRegion]                NVARCHAR (4000) NULL,
    [ProductWorkdayRefundGLAccount]               NVARCHAR (4000) NULL,
    [ReportingDept]                               NVARCHAR (4000) NULL,
    [ReportingDeptForNonCommissionedSales]        NVARCHAR (4000) NULL,
    [RevenueAllocationRule]                       NVARCHAR (4000) NULL,
    [RevenueProductGroupDescription]              NVARCHAR (4000) NULL,
    [RevenueProductGroupDiscountGLAccount]        NVARCHAR (4000) NULL,
    [RevenueProductGroupGLAccount]                NVARCHAR (4000) NULL,
    [RevenueProductGroupRefundGLAccount]          NVARCHAR (4000) NULL,
    [RevenueProductGroupSortOrder]                NVARCHAR (4000) NULL,
    [RevenueReportingRegionType]                  NVARCHAR (4000) NULL,
    [SalesCategoryDescription]                    NVARCHAR (4000) NULL,
    [SourceSystem_LinkTitle]                      NVARCHAR (4000) NULL,
    [SourceSystem_LinkTitleNoMenu]                NVARCHAR (4000) NULL,
    [SourceSystem_Title]                          NVARCHAR (4000) NULL,
    [Subdivision]                                 NVARCHAR (4000) NULL,
    [Type]                                        NVARCHAR (4000) NULL,
    [Version]                                     NVARCHAR (4000) NULL,
    [VirtualLocalRelativePath]                    NVARCHAR (4000) NULL,
    [WorkdayRevenueProductGroupAccount]           NVARCHAR (4000) NULL,
    [WorkdayRevenueProductGroupDiscountGLAccount] NVARCHAR (4000) NULL,
    [WorkdayRevenueProductGroupRefundGLAccount]   NVARCHAR (4000) NULL,
    [dv_batch_id]                                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
