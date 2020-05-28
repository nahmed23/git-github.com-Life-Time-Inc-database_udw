CREATE PROC [dbo].[proc_dim_boss_product_tagging] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON


	IF object_id('tempdb..#etl_step1') IS NOT NULL
		DROP TABLE #etl_step1

	CREATE TABLE dbo.#etl_step1
		WITH (
				distribution = HASH (dim_boss_product_tagging_key)
				,location = user_db
				) AS
select  d_boss_taggings.bk_hash dim_boss_product_tagging_key,
        d_boss_tags.tags_id tags_id,
	    d_boss_tags.tag_type tag_type,
	    d_boss_tags.tag_name tag_name,
	    d_boss_taggings.taggings_id taggings_id,
	    d_boss_taggings.taggable_id taggable_id,
	    d_boss_asi_invtr.dim_boss_product_key dim_boss_product_key,
	    CASE 
			WHEN isnull(d_boss_taggings.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_tags.dv_load_date_time, 'Jan 1, 1753')
			    AND isnull(d_boss_taggings.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_asi_invtr.dv_load_date_time, 'Jan 1, 1753')
				THEN isnull(d_boss_taggings.dv_load_date_time, 'Jan 1, 1753')
			WHEN isnull(d_boss_tags.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_asi_invtr.dv_load_date_time, 'Jan 1, 1753')
				THEN isnull(d_boss_tags.dv_load_date_time, 'Jan 1, 1753')
			 ELSE isnull(d_boss_asi_invtr.dv_load_date_time, 'Jan 1, 1753')
			END dv_load_date_time
		,convert(DATETIME, '99991231', 112) dv_load_end_date_time
		, CASE 
			WHEN isnull(d_boss_taggings.dv_batch_id, -1) >= isnull(d_boss_tags.dv_batch_id, -1)
			    AND isnull(d_boss_taggings.dv_batch_id, -1) >= isnull(d_boss_asi_invtr.dv_batch_id, -1)
				THEN isnull(d_boss_taggings.dv_batch_id, -1)
			WHEN isnull(d_boss_tags.dv_batch_id, - 1) >= isnull(d_boss_asi_invtr.dv_batch_id, - 1)
				THEN isnull(d_boss_tags.dv_batch_id, - 1)
			 ELSE isnull(d_boss_asi_invtr.dv_batch_id, - 1)
			END dv_batch_id
    from [dbo].[d_boss_taggings]
  join [dbo].[d_boss_tags]
    on [d_boss_taggings].d_boss_tags_bk_hash = [d_boss_tags].bk_hash
  join [dbo].[d_boss_asi_invtr]
    on case when d_boss_taggings.bk_hash in ('-997','-998','-999') then d_boss_taggings.bk_hash else d_boss_taggings.taggable_id end
       = case when d_boss_asi_invtr.bk_hash in ('-997','-998','-999') then d_boss_asi_invtr.bk_hash else d_boss_asi_invtr.invtr_id end
  where [d_boss_taggings].[taggable_type] = 'BOSS::Model::Product'
  


  
	--   Delete records from the table that exist
	--   Insert records from records from current and missing batches
	BEGIN TRAN

	DELETE dbo.dim_boss_product_tagging
	WHERE dim_boss_product_tagging_key IN (
			SELECT dim_boss_product_tagging_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_boss_product_tagging (
         dim_boss_product_tagging_key
	    ,tags_id
        ,tag_type
        ,tag_name
        ,taggings_id
        ,taggable_id
		,dim_boss_product_key
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT dim_boss_product_tagging_key
	    ,tags_id
        ,tag_type
        ,tag_name
        ,taggings_id
        ,taggable_id
		,dim_boss_product_key
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN

			
END
 
