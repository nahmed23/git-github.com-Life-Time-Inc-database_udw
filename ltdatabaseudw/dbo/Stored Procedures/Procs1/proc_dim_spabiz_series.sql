CREATE PROC [dbo].[proc_dim_spabiz_series] @dv_batch_id [varchar](500) AS
Begin
set xact_abort on
set nocount on

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
    @dv_batch_id as current_dv_batch_id
    from dbo.dim_spabiz_series

--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 1~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~	
--For a dimension record, the complete record needs to be rebuilt for a change in any field in any of the participating tables, Hence:
-----STEP 1: Collecting Business Keys from the base table - that are corresponding to the changed Recs from all the participating tables & itself
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if object_id('tempdb..#Business_keys') is not null drop table #Business_keys
create table dbo.#Business_keys with(distribution=hash(dim_spabiz_series_key), location=user_db, heap) as
select dim_spabiz_series_key
from (select p_spabiz_series.bk_hash dim_spabiz_series_key 
        from p_spabiz_series
        join #dv_batch_id 
		  on (p_spabiz_series.dv_batch_id > #dv_batch_id.max_dv_batch_id
		      or p_spabiz_series.dv_batch_id = #dv_batch_id.current_dv_batch_id)
        join l_spabiz_series 
          on p_spabiz_series.l_spabiz_series_id = l_spabiz_series.l_spabiz_series_id
	     and isnull(l_spabiz_series.store_number,999999999) not in (1,100,999)
	   where p_spabiz_series.dv_load_end_date_time = 'Dec 31, 9999') Business_keys_Unioned
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~END OF STEP 1: BUSINESS KEY COLLECTION~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~




--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 2:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
---STEP 2: Preparing the required fields to build the dimension table from the individual participating tables--------
---i.e. Business keys collected in "STEP 1" drives collection of records from each participating table!
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 	  
	  
if object_id('tempdb..#spabiz_series') is not null drop table #spabiz_series
create table dbo.#spabiz_series with(distribution=hash(dim_spabiz_series_key), location=user_db, heap) as
 select 
          p_spabiz_series.bk_hash dim_spabiz_series_key,
          p_spabiz_series.series_id series_id,
          p_spabiz_series.store_number store_number, 
          case when p_spabiz_series.bk_hash in ('-997','-998','-999') then null
               when s_spabiz_series.delete_date = convert(date, '18991230', 112) then null
               else s_spabiz_series.delete_date
          end deleted_date_time,
		  case when p_spabiz_series.bk_hash in ('-997','-998','-999') then 'N'
               when s_spabiz_series.series_delete = -1 then 'Y'
               else 'N'
          end deleted_flag,
		  case when p_spabiz_series.bk_hash in ('-997','-998','-999') then p_spabiz_series.bk_hash
               when l_spabiz_series.store_number is null then '-998'
               when l_spabiz_series.store_number = 0 then '-998'
          --util_bk_hash[l_spabiz_series.store_number,h_spabiz_series.store_number]
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast( l_spabiz_series.store_number as varchar(500)),'z#@$k%&P'))),2)
          end dim_spabiz_store_key,
		  s_spabiz_series.edit_time edit_date_time,
		  s_spabiz_series.quick_id quick_id,
		  case when s_spabiz_series.name is null then ''
               else s_spabiz_series.name
          end series_name,
		  case when p_spabiz_series.bk_hash in ('-997','-998','-999') then 'N'
               when s_spabiz_series.taxable = 1 then 'Y'
               else 'N'
          end taxable_flag,
		  p_spabiz_series.p_spabiz_series_id,
          p_spabiz_series.dv_batch_id,
          p_spabiz_series.dv_load_date_time,
          p_spabiz_series.dv_load_end_date_time        
     from #Business_keys
     join p_spabiz_series 
       on p_spabiz_series.bk_hash = #Business_keys.dim_spabiz_series_key
     join l_spabiz_series 
       on p_spabiz_series.l_spabiz_series_id = l_spabiz_series.l_spabiz_series_id
     join s_spabiz_series 
       on p_spabiz_series.s_spabiz_series_id = s_spabiz_series.s_spabiz_series_id
      and p_spabiz_series.dv_load_end_date_time = 'Dec 31, 9999'


 if object_id('tempdb..#sandbox_series_mapping') is not null drop table #sandbox_series_mapping
create table dbo.#sandbox_series_mapping with(distribution=hash(dim_spabiz_series_key), location=user_db, heap) as
 select 
        p_spabiz_series.bk_hash dim_spabiz_series_key,
	    s_sandbox_series_mapping.category category,
		s_sandbox_series_mapping.segment segment,
        p_spabiz_series.dv_load_end_date_time dv_load_end_date_time,
        p_spabiz_series.dv_batch_id dv_batch_id,
        p_spabiz_series.dv_load_date_time dv_load_date_time   		
     from #Business_keys
     join p_spabiz_series 
       on p_spabiz_series.bk_hash = #Business_keys.dim_spabiz_series_key
     join l_spabiz_series 
       on p_spabiz_series.l_spabiz_series_id = l_spabiz_series.l_spabiz_series_id	   
     join l_sandbox_series_mapping
       on p_spabiz_series.series_id = l_sandbox_series_mapping.series_id
      and p_spabiz_series.store_number = l_sandbox_series_mapping.store_number   
     join p_sandbox_series_mapping
       on l_sandbox_series_mapping.l_sandbox_series_mapping_id = p_sandbox_series_mapping.l_sandbox_series_mapping_id
     join s_sandbox_series_mapping
       on p_sandbox_series_mapping.s_sandbox_series_mapping_id = s_sandbox_series_mapping.s_sandbox_series_mapping_id	  
      and p_sandbox_series_mapping.dv_load_end_date_time = 'Dec 31, 9999'
 	  
 

 --~~~~~~~~~~~~~~~END OF STEP 2: Requried Fields from different participating fields have been created as #TEMP tables~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


	 

--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 3:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
----------------STEP 3: INSERT INTO DIM TABLE: By Joining the temp STEP 2's #temp tables, forming the main Dim table record-----------
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
--delete and re-insert
--do as a single transaction
--delete records from the fact table that exist
--insert records from records from current and missing batches
    begin tran
    delete dbo.dim_spabiz_series
    where dim_spabiz_series_key in (select dim_spabiz_series_key from dbo.#spabiz_series) 
	

	
	
 insert into dbo.dim_spabiz_series(dim_spabiz_series_key,
                                        series_id,
                                        store_number,
                                        deleted_date_time,
                                        deleted_flag,
                                        dim_spabiz_store_key,
                                        edit_date_time,
                                        quick_id,
                                        series_name,
                                        taxable_flag,
                                        category,
                                        segment,
                                        p_spabiz_series_id,
                                        dv_load_date_time,
                                        dv_load_end_date_time,
                                        dv_batch_id,
                                        dv_inserted_date_time,
                                        dv_insert_user)
                                                         
                                 select 
								        #spabiz_series.dim_spabiz_series_key,
                                        #spabiz_series.series_id,
                                        #spabiz_series.store_number,
                                        #spabiz_series.deleted_date_time,
                                        #spabiz_series.deleted_flag,
                                        #spabiz_series.dim_spabiz_store_key,
                                        #spabiz_series.edit_date_time,
                                        #spabiz_series.quick_id,
                                        #spabiz_series.series_name,
                                        #spabiz_series.taxable_flag,
                                        #sandbox_series_mapping.category,
                                        #sandbox_series_mapping.segment,
                                        #spabiz_series.p_spabiz_series_id,
										case when isnull(#spabiz_series.dv_load_date_time,'') > isnull(#sandbox_series_mapping.dv_load_date_time,'')
                                             then isnull(#spabiz_series.dv_load_date_time,'')
                                             else isnull(#sandbox_series_mapping.dv_load_date_time,'') 
            	                        end dv_load_date_time,
                                        case when isnull(#spabiz_series.dv_load_end_date_time,'') > isnull(#sandbox_series_mapping.dv_load_end_date_time,'')
                                             then isnull(#spabiz_series.dv_load_end_date_time,'')
                                             else isnull(#sandbox_series_mapping.dv_load_end_date_time,'') 
            	                        end dv_load_end_date_time,
                                        case when #spabiz_series.dv_batch_id > isnull(#sandbox_series_mapping.dv_batch_id,'-2')
                                             then #spabiz_series.dv_batch_id
                                             else isnull(#sandbox_series_mapping.dv_batch_id,'') 
            	                        end dv_batch_id,
									    getdate(),
                                        suser_sname()	
									from #spabiz_series 
                               left join #sandbox_series_mapping
                                      on #spabiz_series.dim_spabiz_series_key=#sandbox_series_mapping.dim_spabiz_series_key
   
   	 commit tran
---------------------------------------END OF STEP 3: END OF DIM INSERTS--------------------------------------
end
