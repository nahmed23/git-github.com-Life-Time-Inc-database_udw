CREATE PROC [dbo].[proc_d_spabiz_series] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_series)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_series_insert') is not null drop table #p_spabiz_series_insert
create table dbo.#p_spabiz_series_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_series.p_spabiz_series_id,
       p_spabiz_series.bk_hash
  from dbo.p_spabiz_series
 where p_spabiz_series.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_series.dv_batch_id > @max_dv_batch_id
        or p_spabiz_series.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_series.bk_hash,
       p_spabiz_series.bk_hash dim_spabiz_series_key,
       p_spabiz_series.series_id series_id,
       p_spabiz_series.store_number store_number,
       case when p_spabiz_series.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_series.delete_date = convert(date, '18991230', 112) then null
            else s_spabiz_series.delete_date
        end deleted_date_time,
       case when s_spabiz_series.series_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_series.bk_hash in ('-997','-998','-999') then p_spabiz_series.bk_hash
            when l_spabiz_series.store_number is null then '-998'
            when l_spabiz_series.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_series.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_series.edit_time edit_date_time,
       case when s_spabiz_series.quick_id is null then ''
            else s_spabiz_series.quick_id
        end quick_id,
       case when s_spabiz_series.name is null then ''
            else s_spabiz_series.name
        end series_name,
       case when s_spabiz_series.taxable = 1 then 'Y'
            else 'N'
        end taxable_flag,
       p_spabiz_series.p_spabiz_series_id,
       p_spabiz_series.dv_batch_id,
       p_spabiz_series.dv_load_date_time,
       p_spabiz_series.dv_load_end_date_time
  from dbo.p_spabiz_series
  join #p_spabiz_series_insert
    on p_spabiz_series.bk_hash = #p_spabiz_series_insert.bk_hash
   and p_spabiz_series.p_spabiz_series_id = #p_spabiz_series_insert.p_spabiz_series_id
  join dbo.l_spabiz_series
    on p_spabiz_series.bk_hash = l_spabiz_series.bk_hash
   and p_spabiz_series.l_spabiz_series_id = l_spabiz_series.l_spabiz_series_id
  join dbo.s_spabiz_series
    on p_spabiz_series.bk_hash = s_spabiz_series.bk_hash
   and p_spabiz_series.s_spabiz_series_id = s_spabiz_series.s_spabiz_series_id
 where l_spabiz_series.store_number not in (1,100,999) OR p_spabiz_series.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_series
   where d_spabiz_series.bk_hash in (select bk_hash from #p_spabiz_series_insert)

  insert dbo.d_spabiz_series(
             bk_hash,
             dim_spabiz_series_key,
             series_id,
             store_number,
             deleted_date_time,
             deleted_flag,
             dim_spabiz_store_key,
             edit_date_time,
             quick_id,
             series_name,
             taxable_flag,
             p_spabiz_series_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_spabiz_series_key,
         series_id,
         store_number,
         deleted_date_time,
         deleted_flag,
         dim_spabiz_store_key,
         edit_date_time,
         quick_id,
         series_name,
         taxable_flag,
         p_spabiz_series_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_series)
--Done!
end
