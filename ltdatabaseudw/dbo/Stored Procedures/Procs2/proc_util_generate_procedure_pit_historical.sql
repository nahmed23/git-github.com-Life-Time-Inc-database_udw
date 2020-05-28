CREATE PROC [dbo].[proc_util_generate_procedure_pit_historical] @pit_table_name [varchar](500) AS
begin

set nocount on
set xact_abort on

--print '************************ proc_util_generate_historical_pit_procedure start ************************'

-- Set local variable @object_base to the name of the pit table minus the 'p' prefix
declare @object_base varchar(200) = substring(@pit_table_name,2,len(@pit_table_name))

--when we fully cut over the azure, remove this; "_azure" will be removed 
--declare @informatica_folder_name varchar(500) = replace(@informatica_folder,'_azure','')
declare @pit_fk_columns varchar(max), 
        @pit_joins varchar(max), 
        @ls_reads varchar(max), 
        @ls_ends varchar(max), 
        @u varchar(max)

declare @start int  
declare @end int
declare @inner_start int
declare @inner_end int
declare @table_name varchar(128)
declare @column_name varchar(128)
declare @greatest varchar(max)
declare @insert_columns varchar(max), @insert_select_columns varchar(max)
declare @pit_columns varchar(max), @bk_columns varchar(max)

if object_id('tempdb..#pit_proc') is not null drop table #pit_proc
create table dbo.#pit_proc
       (pit_proc_id int,
        ls_reads varchar(max),
        ls_ends varchar(max),
        u varchar(max),
        greatest varchar(max),
        first_greatest_flag bit)
with (clustered index (pit_proc_id))

if object_id('tempdb..#bks') is not null drop table #bks
create table dbo.#bks with(distribution=round_robin, location=user_db) as
select dv_column,
       rank() over (order by sort_order) r
  from dv_etl_map
 where dv_table = @pit_table_name
   and business_key_sort_order is not null

set @start = 1
set @end = (select max(r) from #bks)

--this loop puts together a select-friendly sql of the DV business key names
while @start <= @end
begin
    set @column_name = (select dv_column from #bks where r = @start)
    set @bk_columns = isnull(@bk_columns,'')+'       <table_name>.'+@column_name+','+char(13)+char(10)

    set @start = @start + 1
end

-- Create temp table #pit_fk_sources with the table name and primary key column name pertaining to the foreign key references from #tables
if object_id('tempdb..#pit_fk_sources') is not null drop table #pit_fk_sources
create table dbo.#pit_fk_sources with(distribution=round_robin, location=user_db) as
select source,
       source_table table_name,
       source_column column_name,
       rank() over (order by sort_order) table_rank
  from dv_etl_map
 where dv_table = @pit_table_name
   and business_key_sort_order is null

set @start = 1
set @end = (select max(table_rank) from #pit_fk_sources)

-- Loops through list of links and satellites (FKs on the pit table), assembling necessary SQL tidbits for later use
while @start <= @end
begin

    set @table_name = (select table_name from #pit_fk_sources where table_rank = @start)

    --normal (generic) list of the non-bk columns - bk columns come separately on their own.
    set @pit_columns = isnull(@pit_columns,'')+'       <table_name>.'+@table_name+'_id,'+char(13)+char(10)

    --link/satellite reads
    set @ls_reads = isnull(@ls_reads,'') +'if object_id(''tempdb..#'+@table_name+''') is not null drop table #'+@table_name+char(13)+char(10)
                                         +'create table dbo.#'+@table_name+' with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
                                         +'select '+@table_name+'.'+@table_name+'_id,'+char(13)+char(10)
                                         +'       '+@table_name+'.bk_hash,'+char(13)+char(10)
                                         +replace(@bk_columns,'<table_name>',@table_name)
                                         +'       '+@table_name+'.dv_load_date_time,'+char(13)+char(10)
                                         +'       '+@table_name+'.dv_batch_id,'+char(13)+char(10)
                                         +'       dense_rank() over (partition by '+@table_name+'.bk_hash order by '+@table_name+'.dv_load_date_time) ranking'+char(13)+char(10)
                                         +'  from dbo.'+@table_name+char(13)+char(10)
                                         +'  join #max_batch_id'+char(13)+char(10)
                                         +'    on '+@table_name+'.dv_batch_id > #max_batch_id.dv_batch_id'+char(13)+char(10)
                                         + char(13)+char(10)

    --link/satellite effectives
    set @ls_ends = isnull(@ls_ends,'') +'if object_id(''tempdb..#'+@table_name+'_end'') is not null drop table #'+@table_name+'_end'+char(13)+char(10)
                                       +'create table dbo.#'+@table_name+'_end with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
                                       +'select '+@table_name+'_1.'+@table_name+'_id,'+char(13)+char(10)
                                       +'       '+@table_name+'_1.bk_hash,'+char(13)+char(10)
                                       +'       '+@table_name+'_1.'+'dv_load_date_time,'+char(13)+char(10)
                                       +'       '+@table_name+'_1.'+'dv_batch_id,'+char(13)+char(10)
                                       +'       isnull('+@table_name+'_2.dv_load_date_time, ''Dec 31, 9999'') dv_load_end_date_time'+char(13)+char(10)
                                       +'  from #'+@table_name+' '+@table_name+'_1'+char(13)+char(10)
                                       +'  left join #'+@table_name+' '+@table_name+'_2'+char(13)+char(10)
                                       +'    on '+@table_name+'_1.bk_hash = '+@table_name+'_2.bk_hash'+char(13)+char(10)
                                       +'   and '+@table_name+'_1.ranking + 1 = '+@table_name+'_2.ranking'+char(13)+char(10)
                                       +char(13)+char(10)

    --unioned results
    set @u = isnull(@u,'') + case when @start = 1 then '' else 'union'+char(13)+char(10) end
                           +'select bk_hash,'+char(13)+char(10)
                           +replace(@bk_columns,'<table_name>.','')
                           +'       dv_load_date_time,'+char(13)+char(10)
                           +'       dv_batch_id'+char(13)+char(10)
                           +'  from #'+@table_name+char(13)+char(10)
    
    --pit fk columns
    set @pit_fk_columns = isnull(@pit_fk_columns,'')+'       isnull('+@table_name+'_end.'+@table_name+'_id, isnull(p_active.'+@table_name+'_id,-998)) '+@table_name+'_id,'+char(13)+char(10)

    --generate the FK table joins for the #p_new temp table 
    set @pit_joins = isnull(@pit_joins,'')+'  left join #'+@table_name+'_end '+@table_name+'_end'+char(13)+char(10)
                                            +'    on pr1.bk_hash = '+@table_name+'_end.bk_hash'+char(13)+char(10)
                                            +'   and pr1.dv_load_date_time >= '+@table_name+'_end.dv_load_date_time'+char(13)+char(10)
                                            +'   and isnull(pr2.dv_load_date_time,''dec 31, 9999'') <= '+@table_name+'_end.dv_load_end_date_time'+char(13)+char(10)

    --greatest
    if object_id('tempdb..#greatest') is not null drop table #greatest
    create table dbo.#greatest with(distribution=round_robin, location=user_db) as
    select dv_table,dv_column,
            greatest_satellite_date_time_type,
            row_number() over (order by sort_order) column_rank
        from dv_etl_map
        where dv_table = @table_name
        and greatest_satellite_date_time_type is not null
           
    set @greatest = null

    if exists(select top 1 1 from #greatest)
    begin
        
        set @inner_start = 1
        set @inner_end = (select max(column_rank) from #greatest)
        
        while @inner_start <= @inner_end
        begin

            set @greatest = (select isnull(@greatest,'')
                                    +case when @start = 1 and @inner_start = 1 then '' else 'union all'+char(13)+char(10) end
                                    +'select #p_new.bk_hash,'+char(13)+char(10)
                                    +'       #p_new.ranking,'+char(13)+char(10)
                                    +'       '+@table_name+'.'+dv_column+' satellite_date_time,'+char(13)+char(10)
                                    +'       '+@table_name+'.dv_batch_id'+char(13)+char(10)
                                    +' from #p_new'+char(13)+char(10)
                                    +' join dbo.'+@table_name+char(13)+char(10)
                                    +'   on #p_new.'+@table_name+'_id = '+@table_name+'.'+@table_name+'_id'+char(13)+char(10)
                                    +case when greatest_satellite_date_time_type in ('i') then ' where #p_new.dv_first_in_key_series = 1'
                                          when greatest_satellite_date_time_type in ('u') then ' where #p_new.dv_first_in_key_series <> 1'
                                          when greatest_satellite_date_time_type in ('b') then ''
                                          else NULL end +char(13)+char(10)
                            from #greatest
                            where column_rank = @inner_start)
                           
            set @inner_start = @inner_start+1
        end
    end
    else
    begin
            set @greatest = isnull(@greatest,'')
                           +case when @start > 1 then 'union'+char(13)+char(10) else '' end
                           +'select #p_new.bk_hash,'+char(13)+char(10)
                           +'       #p_new.ranking,'+char(13)+char(10)
                           +'       null satellite_date_time,'+char(13)+char(10)
                           +'       '+@table_name+'.dv_batch_id'+char(13)+char(10)
                           +' from #p_new'+char(13)+char(10)
                           +' join dbo.'+@table_name+char(13)+char(10)
                           +'   on #p_new.'+@table_name+'_id = '+@table_name+'.'+@table_name+'_id'+char(13)+char(10)
    end


    insert #pit_proc (pit_proc_id, ls_reads, ls_ends, u, greatest, first_greatest_flag)
    select @start, @ls_reads, @ls_ends, @u, @greatest, case when (select top 1 greatest from #pit_proc where greatest is not null) is null then 1 else 0 end

    set @ls_reads = ''
    set @ls_ends = ''
    set @u = ''
        

    set @start = @start + 1
end



--select @insert_columns, @insert_select_columns
declare @sql1 varchar(max)
declare @sql2 varchar(max)
declare @sql3 varchar(max)
declare @sql4 varchar(max)
declare @sql5 varchar(max)
declare @sql6 varchar(max)
declare @sql7 varchar(max)
declare @sql8 varchar(max)

set @sql1 = case when exists(select 1 from sys.procedures where name = 'proc_'+@pit_table_name) then 'alter' else 'create' end + ' procedure dbo.proc_'+@pit_table_name+char(13)+char(10)
          +'@current_dv_batch_id bigint'+char(13)+char(10)
          +'as'+char(13)+char(10)
          +'begin'+char(13)+char(10)
          +char(13)+char(10)
          +'set nocount on'+char(13)+char(10)
          +'set xact_abort on'+char(13)+char(10)
          +char(13)+char(10)
          +'declare @task_description varchar(500)'+char(13)+char(10)
          +char(13)+char(10)
          +'-- Do this as a single transaction to make sure that exactly one record for a business key is active'+char(13)+char(10)
          +'--   Re-activate the previously active PIT record.  This is the record with dv_load_end_date_time = current record dv_load_date_time'+char(13)+char(10)
          +'--   Delete the active PIT record'+char(13)+char(10)
          +char(13)+char(10)
          +'declare @wf_name varchar(100)'+char(13)+char(10)
          +'set @wf_name = ''wf_dv'+@object_base+''''+char(13)+char(10)
          +char(13)+char(10)
          +'if object_id(''tempdb..#batch_id'') is not null drop table #batch_id'+char(13)+char(10)
          +'create table dbo.#batch_id with(distribution=round_robin, location=user_db) as'+char(13)+char(10)
          +'select distinct dv_batch_id'+char(13)+char(10)
          +'  from dbo.dv_job_status_history'+char(13)+char(10)
          +' where job_name = @wf_name'+char(13)+char(10)
          +'   and dv_batch_id > (select max(dv_batch_id) from dbo.dv_job_status_history where job_name = @wf_name and job_status = ''Complete'')'+char(13)+char(10)
          +' union'+char(13)+char(10)
          +'select @current_dv_batch_id'+char(13)+char(10)
          +char(13)+char(10)
          +'if object_id(''tempdb..#delete'') is not null drop table #delete'+char(13)+char(10)
          +'create table dbo.#delete with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
          +'select bk_hash,'+char(13)+char(10)
          +'       dv_load_date_time'+char(13)+char(10)
          +'  from dbo.'+@pit_table_name+char(13)+char(10)
          +'  join #batch_id'+char(13)+char(10)
          +'    on '+@pit_table_name+'.dv_batch_id = #batch_id.dv_batch_id'+char(13)+char(10)
          +' group by bk_hash,'+char(13)+char(10)
          +'       dv_load_date_time'+char(13)+char(10)
          +char(13)+char(10)
          +'begin tran'+char(13)+char(10)
          +char(13)+char(10)
          +'delete from dbo.'+@pit_table_name+char(13)+char(10)
          +' where dv_batch_id in (select dv_batch_id from #batch_id)'+char(13)+char(10)
          +char(13)+char(10)
          +'update '+@pit_table_name+char(13)+char(10)
          +'   set dv_load_end_date_time = ''dec 31, 9999'''+char(13)+char(10)
          +'  from #delete'+char(13)+char(10)
          +' where '+@pit_table_name+'.bk_hash = #delete.bk_hash'+char(13)+char(10)
          +'   and '+@pit_table_name+'.dv_load_end_date_time = #delete.dv_load_date_time'+char(13)+char(10)
          +char(13)+char(10)
          +'commit tran'+char(13)+char(10)
          +char(13)+char(10)
          +'if object_id(''tempdb..#max_batch_id'') is not null drop table #max_batch_id'+char(13)+char(10)
          +'create table dbo.#max_batch_id with(distribution=round_robin, location=user_db) as'+char(13)+char(10)
          +'select isnull(max(dv_batch_id),-2) dv_batch_id'+char(13)+char(10)
          +'  from dbo.'+@pit_table_name+char(13)+char(10)
          +char(13)+char(10)
          +'-- For each satellite table populate a temp table with the satellite data that is not in the PIT table.'+char(13)+char(10)
          +'-- Rank each business key by dv_load_date_time'+char(13)+char(10)

-- + @ls_reads +

set @sql2 = '-- For each satellite rank table from above populate a temp table with a calculated dv_load_end_date.'+char(13)+char(10)
           +'-- The dv_load_end_date is the dv_load_date_time from the record with the next sequential rank.  If there is no next record then use Dec 31, 9999.'+char(13)+char(10)

-- +@ls_ends+

set @sql3 = '-- Populate temp table #u with the union of the satellite rank tables from above to find the distinct set of business keys and dv_load_date_time'+char(13)+char(10)
           +'if object_id(''tempdb..#u'') is not null drop table #u'+char(13)+char(10)
           +'create table dbo.#u with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)

-- +@u+

set @sql4 = char(13)+char(10)
           +'-- Take the min(dv_batch_id) to cover records being loaded in separate batch_ids with the same source inserted date time'+char(13)+char(10)
           +'if object_id(''tempdb..#mu'') is not null drop table #mu'+char(13)+char(10)
           +'create table dbo.#mu with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
           +'select bk_hash,'+char(13)+char(10)
           +replace(@bk_columns,'<table_name>.','')
           +'       dv_load_date_time,'+char(13)+char(10)
           +'       min(dv_batch_id) dv_batch_id'+char(13)+char(10)
           +'  from #u'+char(13)+char(10)
           +' group by bk_hash,'+char(13)+char(10)
           +replace(replace(@bk_columns,'<table_name>.',''),'       ','          ')
           +'          dv_load_date_time'+char(13)+char(10)
           +char(13)+char(10)
           +'-- Populate temp table #pr with the rank of the union result (#u) by dv_load_date_time'+char(13)+char(10)
           +'if object_id(''tempdb..#pr'') is not null drop table #pr'+char(13)+char(10)
           +'create table dbo.#pr with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
           +'select bk_hash,'+char(13)+char(10)
           +replace(@bk_columns,'<table_name>.','')
           +'       dv_load_date_time,'+char(13)+char(10)
           +'       dv_batch_id,'+char(13)+char(10)
           +'       dense_rank() over (partition by bk_hash order by dv_load_date_time) ranking'+char(13)+char(10)
           +'  from #mu'+char(13)+char(10)
           +char(13)+char(10)
           +'-- Populate temp table #du with the distinct business keys from #u'+char(13)+char(10)
           +'if object_id(''tempdb..#du'') is not null drop table #du'+char(13)+char(10)
           +'create table dbo.#du with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
           +'select distinct bk_hash'+char(13)+char(10)
           +'  from #mu'+char(13)+char(10)
           +char(13)+char(10)
           +'-- Populate temp table #p_active with the active PIT record (if any) associated with the business keys from above (#du)'+char(13)+char(10)
           +'if object_id(''tempdb..#p_active'') is not null drop table #p_active'+char(13)+char(10)
           +'create table dbo.#p_active with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
           +'select '+@pit_table_name+'.'+@pit_table_name+'_id,'+char(13)+char(10)
           +'       '+@pit_table_name+'.bk_hash,'+char(13)+char(10)
           +replace(@pit_columns,'<table_name>',@pit_table_name)
           +'       '+@pit_table_name+'.dv_load_end_date_time'+char(13)+char(10)
           +'  from dbo.'+@pit_table_name+char(13)+char(10)
           +'  join #du'+char(13)+char(10)
           +'    on '+@pit_table_name+'.bk_hash = #du.bk_hash'+char(13)+char(10)
           +' where '+@pit_table_name+'.dv_load_end_date_time = ''Dec 31, 9999'''+char(13)+char(10)
           +char(13)+char(10)

set @sql5 = '-- Populate temp table #p_new with the new PIT records.  Also include the rank and the active PIT record id (if any) to be used'+char(13)+char(10)
           +'-- when setting the dv_load_end_date_time on the existing active record.'+char(13)+char(10)
           +'-- If the record is the first in a series set the dv_first_in_key_series flag.'+char(13)+char(10)
           +'if object_id(''tempdb..#p_new'') is not null drop table #p_new'+char(13)+char(10)
           +'create table dbo.#p_new with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
           +'select pr1.bk_hash,'+char(13)+char(10)
           +'       pr1.dv_load_date_time,'+char(13)+char(10)
           +'       pr1.ranking,'+char(13)+char(10)
           +replace(@bk_columns,'<table_name>','pr1')
           +'       isnull(pr2.dv_load_date_time, ''dec 31, 9999'') dv_load_end_date_time,'+char(13)+char(10)
           +@pit_fk_columns
           +'       p_active.'+@pit_table_name+'_id,'+char(13)+char(10)
           +'       case when p_active.'+@pit_table_name+'_id is null and pr1.ranking = 1 then 1 else 0 end dv_first_in_key_series'+char(13)+char(10)
           --+'       row_number() over (order by pr1.bk_hash) row_num'+char(13)+char(10)
           +'  from #pr pr1'+char(13)+char(10)
           +'  left join #pr pr2'+char(13)+char(10)
           +'    on pr1.bk_hash = pr2.bk_hash'+char(13)+char(10)
           +'   and pr1.ranking + 1 = pr2.ranking'+char(13)+char(10)
           +'  left join #p_active p_active'+char(13)+char(10)
           +'    on pr1.bk_hash = p_active.bk_hash'+char(13)+char(10)
           +@pit_joins+char(13)+char(10)

--set @sql6 = '--stack values for greatest() calculation: satellite_date_time and dv_batch_id'+char(13)+char(10)
--           +'if object_id(''tempdb..#greatest_calc_prep'') is not null drop table #greatest_calc_prep'+char(13)+char(10)
--           +'create table dbo.#greatest_calc_prep with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)+(
--select isnull((select greatest from #pit_proc where pit_proc_id = 1), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 2), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 3), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 4), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 5), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 6), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 7), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 8), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 9), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 10), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 11), '') +
--       isnull((select greatest from #pit_proc where pit_proc_id = 12), '') + char(13)+char(10))
set @sql6 = '--stack values for greatest() calculation: satellite_date_time and dv_batch_id'+char(13)+char(10)
           +'if object_id(''tempdb..#greatest'') is not null drop table #greatest'+char(13)+char(10)
           +'create table dbo.#greatest with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
           +'select bk_hash,'+char(13)+char(10)
           +'       ranking,'+char(13)+char(10)
           +'       max(satellite_date_time) max_satellite_date_time,'+char(13)+char(10)
           +'       max(dv_batch_id) max_dv_batch_id'+char(13)+char(10)
           +'  from ('+char(13)+char(10)+(

select isnull((select greatest from #pit_proc where pit_proc_id = 1), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 2), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 3), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 4), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 5), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 6), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 7), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 8), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 9), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 10), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 11), '') +
       isnull((select greatest from #pit_proc where pit_proc_id = 12), '') + char(13)+char(10))
           +'  ) x'+char(13)+char(10)
           +' group by bk_hash,ranking'+char(13)+char(10)

--set @sql7 = 'if object_id(''tempdb..#greatest'') is not null drop table #greatest'+char(13)+char(10)
--           +'create table dbo.#greatest with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
--           +'select bk_hash,'+char(13)+char(10)
--           +'       row_num,'+char(13)+char(10)
--           +'       max(satellite_date_time) max_satellite_date_time,'+char(13)+char(10)
--           +'       max(dv_batch_id) max_dv_batch_id'+char(13)+char(10)
--           +'  from #greatest_calc_prep'+char(13)+char(10)
--           +' group by bk_hash,row_num'+char(13)+char(10)
--           +char(13)+char(10)
set @sql7 = char(13)+char(10)
           +'if object_id(''tempdb..#inserts'') is not null drop table #inserts'+char(13)+char(10)
           +'create table dbo.#inserts with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as'+char(13)+char(10)
           +'select #p_new.'+@pit_table_name+'_id,'+char(13)+char(10)
           +'       #p_new.bk_hash,'+char(13)+char(10)
           +replace(@bk_columns,'<table_name>','#p_new')
           +replace(@pit_columns,'<table_name>','#p_new')
           +'       #greatest.max_satellite_date_time greatest_satellite_date_time,'+char(13)+char(10)
           +'       #p_new.dv_load_date_time,'+char(13)+char(10)
           +'       #p_new.dv_load_end_date_time,'+char(13)+char(10)
           +'       #greatest.max_dv_batch_id dv_batch_id,'+char(13)+char(10)
           +'       #p_new.dv_first_in_key_series,'+char(13)+char(10)
           +'       #p_new.ranking'+char(13)+char(10)
           +'  from #p_new'+char(13)+char(10)
           +'  join #greatest'+char(13)+char(10)
           +'    on #p_new.ranking = #greatest.ranking' +char(13)+char(10)
           +'   and #p_new.bk_hash = #greatest.bk_hash' +char(13)+char(10)

set @sql8 = char(13)+char(10)
           +'-- Do this as a single transaction to make sure that exactly one record for a business key is active'+char(13)+char(10)
           +'--   Change dv_load_end_date_time from dec 31, 9999 on the existing PIT table records to the earliest dv_load_date_time in the new records'+char(13)+char(10)
           +'--   Insert the new PIT table records'+char(13)+char(10)
           --+'declare @start_r_id bigint, @c int'+char(13)+char(10)
           --+'set @c = isnull((select max(row_num) from #inserts),0)'+char(13)+char(10)
           --+'exec dbo.proc_util_sequence_number_get_next @table_name = '''+@pit_table_name+''', @id_count = @c, @start_id = @start_r_id out'+char(13)+char(10)
           +char(13)+char(10)
           +'declare @start int, @end int, @user varchar(50)'+char(13)+char(10)
           --+'set @start = 1'+char(13)+char(10)
           --+'set @end = (select max(row_num) from #p_new)'+char(13)+char(10)
           +'set @user = suser_sname()'+char(13)+char(10)
           +'declare @insert_date_time datetime = getdate()'+char(13)+char(10)
           --+'while @start <= @end'+char(13)+char(10)
           --+'begin'+char(13)+char(10)
           +'begin tran'+char(13)+char(10)
           +'    update dbo.'+@pit_table_name+char(13)+char(10)
           +'       set dv_load_end_date_time = p_new.dv_load_date_time,'+char(13)+char(10)
           +'           dv_next_greatest_satellite_date_time = p_new.greatest_satellite_date_time,'+char(13)+char(10) 
           +'           dv_updated_date_time = @insert_date_time,'+char(13)+char(10)
           +'           dv_update_user = @user'+char(13)+char(10)
           +'      from #inserts p_new'+char(13)+char(10)
           +'     where '+@pit_table_name+'.'+@pit_table_name+'_id = p_new.'+@pit_table_name+'_id'+char(13)+char(10)
           +'       and '+@pit_table_name+'.bk_hash = p_new.bk_hash'+char(13)+char(10)
           +'       and p_new.ranking = 1'+char(13)+char(10)
           --+'       and p_new.row_num >= @start'+char(13)+char(10)
           --+'       and p_new.row_num < @start+1000000'+char(13)+char(10)
           +char(13)+char(10)
           +'    insert into dbo.'+@pit_table_name+'('+char(13)+char(10)
           +'        bk_hash,'+char(13)+char(10)
           +replace(replace(@bk_columns,'<table_name>.',''),'       ','        ')
           +replace(replace(@pit_columns,'<table_name>.',''),'       ','        ')
           +'        dv_greatest_satellite_date_time,'+char(13)+char(10)
           +'        dv_next_greatest_satellite_date_time,'+char(13)+char(10)
           +'        dv_inserted_date_time,'+char(13)+char(10)
           +'        dv_insert_user,'+char(13)+char(10)
           +'        dv_load_date_time,'+char(13)+char(10)
           +'        dv_load_end_date_time,'+char(13)+char(10)
           +'        dv_batch_id,'+char(13)+char(10)
           +'        dv_first_in_key_series)'+char(13)+char(10)
           +'    select p1.bk_hash,'+char(13)+char(10)
           +replace(replace(@bk_columns,'<table_name>','p1'),'       ','           ')
           +replace(replace(@pit_columns,'<table_name>','p1'),'       ','           ')
           +'           p1.greatest_satellite_date_time,'+char(13)+char(10)
           +'           p2.greatest_satellite_date_time,'+char(13)+char(10)
           +'           @insert_date_time,'+char(13)+char(10)
           +'           suser_sname(),'+char(13)+char(10)
           +'           p1.dv_load_date_time,'+char(13)+char(10)
           +'           p1.dv_load_end_date_time,'+char(13)+char(10)
           +'           p1.dv_batch_id,'+char(13)+char(10)
           +'           p1.dv_first_in_key_series'+char(13)+char(10)
           +'      from #inserts p1'+char(13)+char(10)
           +'      left join #inserts p2'+char(13)+char(10)
           +'        on p1.bk_hash = p2.bk_hash'+char(13)+char(10)
           +'       and p1.ranking + 1 = p2.ranking'+char(13)+char(10) 
           --+'     where p1.row_num >= @start'+char(13)+char(10)
           --+'       and p1.row_num < @start+1000000'+char(13)+char(10)
           +'commit tran'+char(13)+char(10)
           +char(13)+char(10)
           --+'    set @start = @start + 1000000'+char(13)+char(10)
           --+char(13)+char(10)
           --+'end'+char(13)+char(10)
           +char(13)+char(10)
           +'end'+char(13)+char(10)

declare @ls_reads1 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 1), ''))
declare @ls_reads2 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 2), ''))
declare @ls_reads3 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 3), ''))
declare @ls_reads4 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 4), ''))
declare @ls_reads5 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 5), ''))
declare @ls_reads6 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 6), ''))
declare @ls_reads7 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 7), ''))
declare @ls_reads8 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 8), ''))
declare @ls_reads9 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 9), ''))
declare @ls_reads10 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 10), ''))
declare @ls_reads11 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 11), ''))
declare @ls_reads12 varchar(max) = (select isnull((select ls_reads from #pit_proc where pit_proc_id = 12), ''))

declare @ls_ends1 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 1), ''))
declare @ls_ends2 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 2), ''))
declare @ls_ends3 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 3), ''))
declare @ls_ends4 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 4), ''))
declare @ls_ends5 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 5), ''))
declare @ls_ends6 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 6), ''))
declare @ls_ends7 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 7), ''))
declare @ls_ends8 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 8), ''))
declare @ls_ends9 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 9), ''))
declare @ls_ends10 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 10), ''))
declare @ls_ends11 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 11), ''))
declare @ls_ends12 varchar(max) = (select isnull((select ls_ends from #pit_proc where pit_proc_id = 12), ''))

declare @u1 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 1), ''))
declare @u2 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 2), ''))
declare @u3 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 3), ''))
declare @u4 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 4), ''))
declare @u5 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 5), ''))
declare @u6 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 6), ''))
declare @u7 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 7), ''))
declare @u8 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 8), ''))
declare @u9 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 9), ''))
declare @u10 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 10), ''))
declare @u11 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 11), ''))
declare @u12 varchar(max) = (select isnull((select u from #pit_proc where pit_proc_id = 12), ''))

exec( @sql1 +
      @ls_reads1 +
      @ls_reads2 +
      @ls_reads3 +
      @ls_reads4 +
      @ls_reads5 +
      @ls_reads6 +
      @ls_reads7 +
      @ls_reads8 +
      @ls_reads9 +
      @ls_reads10 +
      @ls_reads11 +
      @ls_reads12 +
      @sql2 +
      @ls_ends1 +
      @ls_ends2 +
      @ls_ends3 +
      @ls_ends4 +
      @ls_ends5 +
      @ls_ends6 +
      @ls_ends7 +
      @ls_ends8 +
      @ls_ends9 +
      @ls_ends10 +
      @ls_ends11 + 
      @ls_ends12 +
      @sql3 +
      @u1 +
      @u2 +
      @u3 +
      @u4 +
      @u5 +
      @u6 +
      @u7 +
      @u8 +
      @u9 +
      @u10 +
      @u11 +
      @u12 +
      @sql4 +
      @sql5 +
      @sql6 +
      @sql7 +
      @sql8
      --'end'
)
/*

print @sql1
print @ls_reads1
print @ls_reads2
print @ls_reads3
print @ls_reads4
print @ls_reads5
print @ls_reads6
print @ls_reads7
print @ls_reads8
print @ls_reads9
print @ls_reads10
print @ls_reads11
print @ls_reads12
print @sql2
print @ls_ends1
print @ls_ends2
print @ls_ends3
print @ls_ends4
print @ls_ends5
print @ls_ends6
print @ls_ends7
print @ls_ends8
print @ls_ends9
print @ls_ends10
print @ls_ends11 
print @ls_ends12
print @sql3
print @u1
print @u2
print @u3
print @u4
print @u5
print @u6
print @u7
print @u8
print @u9
print @u10
print @u11
print @u12
print @sql4
print @sql5
print @sql6
print @sql7
print @sql8
*/
drop table #pit_proc

--print '************************ proc_util_generate_historical_pit_procedure end ************************'

end
