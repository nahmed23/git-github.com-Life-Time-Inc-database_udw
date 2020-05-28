CREATE PROC [dbo].[proc_etl_hybris_point_of_service] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_pointofservice

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_pointofservice (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_name,
       p_address,
       p_description,
       p_type,
       p_mapicon,
       p_latitude,
       p_longitude,
       p_geocodetimestamp,
       p_openingschedule,
       p_storeimage,
       p_basestore,
       p_displayname,
       p_nearbystoreradius,
       p_ltfclubid,
       p_nextmonthduesflag,
       p_nextmonthduesdayofmonth,
       p_catalog,
       p_activeflag,
       aCLTS,
       propTS,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_name,
       p_address,
       p_description,
       p_type,
       p_mapicon,
       p_latitude,
       p_longitude,
       p_geocodetimestamp,
       p_openingschedule,
       p_storeimage,
       p_basestore,
       p_displayname,
       p_nearbystoreradius,
       p_ltfclubid,
       p_nextmonthduesflag,
       p_nextmonthduesdayofmonth,
       p_catalog,
       p_activeflag,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_pointofservice.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_pointofservice
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_point_of_service @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_point_of_service (
       bk_hash,
       point_of_service_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_pointofservice.bk_hash,
       stage_hash_hybris_pointofservice.[PK] point_of_service_pk,
       isnull(cast(stage_hash_hybris_pointofservice.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_pointofservice
  left join h_hybris_point_of_service
    on stage_hash_hybris_pointofservice.bk_hash = h_hybris_point_of_service.bk_hash
 where h_hybris_point_of_service_id is null
   and stage_hash_hybris_pointofservice.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_point_of_service
if object_id('tempdb..#l_hybris_point_of_service_inserts') is not null drop table #l_hybris_point_of_service_inserts
create table #l_hybris_point_of_service_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_pointofservice.bk_hash,
       stage_hash_hybris_pointofservice.TypePkString type_pk_string,
       stage_hash_hybris_pointofservice.OwnerPkString owner_pk_string,
       stage_hash_hybris_pointofservice.[PK] point_of_service_pk,
       stage_hash_hybris_pointofservice.p_address p_address,
       stage_hash_hybris_pointofservice.p_type p_type,
       stage_hash_hybris_pointofservice.p_basestore p_base_store,
       stage_hash_hybris_pointofservice.p_ltfclubid p_ltf_club_id,
       stage_hash_hybris_pointofservice.p_catalog p_catalog,
       stage_hash_hybris_pointofservice.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_address as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_type as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_basestore as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_ltfclubid as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_catalog as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_pointofservice
 where stage_hash_hybris_pointofservice.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_point_of_service records
set @insert_date_time = getdate()
insert into l_hybris_point_of_service (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       point_of_service_pk,
       p_address,
       p_type,
       p_base_store,
       p_ltf_club_id,
       p_catalog,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_point_of_service_inserts.bk_hash,
       #l_hybris_point_of_service_inserts.type_pk_string,
       #l_hybris_point_of_service_inserts.owner_pk_string,
       #l_hybris_point_of_service_inserts.point_of_service_pk,
       #l_hybris_point_of_service_inserts.p_address,
       #l_hybris_point_of_service_inserts.p_type,
       #l_hybris_point_of_service_inserts.p_base_store,
       #l_hybris_point_of_service_inserts.p_ltf_club_id,
       #l_hybris_point_of_service_inserts.p_catalog,
       case when l_hybris_point_of_service.l_hybris_point_of_service_id is null then isnull(#l_hybris_point_of_service_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_point_of_service_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_point_of_service_inserts
  left join p_hybris_point_of_service
    on #l_hybris_point_of_service_inserts.bk_hash = p_hybris_point_of_service.bk_hash
   and p_hybris_point_of_service.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_point_of_service
    on p_hybris_point_of_service.bk_hash = l_hybris_point_of_service.bk_hash
   and p_hybris_point_of_service.l_hybris_point_of_service_id = l_hybris_point_of_service.l_hybris_point_of_service_id
 where l_hybris_point_of_service.l_hybris_point_of_service_id is null
    or (l_hybris_point_of_service.l_hybris_point_of_service_id is not null
        and l_hybris_point_of_service.dv_hash <> #l_hybris_point_of_service_inserts.source_hash)

--calculate hash and lookup to current s_hybris_point_of_service
if object_id('tempdb..#s_hybris_point_of_service_inserts') is not null drop table #s_hybris_point_of_service_inserts
create table #s_hybris_point_of_service_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_pointofservice.bk_hash,
       stage_hash_hybris_pointofservice.hjmpTS hjmpts,
       stage_hash_hybris_pointofservice.createdTS created_ts,
       stage_hash_hybris_pointofservice.modifiedTS modified_ts,
       stage_hash_hybris_pointofservice.[PK] point_of_service_pk,
       stage_hash_hybris_pointofservice.p_name p_name,
       stage_hash_hybris_pointofservice.p_description p_description,
       stage_hash_hybris_pointofservice.p_mapicon p_mapicon,
       stage_hash_hybris_pointofservice.p_latitude p_latitude,
       stage_hash_hybris_pointofservice.p_longitude p_longitude,
       stage_hash_hybris_pointofservice.p_geocodetimestamp p_geo_code_time_stamp,
       stage_hash_hybris_pointofservice.p_openingschedule p_opening_schedule,
       stage_hash_hybris_pointofservice.p_storeimage p_store_image,
       stage_hash_hybris_pointofservice.p_displayname p_display_name,
       stage_hash_hybris_pointofservice.p_nearbystoreradius p_nearby_store_radius,
       stage_hash_hybris_pointofservice.p_nextmonthduesflag p_next_month_dues_flag,
       stage_hash_hybris_pointofservice.p_nextmonthduesdayofmonth p_next_month_dues_day_of_month,
       stage_hash_hybris_pointofservice.p_activeflag p_active_flag,
       stage_hash_hybris_pointofservice.aCLTS acl_ts,
       stage_hash_hybris_pointofservice.propTS prop_ts,
       stage_hash_hybris_pointofservice.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_pointofservice.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_pointofservice.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_pointofservice.p_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_pointofservice.p_description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_mapicon as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_latitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_longitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_pointofservice.p_geocodetimestamp,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_openingschedule as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_storeimage as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_pointofservice.p_displayname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_nearbystoreradius as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_nextmonthduesflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_nextmonthduesdayofmonth as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.p_activeflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_pointofservice.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_pointofservice
 where stage_hash_hybris_pointofservice.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_point_of_service records
set @insert_date_time = getdate()
insert into s_hybris_point_of_service (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       point_of_service_pk,
       p_name,
       p_description,
       p_mapicon,
       p_latitude,
       p_longitude,
       p_geo_code_time_stamp,
       p_opening_schedule,
       p_store_image,
       p_display_name,
       p_nearby_store_radius,
       p_next_month_dues_flag,
       p_next_month_dues_day_of_month,
       p_active_flag,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_point_of_service_inserts.bk_hash,
       #s_hybris_point_of_service_inserts.hjmpts,
       #s_hybris_point_of_service_inserts.created_ts,
       #s_hybris_point_of_service_inserts.modified_ts,
       #s_hybris_point_of_service_inserts.point_of_service_pk,
       #s_hybris_point_of_service_inserts.p_name,
       #s_hybris_point_of_service_inserts.p_description,
       #s_hybris_point_of_service_inserts.p_mapicon,
       #s_hybris_point_of_service_inserts.p_latitude,
       #s_hybris_point_of_service_inserts.p_longitude,
       #s_hybris_point_of_service_inserts.p_geo_code_time_stamp,
       #s_hybris_point_of_service_inserts.p_opening_schedule,
       #s_hybris_point_of_service_inserts.p_store_image,
       #s_hybris_point_of_service_inserts.p_display_name,
       #s_hybris_point_of_service_inserts.p_nearby_store_radius,
       #s_hybris_point_of_service_inserts.p_next_month_dues_flag,
       #s_hybris_point_of_service_inserts.p_next_month_dues_day_of_month,
       #s_hybris_point_of_service_inserts.p_active_flag,
       #s_hybris_point_of_service_inserts.acl_ts,
       #s_hybris_point_of_service_inserts.prop_ts,
       case when s_hybris_point_of_service.s_hybris_point_of_service_id is null then isnull(#s_hybris_point_of_service_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_point_of_service_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_point_of_service_inserts
  left join p_hybris_point_of_service
    on #s_hybris_point_of_service_inserts.bk_hash = p_hybris_point_of_service.bk_hash
   and p_hybris_point_of_service.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_point_of_service
    on p_hybris_point_of_service.bk_hash = s_hybris_point_of_service.bk_hash
   and p_hybris_point_of_service.s_hybris_point_of_service_id = s_hybris_point_of_service.s_hybris_point_of_service_id
 where s_hybris_point_of_service.s_hybris_point_of_service_id is null
    or (s_hybris_point_of_service.s_hybris_point_of_service_id is not null
        and s_hybris_point_of_service.dv_hash <> #s_hybris_point_of_service_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_point_of_service @current_dv_batch_id

end
