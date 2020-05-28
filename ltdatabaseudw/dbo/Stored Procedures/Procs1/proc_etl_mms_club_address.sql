CREATE PROC [dbo].[proc_etl_mms_club_address] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ClubAddress

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ClubAddress (
       bk_hash,
       ClubAddressID,
       ClubID,
       AddressLine1,
       AddressLine2,
       City,
       ValAddressTypeID,
       Zip,
       InsertedDateTime,
       ValCountryID,
       ValStateID,
       UpdatedDateTime,
       Latitude,
       Longitude,
       MapCenterLatitude,
       MapCenterLongitude,
       MapZoomLevel,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubAddressID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ClubAddressID,
       ClubID,
       AddressLine1,
       AddressLine2,
       City,
       ValAddressTypeID,
       Zip,
       InsertedDateTime,
       ValCountryID,
       ValStateID,
       UpdatedDateTime,
       Latitude,
       Longitude,
       MapCenterLatitude,
       MapCenterLongitude,
       MapZoomLevel,
       isnull(cast(stage_mms_ClubAddress.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_ClubAddress
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_club_address @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_club_address (
       bk_hash,
       club_address_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_ClubAddress.bk_hash,
       stage_hash_mms_ClubAddress.ClubAddressID club_address_id,
       isnull(cast(stage_hash_mms_ClubAddress.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ClubAddress
  left join h_mms_club_address
    on stage_hash_mms_ClubAddress.bk_hash = h_mms_club_address.bk_hash
 where h_mms_club_address_id is null
   and stage_hash_mms_ClubAddress.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_club_address
if object_id('tempdb..#l_mms_club_address_inserts') is not null drop table #l_mms_club_address_inserts
create table #l_mms_club_address_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ClubAddress.bk_hash,
       stage_hash_mms_ClubAddress.ClubAddressID club_address_id,
       stage_hash_mms_ClubAddress.ClubID club_id,
       stage_hash_mms_ClubAddress.ValAddressTypeID val_address_type_id,
       stage_hash_mms_ClubAddress.ValCountryID val_country_id,
       stage_hash_mms_ClubAddress.ValStateID val_state_id,
       stage_hash_mms_ClubAddress.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.ClubAddressID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.ValAddressTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.ValCountryID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.ValStateID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ClubAddress
 where stage_hash_mms_ClubAddress.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_club_address records
set @insert_date_time = getdate()
insert into l_mms_club_address (
       bk_hash,
       club_address_id,
       club_id,
       val_address_type_id,
       val_country_id,
       val_state_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_club_address_inserts.bk_hash,
       #l_mms_club_address_inserts.club_address_id,
       #l_mms_club_address_inserts.club_id,
       #l_mms_club_address_inserts.val_address_type_id,
       #l_mms_club_address_inserts.val_country_id,
       #l_mms_club_address_inserts.val_state_id,
       case when l_mms_club_address.l_mms_club_address_id is null then isnull(#l_mms_club_address_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_club_address_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_club_address_inserts
  left join p_mms_club_address
    on #l_mms_club_address_inserts.bk_hash = p_mms_club_address.bk_hash
   and p_mms_club_address.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_club_address
    on p_mms_club_address.bk_hash = l_mms_club_address.bk_hash
   and p_mms_club_address.l_mms_club_address_id = l_mms_club_address.l_mms_club_address_id
 where l_mms_club_address.l_mms_club_address_id is null
    or (l_mms_club_address.l_mms_club_address_id is not null
        and l_mms_club_address.dv_hash <> #l_mms_club_address_inserts.source_hash)

--calculate hash and lookup to current s_mms_club_address
if object_id('tempdb..#s_mms_club_address_inserts') is not null drop table #s_mms_club_address_inserts
create table #s_mms_club_address_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ClubAddress.bk_hash,
       stage_hash_mms_ClubAddress.ClubAddressID club_address_id,
       stage_hash_mms_ClubAddress.AddressLine1 address_line1,
       stage_hash_mms_ClubAddress.AddressLine2 address_line2,
       stage_hash_mms_ClubAddress.City city,
       stage_hash_mms_ClubAddress.Zip zip_code,
       stage_hash_mms_ClubAddress.InsertedDateTime inserted_date_time,
       stage_hash_mms_ClubAddress.Latitude latitude,
       stage_hash_mms_ClubAddress.Longitude longitude,
       stage_hash_mms_ClubAddress.MapCenterLatitude map_center_latitude,
       stage_hash_mms_ClubAddress.MapCenterLongitude map_center_longitude,
       stage_hash_mms_ClubAddress.MapZoomLevel map_zoom_level,
       stage_hash_mms_ClubAddress.UpdatedDateTime updated_date_time,
       stage_hash_mms_ClubAddress.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.ClubAddressID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubAddress.AddressLine1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubAddress.AddressLine2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubAddress.City,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_ClubAddress.Zip,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ClubAddress.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.Latitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.Longitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.MapCenterLatitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.MapCenterLongitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubAddress.MapZoomLevel as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ClubAddress.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ClubAddress
 where stage_hash_mms_ClubAddress.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_club_address records
set @insert_date_time = getdate()
insert into s_mms_club_address (
       bk_hash,
       club_address_id,
       address_line1,
       address_line2,
       city,
       zip_code,
       inserted_date_time,
       latitude,
       longitude,
       map_center_latitude,
       map_center_longitude,
       map_zoom_level,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_club_address_inserts.bk_hash,
       #s_mms_club_address_inserts.club_address_id,
       #s_mms_club_address_inserts.address_line1,
       #s_mms_club_address_inserts.address_line2,
       #s_mms_club_address_inserts.city,
       #s_mms_club_address_inserts.zip_code,
       #s_mms_club_address_inserts.inserted_date_time,
       #s_mms_club_address_inserts.latitude,
       #s_mms_club_address_inserts.longitude,
       #s_mms_club_address_inserts.map_center_latitude,
       #s_mms_club_address_inserts.map_center_longitude,
       #s_mms_club_address_inserts.map_zoom_level,
       #s_mms_club_address_inserts.updated_date_time,
       case when s_mms_club_address.s_mms_club_address_id is null then isnull(#s_mms_club_address_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_club_address_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_club_address_inserts
  left join p_mms_club_address
    on #s_mms_club_address_inserts.bk_hash = p_mms_club_address.bk_hash
   and p_mms_club_address.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_club_address
    on p_mms_club_address.bk_hash = s_mms_club_address.bk_hash
   and p_mms_club_address.s_mms_club_address_id = s_mms_club_address.s_mms_club_address_id
 where s_mms_club_address.s_mms_club_address_id is null
    or (s_mms_club_address.s_mms_club_address_id is not null
        and s_mms_club_address.dv_hash <> #s_mms_club_address_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_club_address @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_club_address @current_dv_batch_id

end
