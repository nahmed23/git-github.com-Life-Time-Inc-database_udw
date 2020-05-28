CREATE VIEW [marketing].[v_dim_spabiz_segment] AS select d_dim_spabiz_segment.d_dim_spabiz_segment_id,
       d_dim_spabiz_segment.dim_spabiz_segment_key,
       d_dim_spabiz_segment.segment_id,
       d_dim_spabiz_segment.bucks_allocation_sequence,
       d_dim_spabiz_segment.segment_level_1,
       d_dim_spabiz_segment.segment_level_2,
       d_dim_spabiz_segment.segment_name
  from dbo.d_dim_spabiz_segment;