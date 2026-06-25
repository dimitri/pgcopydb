-- Verify all matviews are populated on the target after pgcopydb clone.
-- Root 1: ts_stat view requires non-empty search_path at REFRESH time.
select count(*) > 0 as mv_word_stats_has_rows from public.mv_word_stats;
-- Root 2 and Root 3: simple aggregate views with known row counts.
select count(*) as mv_author_summary_count from mvtest.mv_author_summary;
select count(*) as mv_tag_stats_count from mvtest.mv_tag_stats;
-- Level 2 and 3 depend on roots -- verify they are populated.
select count(*) > 0 as mv_combined_has_rows from mvtest.mv_combined;
select count(*) > 0 as mv_final_has_rows from mvtest.mv_final;
