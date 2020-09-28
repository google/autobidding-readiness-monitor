# Calculate spend in the past 7 days per LI
WITH spend_agg AS (
  SELECT
    line_item_id,
    sum(media_cost) as media_cost_7_days
  FROM
    `{{ params.dv_report_dataset }}`
  WHERE
   CAST(REPLACE(date, "/", "-") as DATE) <= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
  GROUP BY line_item_id)

# Aggregate SDF and DV360 data
SELECT
  dv.partner,
  dv.partner_id,
  dv.advertiser,
  dv.advertiser_id,
  dv.campaign_id,
  dv.insertion_order,
  dv.insertion_order_id,
  dv.line_item,
  dv.line_item_id,
  lisdf.Type as line_item_type,
  lisdf.bid_strategy_type as li_bid_strategy_type,
  lisdf.bid_strategy_unit as li_bid_strategy_unit,
  CASE
    WHEN lisdf.bid_strategy_type = "Fixed" THEN "Fixed"
    WHEN lisdf.bid_strategy_type = "Optimize VCPM" THEN "vCPM"
    WHEN lisdf.bid_strategy_type = "Optimize VCPM" THEN "vCPM"
    WHEN lisdf.bid_strategy_type IS NULL THEN "Null"
    ELSE
      CASE
        WHEN lisdf.bid_strategy_unit = "CPA" THEN "CPA"
        WHEN lisdf.bid_strategy_unit = "CPC" THEN "CPC"
        WHEN lisdf.bid_strategy_unit = "CIVA" THEN "CIVA"
        WHEN lisdf.bid_strategy_unit = "IVO_TEN" THEN "IVO_TEN"
        WHEN lisdf.bid_strategy_unit = "AV_VIEWED" THEN "AV_VIEWED"
        WHEN lisdf.bid_strategy_unit = "INCREMENTAL_CONVERSIONS" THEN "INCREMENTAL_CONVERSIONS"
        ELSE "Custom"
      END
  END AS li_bid_strategy,
  iosdf.performance_goal_type as io_performance_goal,
  sum(dv.impressions) as imps,
  sum(dv.clicks) as clicks,
  sum(dv.conversions) as conversions,
  sum(dv.active_view_viewable_impressions) as viewable_imps,
  sum(dv.Active_View_Audible_Visible_on_Completion_Impressions) as audible_visible_imps,
  sum(dv.Active_View_Viewable_10_Seconds) as viewable_10s_imps,
  sum(sa.media_cost_7_days) as media_cost_7_days
FROM
  `{{ params.dv_report_dataset }}` dv
JOIN
   `{{ params.sdf_report_dataset }}.SDFLineItem` lisdf
ON dv.line_item_id = CAST(lisdf.Line_item_id AS INT64)
JOIN
   `{{ params.sdf_report_dataset }}.SDFInsertionOrder` iosdf
ON dv.insertion_order_id = CAST(iosdf.Io_id AS INT64)
JOIN
  spend_agg sa
ON dv.line_item_id = sa.line_item_id
WHERE iosdf.status = "Active"
AND lisdf.status = "Active"
GROUP BY
  partner,
  partner_id,
  advertiser,
  advertiser_id,
  campaign_id,
  line_item,
  line_item_id,
  insertion_order,
  insertion_order_id,
  li_bid_strategy_type,
  li_bid_strategy_unit,
  io_performance_goal,
  line_item_type;


