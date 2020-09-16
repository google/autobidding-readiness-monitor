WITH adv_agg AS (
  SELECT
    advertiser_id,
    SUM(clicks) adv_clicks,
    SUM(conversions) as adv_conversions,
    SUM(viewable_imps) as adv_viewable_impressions,
    SUM(audible_visible_imps) as adv_audible_visible_imps,
    SUM(viewable_10s_imps) as adv_viewable_10s_imps
  FROM
   `{{ params.report_dataset }}.sdf_dv360_join`
  GROUP BY 1)

SELECT
  partner_id,
  partner,
  adv.advertiser_id,
  advertiser,
  campaign_id,
  insertion_order,
  insertion_order_id,
  line_item,
  line_item_id,
  line_item_type,
  li_bid_strategy,
  io_performance_goal,
  sum(imps) as imps,
  sum(clicks) as clicks,
  sum(conversions) as conversions,
  sum(viewable_imps) as viewable_imps,
  sum(audible_visible_imps) as audible_visible_imps,
  sum(viewable_10s_imps) as viewable_10s_imps,
  CASE
    WHEN sum(clicks) >= 100 AND sum(adv_clicks) >=10000 THEN "GREEN"
    WHEN sum(clicks) <80 OR sum(adv_clicks) <8000 THEN "RED"
    ELSE "ORANGE"
  END as CPC_Score,
  CASE
    WHEN sum(conversions) >= 100 THEN "GREEN"
    WHEN sum(conversions) <80 THEN "RED"
    ELSE "ORANGE"
  END as CPA_Score,
  CASE
    WHEN sum(conversions) >= 100 THEN "GREEN"
    WHEN sum(conversions) <80 THEN "RED"
    ELSE "ORANGE"
  END as CPI_Score,
  CASE
    WHEN sum(viewable_imps) >= 100 THEN "GREEN"
    WHEN sum(viewable_imps) <80 THEN "RED"
    ELSE "ORANGE"
  END as AV_Score,
  CASE
    WHEN sum(audible_visible_imps) >= 100 OR sum(adv_audible_visible_imps) >=5000 THEN "GREEN"
    WHEN sum(audible_visible_imps) <80 OR sum(adv_audible_visible_imps) <4000 THEN "RED"
    ELSE "ORANGE"
  END as CIVA_Score,
  CASE
    WHEN sum(viewable_10s_imps) >= 100 AND sum(adv_viewable_10s_imps) >=5000 THEN "GREEN"
    WHEN sum(viewable_10s_imps) <80 OR sum(adv_viewable_10s_imps) <4000 THEN "RED"
    ELSE "ORANGE"
  END as TOS_Score,
  CASE
    WHEN li_bid_strategy = "Fixed" THEN "Fixed"
    WHEN li_bid_strategy = "CPM" AND io_performance_goal = "CPM" THEN "Matched"
    WHEN li_bid_strategy = "CPA" AND io_performance_goal = "CPA" THEN "Matched"
    WHEN li_bid_strategy = "CPC" AND io_performance_goal = "CPC" THEN "Matched"
    WHEN (li_bid_strategy = "CPV" OR li_bid_strategy = "vCPM" OR li_bid_strategy = "IVO_TEN") AND io_performance_goal = "CPV" THEN "Matched"
    WHEN li_bid_strategy = "CIVA" AND io_performance_goal = "CPIAVC" THEN "Matched"
    WHEN li_bid_strategy = "CPC" AND io_performance_goal = "CTR" THEN "Matched"
    WHEN io_performance_goal = "% Viewability" OR io_performance_goal = "None" OR io_performance_goal = "Other" THEN "N/A"
    ELSE "Mismatched"
  END AS match_type,
sum(media_cost_7_days) as media_cost_past_7_days
FROM `{{ params.report_dataset }}.sdf_dv360_join` as li
JOIN adv_agg as adv
ON
  adv.advertiser_id = li.advertiser_id
WHERE line_item_type != "TrueView"
AND media_cost_7_days > 0
GROUP BY
  partner_id,
  partner,
  advertiser_id,
  advertiser,
  campaign_id,
  line_item,
  line_item_id,
  li_bid_strategy,
  io_performance_goal,
  line_item_type,
  insertion_order,
  insertion_order_id,
  match_type;


