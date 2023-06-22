DECLARE advertiser_id_list ARRAY <STRING> DEFAULT ['AR05537129768522088449','AR00513184064283344897', 'AR16343690797234782209', 'AR07918942709023244289', 'AR11891573877689548801', 'AR15322995736350556161', 'AR01201874992090841089', 'AR11825178974693097473'];


-- Totals by candidate
SELECT
  advertiser_id,
  advertiser_name,
  COUNT(*) AS ad_count,
  SUM( (CAST( SPLIT(impressions, "-")[0] AS INTEGER) +CAST (SPLIT(impressions, "-")[1] AS INTEGER)) / 2 ) AS total_impressions,
  SUM( ( spend_range_min_usd + spend_range_max_usd ) / 2 ) AS total_spend
FROM `bigquery-public-data.google_political_ads.creative_stats`
WHERE advertiser_id IN UNNEST(advertiser_id_list)
  AND first_served_timestamp > '2023-03-09'
  AND first_served_timestamp <= '2023-06-06'
GROUP BY advertiser_id, advertiser_name
ORDER BY total_spend;

