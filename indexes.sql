-- LTE CQI
CREATE INDEX IF NOT EXISTS idx_lte_cqi_daily_site_date
ON lte_cqi_daily (site_att, date);

-- NR CQI
CREATE INDEX IF NOT EXISTS idx_nr_cqi_daily_site_date
ON nr_cqi_daily (site_att, date);

-- UMTS CQI
CREATE INDEX IF NOT EXISTS idx_umts_cqi_daily_site_date
ON umts_cqi_daily (site_att, date);

-- VoLTE (voice)
CREATE INDEX IF NOT EXISTS idx_volte_cqi_vendor_daily_site_date
ON volte_cqi_vendor_daily (site_att, date);


CREATE  INDEX IF NOT EXISTS idx_master_node_total_att_name
ON public.master_node_total (att_name);

CREATE INDEX IF NOT EXISTS idx_master_node_total_geog_func
 ON public.master_node_total
USING GIST ((ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography));
