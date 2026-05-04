-- Q1: Top 10 reactions for warfarin
SELECT r.reaction_term, r.body_system,
       SUM(f.reaction_count) AS reports
FROM fda_rag.gold.fact_adverse_event f
JOIN fda_rag.gold.dim_drug d     ON f.drug_key = d.drug_key
JOIN fda_rag.gold.dim_reaction r ON f.reaction_key = r.reaction_key
WHERE d.drug_generic = 'warfarin'
GROUP BY r.reaction_term, r.body_system
ORDER BY reports DESC
LIMIT 10;

-- Q2: Cardiac reactions for metformin in patients 65+
SELECT p.age_band, r.reaction_term,
       SUM(f.reaction_count) AS reports,
       SUM(f.is_serious) AS serious_reports
FROM fda_rag.gold.fact_adverse_event f
JOIN fda_rag.gold.dim_drug d     ON f.drug_key = d.drug_key
JOIN fda_rag.gold.dim_reaction r ON f.reaction_key = r.reaction_key
JOIN fda_rag.gold.dim_patient p  ON f.patient_key = p.patient_key
WHERE d.drug_generic = 'metformin'
  AND r.body_system = 'cardiac'
  AND p.age_band IN ('65-74', '75+')
GROUP BY p.age_band, r.reaction_term
ORDER BY reports DESC;

-- Q3: Quarterly serious-event trend by drug
SELECT d.drug_generic, dt.year, dt.quarter,
       SUM(f.reaction_count) AS total_reactions,
       SUM(f.is_serious) AS serious_reactions,
       ROUND(100.0 * SUM(f.is_serious) / SUM(f.reaction_count), 2) AS pct_serious
FROM fda_rag.gold.fact_adverse_event f
JOIN fda_rag.gold.dim_drug d   ON f.drug_key = d.drug_key
JOIN fda_rag.gold.dim_date dt  ON f.date_key = dt.date_key
GROUP BY d.drug_generic, dt.year, dt.quarter
ORDER BY d.drug_generic, dt.year, dt.quarter;

-- Q4: Body-system heatmap data
SELECT d.drug_generic, r.body_system,
       SUM(f.reaction_count) AS reports
FROM fda_rag.gold.fact_adverse_event f
JOIN fda_rag.gold.dim_drug d     ON f.drug_key = d.drug_key
JOIN fda_rag.gold.dim_reaction r ON f.reaction_key = r.reaction_key
GROUP BY d.drug_generic, r.body_system
ORDER BY d.drug_generic, reports DESC;