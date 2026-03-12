WITH beds AS (

SELECT
    province,
    year,
    SUM(beds) AS total_beds,

    SUM(CASE
        WHEN hospital_type = 'Özel - Private'
        THEN beds
        ELSE 0
    END) AS private_beds

FROM {{ ref('stg_hastane_yatak_sayilarinin_illere_gore_dagilimi') }}

GROUP BY province, year

),

population AS (

SELECT
    city,
    year,
    population
FROM {{ ref('stg_nufus_sayisi') }}

),

gdp AS (

SELECT
    city,
    year,
    hasila AS gdp_per_capita
FROM {{ ref('stg_gayri_safi_yurt_ici_hasila') }}

),

base AS (

SELECT
    p.city,
    p.year,

    (b.total_beds * 1000) / p.population AS beds_per_1000,

    SAFE_DIVIDE(b.private_beds, b.total_beds) AS private_hospital_ratio,

    g.gdp_per_capita

FROM population p

LEFT JOIN beds b
ON p.city = b.province
AND p.year = b.year

LEFT JOIN gdp g
ON p.city = g.city
AND p.year = g.year

),

stats AS (

SELECT
    MIN(beds_per_1000) AS min_beds,
    MAX(beds_per_1000) AS max_beds,
    MIN(private_hospital_ratio) AS min_private,
    MAX(private_hospital_ratio) AS max_private,
    MIN(gdp_per_capita) AS min_gdp,
    MAX(gdp_per_capita) AS max_gdp
FROM base

),

scored AS (

SELECT
    base.city,
    base.year,
    base.beds_per_1000,
    base.private_hospital_ratio,
    base.gdp_per_capita,

    ROUND(
      (base.beds_per_1000 - stats.min_beds) /
      NULLIF(stats.max_beds - stats.min_beds, 0)
    ,4) AS beds_score,

    ROUND(
      (base.private_hospital_ratio - stats.min_private) /
      NULLIF(stats.max_private - stats.min_private, 0)
    ,4) AS private_score,

    ROUND(
      (base.gdp_per_capita - stats.min_gdp) /
      NULLIF(stats.max_gdp - stats.min_gdp, 0)
    ,4) AS income_score

FROM base
CROSS JOIN stats

)

SELECT
    city,
    year,
    beds_per_1000,
    private_hospital_ratio,
    gdp_per_capita,
    beds_score,
    private_score,
    income_score,

    ROUND(
      100 * (
        0.4 * beds_score +
        0.3 * private_score +
        0.3 * income_score
      )
    ,2) AS market_potential_index

FROM scored
ORDER BY year, market_potential_index DESC