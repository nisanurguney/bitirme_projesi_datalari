select
    province,
    year,

    sum(case 
        when hospital_type = 'Özel - Private'
        then beds
        else 0
    end) as private_beds,

    sum(beds) as total_beds,

    round(
        safe_divide(
            sum(case 
                when hospital_type = 'Özel - Private'
                then beds
                else 0
            end),
            sum(beds)
        ),2
    ) as private_bed_ratio

from {{ ref('stg_hastane_yatak_sayilarinin_illere_gore_dagilimi') }}

group by province, year
order by province, year