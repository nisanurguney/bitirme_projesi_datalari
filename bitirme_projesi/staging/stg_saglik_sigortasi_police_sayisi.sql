select *
from {{ source('saglik','saglik_sigortasi_police_sayisi_clean_data') }}