select *
from {{ source('saglik','hekim_sayilari_clean_data') }}