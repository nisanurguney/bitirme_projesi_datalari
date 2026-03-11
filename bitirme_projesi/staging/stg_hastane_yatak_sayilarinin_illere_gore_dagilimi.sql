select *
from {{ source('saglik','hastane_yatak_sayilarinin_illere_gore_dagilimi') }}