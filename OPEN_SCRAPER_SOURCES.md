# Open Scraper – fuentes
Este scraper usa PDFs públicos (OPEN) para poblar:

- `public.standards_usa`
- `public.standards_cadda`

Fuentes actuales:
- USA Swimming Motivational Standards 2024-2028 (Age Group) (PDF)  -> `usa_2024_2028_age_group`
- CADDA Marcas Mínimas 2023/2024 (PDF) -> `cadda_minimas_2023_2024`

Notas:
- `tabula-py` requiere Java.
- El parser guarda CSVs debug en `out/debug/*` para ajustar heurísticas si cambia el layout del PDF.
