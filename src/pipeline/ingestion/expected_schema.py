"""The API columns the pipeline expects: fieldName -> dataTypeName.

Copied from <API_BASE_URL>/api/views/ijzp-q8t2.json on 2026-11-26, without the
':@computed_region_*' columns (records never contain them). check_api_schema() compares
the live metadata with this; change it only together with the code that reads the column.

Types are the API's own names, not ours: 'community_area' is 'text' in the API even
though its values are whole numbers (silver stores it as INTEGER).
"""

EXPECTED_SCHEMA: dict[str, str] = {
    "id": "number",
    "case_number": "text",
    "date": "calendar_date",
    "block": "text",
    "iucr": "text",
    "primary_type": "text",
    "description": "text",
    "location_description": "text",
    "arrest": "checkbox",
    "domestic": "checkbox",
    "beat": "text",
    "district": "text",
    "ward": "number",
    "community_area": "text",
    "fbi_code": "text",
    "x_coordinate": "number",
    "y_coordinate": "number",
    "year": "number",
    "updated_on": "calendar_date",
    "latitude": "number",
    "longitude": "number",
    "location": "location",
}
