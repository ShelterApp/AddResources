from shelterapputils.scraper_config import ScraperConfig
from shelterapputils.scraper_utils import main_scraper
from shelterapputils.utils import client

california_food_resources_scraper_config: ScraperConfig = ScraperConfig(
    source="CaliforniaFoodResources",
    data_url="https://controllerdata.lacity.org/api/views/v2mg-qsxf/rows.csv",
    data_format="CSV",
    extract_usecols=[
        "Name", "Street Address", "City", "State", "Zip Code",
        "County", "Phone", "Resource Type", "Web Link"
    ],
    drop_duplicates_columns=[
        "Name", "Street Address", "City", "State",
        "Zip Code", "County", "Phone"
    ],
    rename_columns={
        "Name": "name", "Street Address": "address",
        "City": "city", "State": "state", "Zip Code": "zip",
        "County": "county", "Phone": "phone",
        "Resource Type": "resource_type", "Web Link": "url"
    },
    service_summary="Food Pantry",
    check_collection="services",
    dump_collection="tmpCaliforniaFoodResources",
    dupe_collection="tmpCaliforniaFoodResourcesFoundDuplicates",
    data_source_collection_name="california_food_resources_scraper"
)


if __name__ == "__main__":
    main_scraper(client, california_food_resources_scraper_config)
