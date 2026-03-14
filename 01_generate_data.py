import json
import logging
import random
import uuid
import os
from datetime import datetime, timedelta

# Define Porsche-specific dimensions used to generate mock sales data.
MODELS = ["911 Carrera", "Taycan", "Cayenne", "Panamera", "718 Cayman", "Macan", "911 GT3"]
COUNTRIES = ["Germany", "Romania", "USA", "China", "France", "UK"]

# Create a local staging folder before uploading files to Azure.
LOCAL_FOLDER = "data_source"
os.makedirs(LOCAL_FOLDER, exist_ok=True)
FILES_TO_GENERATE = 100


def configure_logging() -> None:
    """Configure timestamped logs for generation progress and failures."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def generate_porsche_sale() -> dict:
    """Return one randomly generated Porsche sale record."""
    # Pick a sale date from the last 30 days.
    sale_date = datetime.now() - timedelta(days=random.randint(0, 30))

    # Inject occasional bad records (5% chance of price being 0).
    is_error = random.random() < 0.05
    base_price = 0 if is_error else random.randint(70000, 250000)
    model_name = random.choice(MODELS)

    return {
        "sale_id": str(uuid.uuid4()),
        "vin_number": f"WP0ZZZ91Z{random.randint(10000, 99999)}",  # Simplified Porsche VIN-like pattern
        "model_name": model_name,
        "price": base_price,
        "currency": "EUR",
        "country": random.choice(COUNTRIES),
        "sale_timestamp": sale_date.isoformat(),
        "is_electric": model_name == "Taycan",
    }


def main() -> None:
    """Generate mock Porsche sale JSON files in the local source folder."""
    configure_logging()
    logging.info("Starting data generation into folder '%s'.", LOCAL_FOLDER)

    for i in range(1, FILES_TO_GENERATE + 1):
        sale_data = generate_porsche_sale()

        file_name = f"{LOCAL_FOLDER}/sale_{sale_data['sale_id']}.json"
        with open(file_name, "w", encoding="utf-8") as file_obj:
            json.dump(sale_data, file_obj, indent=4)

        if i % 10 == 0:
            logging.info("Generated %s/%s files...", i, FILES_TO_GENERATE)

    logging.info("Done! Generated %s JSON files in '%s'.", FILES_TO_GENERATE, LOCAL_FOLDER)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Data generation failed.")
        raise
