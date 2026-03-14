import os
import logging
from dotenv import load_dotenv
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

def configure_logging() -> None:
    """Configure timestamped logs for upload progress and failures."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def main() -> None:
    """Upload local JSON files to the Bronze container."""
    configure_logging()

    load_dotenv()
    connect_str = os.getenv("AZURE_CONNECTION_STRING")
    if not connect_str:
        raise EnvironmentError("AZURE_CONNECTION_STRING was not found in the .env file.")

    container_name = "1-bronze"
    local_folder = "data_source"

    if not os.path.isdir(local_folder):
        raise FileNotFoundError(
            f"Local folder '{local_folder}' does not exist. Run 01_generate_data.py first."
        )

    json_files = sorted(name for name in os.listdir(local_folder) if name.endswith(".json"))
    if not json_files:
        logging.warning("No JSON files found in '%s'. Nothing to upload.", local_folder)
        return

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    logging.info("Connected to Azure. Starting upload to container '%s'.", container_name)

    uploaded_files = 0
    try:
        for file_name in json_files:
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
            file_path = os.path.join(local_folder, file_name)

            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            uploaded_files += 1
            if uploaded_files % 10 == 0:
                logging.info("Uploaded %s/%s files...", uploaded_files, len(json_files))
    except ResourceNotFoundError:
        logging.error("Bronze container '%s' was not found.", container_name)
        raise
    except (ClientAuthenticationError, HttpResponseError) as exc:
        logging.error("Azure authentication/authorization failure during upload: %s", exc)
        raise

    logging.info(
        "Done! Successfully uploaded all %s files to the Azure Bronze container.",
        uploaded_files,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Bronze upload failed.")
        raise
