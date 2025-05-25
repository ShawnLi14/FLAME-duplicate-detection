import requests
import zipfile
import io
import os

def download_and_extract_zip(url, zip_path, extract_to=None):
    """
    Downloads a ZIP file from the given URL and saves it locally.
    If extract_to is provided, also extracts all files into that directory.
    """
    # Stream download the ZIP
    response = requests.get(url, stream=True)
    response.raise_for_status()

    # Save ZIP to disk
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print(f"Saved ZIP file to: {zip_path}")

    # If an extract directory is given, unzip into it
    if extract_to:
        os.makedirs(extract_to, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(path=extract_to)
        print(f"Extracted all files to: {extract_to}")

if __name__ == "__main__":
    download_url = (
        "http://csla100w.princeton.edu:82/Home/DownloadCSV"
        "?region=null&subregion=null&metal=&denomination=&"
        "excavationsHoards=&year=325-750&showTotalValues=false&"
        "RulerID=null&datasets=6,1,4,2,5,3&VagueDates=0&ObjectIDs=&"
        "Reference=&Country=&YearFromExcavationDatesCS=&"
        "YearToExcavationDatesCS="
    )
    zip_filename = "flame_data.zip"
    extract_folder = "flame_data"

    download_and_extract_zip(download_url, zip_filename, extract_folder)
