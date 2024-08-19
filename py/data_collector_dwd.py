#%%
import os
import time
import requests
import zipfile
import io

# Liste von Stations-IDs
stations = [
    '00078',  
    '00053',    
]

# ------------------------------------------------------------------------------
# Basis URL des DWD-Server
# ------------------------------------------------------------------------------
# >>> HIER URL des DWD-Server eintragen
base_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/1_minute/precipitation/now/"

# ------------------------------------------------------------------------------
# Ausgabe (Speicherort)
# ------------------------------------------------------------------------------
# >>> HIER Speicherort eintragen
output_dir = r'C:\Users\User\Desktop\stundendaten'

#%%

# ------------------------------------------------------------------------------
# Download und unzip
# ------------------------------------------------------------------------------
# >>> Eigentlicher Hauptteil des Scripts
def download_and_unzip_file(station_id):
    file_name = f"1minutenwerte_nieder_{station_id}_now.zip"
    url = base_url + file_name

    try:
        print(f"Downloading {file_name} from {url}")  #  Print der Anfrage
        response = requests.get(url)  # Anfrage an den Zielserver
        # Jede Antwort enthält einen Status-Code vom Server (z.B. 404)
        # die Funtkion wirft einen Fehler, sobald eine Antwort nicht den Code 200 (für alles ok) hat.
        response.raise_for_status()  # Überprüfe, ob der Download erfolgreich war

        # Zip-Datei in den Arbeitsspeicher laden
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(os.path.join(output_dir, station_id))
            print(f"Extracted {file_name} to {output_dir}/{station_id}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # HTTP Fehler anzeigen
    except Exception as err:
        print(f"Other error occurred: {err}")  # Andere Fehler anzeigen

# Diese Funktion wird jede Minute aufgerufen
def main():
    while True:
        for station in stations:
            download_and_unzip_file(station)
        
        # Warte eine Minute
        print("Waiting for the next cycle...")
        time.sleep(60)


if __name__ == "__main__":
    main()
# %%
