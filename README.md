# SensorHub

Repository of the SensorHub project.  

The SensorHub project was part of the DataScientest Data Engineering Course and this repository contains the results.

The goal of the project was to evaluate the possibilities of integrating different
sources of sensor data via the [FROST Server](https://github.com/FraunhoferIOSB/FROST-Server) by [Frauenhofer Institute](https://www.iosb.fraunhofer.de/de/projekte-produkte/frostserver.html).

## Install

The installation of SensorHub requires **python 3.10** (or higher) and **docker** (27.5 or higher).

The following steps need to be executed to install SensorHub:  

The SensorHub repository either needs to be cloned or copied to the local machine. It is recommended to create a virtual environment inside the SensorHub folder.  

With a prompt inside the folder the following commands need to be executed to create the environment and to install the required packages:  

> py -m venv .venv  

> .venv\Scripts\activate  

> py -m pip install -r requirements.txt  

The next step is to build the two docker images frost-init and frost-dashboard.  

The necessary docker files are in the folders docker\frost_init and docker\dash. To build the images run the following commands:  

> docker image build docker\frost_init -t frost-init:latest  

> docker image build docker\dash -t frost-dash:latest  

After building the images the SensorHub can be started by running docker-compose from the parent directory.  

> docker-compose up -d  

After all containers have been successfully started the python script to import have to be executed. The following scripts need to be run to initially import the stations (things) into the FROST server:  

py\data_importer_dwd_stations.py  

py\data_importer_nlwkn_stations.py  

py\data_importer_wsa_stations.py  

All three scripts can also be used to update existing stations in the FROST server.  

## Architecture

The architecture consist of the following docker container:

- Airflow ()
- FROST Server
- PostGIS
- Grafana
- Dash

## DataSources

- DWD
- [PegelOnline](https://www.pegelonline.wsv.de/gast/start)
- [NLWKN Groundwater](https://www.grundwasserstandonline.nlwkn.niedersachsen.de/Start)

## Sources

- [Sensor Things API](https://www.ogc.org/standard/sensorthings/)
- [FROST Server](https://www.iosb.fraunhofer.de/de/projekte-produkte/frostserver.html)
- [FROST Server GitHub](https://github.com/FraunhoferIOSB/FROST-Server)
- [FROST Server Documentation](https://fraunhoferiosb.github.io/FROST-Server/)
