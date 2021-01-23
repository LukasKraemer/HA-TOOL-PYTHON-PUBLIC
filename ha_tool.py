# -*- coding: utf-8 -*-
# HA - Tool version 2.1
# Lukas Krämer
# MIT License
# 2021

import time
import pandas as pd
import sqlalchemy
import pymysql
import sys
import shutil
import threading
import os
import configparser
import re
from datetime import datetime


class HaTool:
    # global scope var
    config = configparser.ConfigParser()  # config tool
    fred = threading.Thread()  # thread management
    lock = threading.Lock()  # thread lock
    login_db = False  # check if user is logged in the Database
    engine = None  # Database connection
    config.read("./config.ini")  # Config File

    # Table und Path
    raw_data_table = config.get("table", "raw_data_table")
    sprit_table = config.get("table", "sprit_table")
    overview_table = config.get("table", "overview_table")
    path = config.get("system", "path")
    logertable = config.get("table", "list_created_trips")

    DB_IP = config.get("database", "database_ip")
    DB_USER = config.get("database", "database_user")
    DB_PASSWD = config.get("database", "database_passwd")
    DB_PORT = config.get("database", "database_port")
    DB_SCHEMA = config.get("database", "database_schema")
    threads = config.get("database", "threads")
    todo_trips = list()

    def __init__(self):
        """check if System is ready configure"""
        path = self.path

        if path == "not set":
            path = "/var/ha-tools/"
        else:
            pass

        if not (os.path.isdir(path)):
            os.makedirs(path)
        self.login_value("yes")

    def login_value(self, create="No"):
        """Connection to the Database and log """
        if create != "No":
            # [driver]://[username][password]@[IP]/[Schema in DB]
            db_uri = f'mysql+pymysql://{self.DB_USER}:{self.DB_PASSWD}@{self.DB_IP}:{self.DB_PORT}/{self.DB_SCHEMA}'
            self.engine = sqlalchemy.create_engine(db_uri)  # connect to Database
        else:
            pass

        try:
            now = datetime.now()
            data = {'username': [self.DB_USER], "time": [now.strftime("%d/%m/%Y, %H:%M:%S")], "Remote": [self.DB_IP],
                    "OS": ["RPI"]}
            pd.DataFrame(data).to_sql("loger", con=self.engine, if_exists='append')
            self.login_db = True
        except Exception as e:
            print(e)
            self.login_db = False

    def lasttrip(self, tablename, tripid="trip_counter"):
        """get last trip on the Database"""
        try:
            return pd.read_sql_query(f'SELECT {tripid} FROM {tablename} ORDER BY {tripid} DESC limit 1;', con=self.engine)
        except Exception:
            print(f'lasttrip Error \n {tablename} \n {tripid}')
            data = {tripid: [0]}
            return pd.DataFrame(data)

    def tripermitteln(self):
        """gibt des zu verarbeitenden Trip zurück - Übersicht """

        try:
            counteru = self.lasttrip(self.overview_table, "trip_nummer")
            start = int(counteru.at[0, 'trip_nummer'])  # value um 1 erhoehen

            counterc = self.lasttrip(self.raw_data_table)
            ziel = counterc.at[0, 'trip_counter']
            if ziel == start:
                print("all uploaded")
                return -1
            else:
                return start
        except Exception:

            print("Error")
            return 0

    def trip_handler(self, processesanzahl):
        """Verwaltet die Trips, jeder processes bekommt ein trip- callback fehlt"""
        value = self.tripermitteln()
        for i in range(processesanzahl):
            value = value + 1
            self.todo_trips.append(value)

        z = 0

        while True:
            i = 0

            for i in range(processesanzahl):
                if self.todo_trips[i] == "next":
                    value = value + 1
                    self.todo_trips[i] = value

            for y in range(processesanzahl):
                if self.todo_trips[i] == 'finished':
                    z += 1
            if z == processesanzahl:
                sys.exit()

    def trips(self, move=False):
        """lädt die txt dateien auf die DB"""
        path = self.path
        engine = self.engine
        raw_data_table = self.raw_data_table

        finished = int()
        regex = re.compile("Trip_20[1-3][0-9]-[0-2][0-9]-[0-3][0-9]_[0-3][0-9]-[0-9][0-9]-[0-9][0-9].txt")
        if self.login_db:
            menge_trips = 0
            for file in os.listdir(path):
                if regex.match(file):
                    menge_trips = int(menge_trips) + 1
            for file in os.listdir(path):
                if regex.match(file):
                    valuesofthetxtdata = pd.read_csv(path + file, sep='\t')
                    zahl = valuesofthetxtdata.shape[0]
                    dupli = False

                    try:
                        try:
                            # prevents double uploading of txt files
                            triplist = pd.read_sql_query(f'SELECT * from {self.logertable};', con=engine)
                            for c in range(int(triplist.shape[0])):
                                if triplist['filename'][c] == file:
                                    databasecheck = pd.read_sql_query(
                                        f'SELECT odo FROM {raw_data_table} where counter=0 group by trip_counter;',
                                        con=engine)
                                    for val in databasecheck:
                                        if val == triplist['filename'][c] == zahl['odo']:
                                            print("duplicate file found!")
                                            shutil.move(path + file, path + 'fehler/')
                                            dupli = True
                                            break
                        except Exception:
                            pass

                    except Exception:
                        # can be triggered at the first start
                        print("Failure to find duplicates" + file)

                    if dupli:
                        finished = finished + 1
                        continue
                    try:  # normal
                        counter = pd.read_sql_query(
                            f"SELECT trip_counter FROM {raw_data_table} ORDER BY trip_counter DESC limit 1;",
                            con=engine)  # letzten Trip-counter aus der DB holen
                        counter.at[0, 'trip_counter'] = int(counter.at[0, 'trip_counter']) + 1  # value um 1 erhöhen
                    except Exception:
                        data = {'trip_counter': [1]}
                        counter = pd.DataFrame(data)

                    if zahl >= 10 and valuesofthetxtdata['speed_obd'].max() >= 10:
                        # wenn die Fahrt weniger als zirka 10 sekunden ging und man nciht schneller als 10km/h fuhr
                        pass

                    dbcounter = counter
                    dbcounterpotenz = counter

                    while dbcounter.size < zahl:
                        if zahl >= dbcounter.size * 2:
                            dbcounterpotenz = dbcounterpotenz.append(dbcounterpotenz, ignore_index=True)
                            dbcounter = dbcounter.append(dbcounterpotenz, ignore_index=True)
                        else:
                            if dbcounter.size + dbcounterpotenz.size < zahl:
                                dbcounter = dbcounter.append(dbcounterpotenz, ignore_index=True)
                                dbcounterpotenz = dbcounterpotenz.loc[0:dbcounterpotenz.size / 2]
                            elif dbcounterpotenz.size == 2:
                                dbcounter = dbcounter.append(dbcounterpotenz.loc[0], ignore_index=True)
                            else:
                                dbcounterpotenz = dbcounterpotenz.loc[0:dbcounterpotenz.size / 2]
                    del dbcounterpotenz, counter

                    new = valuesofthetxtdata.join(dbcounter)  # tripcounter plus values
                    new.to_sql(raw_data_table, con=engine, if_exists='append', index='counter')

                    if move:
                        if not (os.path.isdir(path + "Archiv/")):
                            os.makedirs(path + "Archiv/")
                        shutil.move(path + file, path + 'Archiv/')  # verschiebt die bearbeitete Datei ins archiv

                    # reset values
                    del dbcounter
                    finished = finished + 1
                    trip_loggend = {'filename': [str(file)],
                                    'Datum': [datetime.now().strftime("%d/%m/%Y, %H:%M:%S")]
                                    }
                    triplist = pd.DataFrame(trip_loggend)
                    triplist.to_sql(self.logertable, con=engine, if_exists='append')

        else:
            print("not logged into Database")

    @staticmethod
    def dataframe_difference(df1, df2, which=None):
        """Find rows which are different between two DataFrames."""
        comparison_df = df1.merge(df2,
                                  indicator=True,
                                  how='outer')
        if which is None:
            diff_df = comparison_df[comparison_df['_merge'] != 'both']
        else:
            diff_df = comparison_df[comparison_df['_merge'] == which]
        # diff_df.to_csv('data/diff.csv')
        return diff_df

    
    def overview(self, theard_nr):
        """gen overview trip by trip"""
        todo_trips = self.todo_trips
        raw_data_table = self.raw_data_table
        overview_table = self.overview_table
        engine = self.engine
        try:
            if todo_trips[theard_nr] == "finished":
                sys.exit()
            timeout = 0
            while todo_trips[theard_nr] == "next":
                time.sleep(5)
                if timeout >= 3:
                    sys.exit()
                timeout += 1

            trip_number = todo_trips[theard_nr]

            query = f"""
            SELECT * FROM {raw_data_table}
            WHERE trip_counter = {trip_number} ORDER BY Date asc; """
            trip_values_database = pd.read_sql_query(query, engine)
            number_lines = trip_values_database.shape[0]
            if number_lines == 0:
                todo_trips[theard_nr] = "finished"
                sys.exit()
            elif number_lines <= 20:
                todo_trips[theard_nr] = "next"
                time.sleep(0.5)
                self.overview(theard_nr)
            df4 = pd.DataFrame(columns=['soc'])

            for x in range(0, number_lines):  # alle 0er aus dem Datensatz hauen, die Akkuzelle kann nie 0 % haben
                if trip_values_database.at[x, 'soc'] != 0:
                    soc_val = float(trip_values_database.at[x, 'soc'])
                    df4 = df4.append({'soc': soc_val}, ignore_index=True)
            lastrow = int(number_lines - 1)

            c_soc_durchschnittlich = trip_values_database['soc'].mean()

            c_soc_start = df4.at[0, "soc"]

            c_soc_min = df4['soc'].min()

            c_soc_max = trip_values_database['soc'].max()

            c_soc_ende = trip_values_database['soc'][number_lines - 1]

            verbauch_durchschnitt = float(trip_values_database['tripfuel'][lastrow]) / 10 / float(
                trip_values_database['trip_dist'][lastrow])  # verbrauch km/l
            ev_anteil = (float(trip_values_database['trip_ev_dist'][lastrow]) * 100) / float(
                trip_values_database['trip_dist'][lastrow])  # Anteil der elektrisch gefahren wurde
            fahrzeit_stillstand = float(trip_values_database['trip_nbs'][lastrow]) - float(
                trip_values_database['trip_mov_nbs'][lastrow])  # Anteil der elektrisch gefahren wurde
            # der eigentliche Datensatz
            regex = "[0-2][0-9]:[0-5][0-9]"
            overviewvalues = {'trip_nummer': trip_values_database['trip_counter'][1],
                              'tag': pd.to_datetime(trip_values_database['Date'][0]).date(),
                              'uhrzeit_Beginns': re.match(regex, trip_values_database['Time'][0])[0],
                              'uhrzeit_Ende': re.match(regex, trip_values_database['Time'][lastrow])[0],
                              'kmstand_start': trip_values_database['odo'][0],
                              'kmstand_ende': trip_values_database['odo'][lastrow],
                              'trip_laenge': round(trip_values_database['trip_dist'][lastrow], 2),
                              'trip_laengeev': round(trip_values_database['trip_ev_dist'][lastrow], 2),
                              'fahrzeit': round(trip_values_database['trip_nbs'][lastrow] / 60, 2),
                              'fahrzeit_ev': round(trip_values_database['trip_ev_nbs'][lastrow] / 60, 2),
                              'fahrzeit_bewegung': round(trip_values_database['trip_mov_nbs'][lastrow] / 60, 4),
                              'fahrzeit_stillstand': round(int(fahrzeit_stillstand) / 60, 4),
                              'spritverbrauch': round(float(trip_values_database['tripfuel'][lastrow]), 0),
                              'max_aussentemperatur': round(float(trip_values_database['ambient_temp'].max()), 2),
                              'aussentemperatur_durchschnitt': round(float(trip_values_database['ambient_temp'].mean()),
                                                                     2),
                              'soc_durchschnitt': round(float(c_soc_durchschnittlich), 2),
                              'soc_minimum': round(float(c_soc_min), 2),
                              'soc_maximal': round(float(c_soc_max), 2),
                              'soc_start': round(float(c_soc_start), 2),
                              'soc_ende': round(float(c_soc_ende), 2),
                              'verbauch_durchschnitt': round(float(verbauch_durchschnitt), 2),
                              'ev_anteil': [int(ev_anteil)],
                              'geschwindichkeit_durchschnitt': int(trip_values_database['speed_obd'].mean()),
                              'geschwindichkeit_maximal': [trip_values_database['speed_obd'].max()],
                              'soc_veraenderung': round(int(c_soc_ende) - int(c_soc_start), 2),
                              'Durchschnittliche Drehzahl': round(trip_values_database['ice_rpm'].mean(), 0),
                              'Maximale Drehzahl': [trip_values_database['ice_rpm'].max()],
                              'Durchschnittliche Motorlast': round(trip_values_database['ice_load'].mean(), 0),
                              'Maximale Motorlast': [trip_values_database['ice_load'].max()],
                              'max_Batterietemperatur': round(trip_values_database['battery_temp'].max(), 2),
                              'Batterietemperatur_durchschnitt': round(trip_values_database['battery_temp'].mean(), 2),
                              'min_Batterietemperatur': round(trip_values_database['battery_temp'].min(), 2),
                              'max_Kühlertemperatur': round(trip_values_database['ice_temp'].max(), 2),
                              'Kühlertemperatur_durchschnitt': round(trip_values_database['ice_temp'].mean(), 2),
                              'min_Kühlertemperatur': round(trip_values_database['ice_temp'].min(), 2),
                              'max_Elektromotortemperatur': round(trip_values_database['mg_temp'].max(), 2),
                              'Elektromotortemperatur_durchschnitt': round(trip_values_database['mg_temp'].mean(), 2),
                              'min_Elektromotortemperatur': round(trip_values_database['mg_temp'].min(), 2),
                              'max_Invertertemperatur': round(trip_values_database['inverter_temp'].max(), 2),
                              'Invertertemperatur_durchschnitt': round(trip_values_database['inverter_temp'].mean(), 2),
                              'min_Invertertemperatur': round(trip_values_database['inverter_temp'].min(), 2),
                              'max_Innenraumtemperatur': round(trip_values_database['inhaling_temp'].max(), 2),
                              'Innenraumtemperatur_durchschnitt': round(trip_values_database['inhaling_temp'].mean(), 2),
                              'min_Innenraumtemperatur': round(trip_values_database['inhaling_temp'].min(), 2)
                              }

            overviewframe = pd.DataFrame(data=overviewvalues)
            del trip_values_database
            del overviewvalues

            self.lock.acquire()
            overviewframe.to_sql(overview_table,
                                 con=engine,
                                 index=True,
                                 index_label='id',
                                 if_exists='append')
            self.lock.release()
            del overviewframe
            todo_trips[theard_nr] = "next"
            time.sleep(0.1)
            self.overview(theard_nr=theard_nr)

        except ZeroDivisionError:
            todo_trips[theard_nr] = "next"
            time.sleep(0.3)
            self.overview(theard_nr=theard_nr)

        print("Overview finished")

    def programs(self, program, processes=1):
        """run the programs with all parameter"""
        raw_data_table = self.raw_data_table
        overview_table = self.overview_table
        threadsrunning = []
        if program == "trips":
            p1 = threading.Thread(target=self.trips, args=(True,))
            p1.start()
        elif program == "sprit":
            p2 = threading.Thread(target=self.sprit, args=(False,))
            p2.start()
        elif program == "overview":
            raw_trip = 0
            overview_trip = 0
            if self.lasttrip(raw_data_table)['trip_counter'][0] != 0:
                raw_trip = self.lasttrip(raw_data_table)['trip_counter'][0]
            if self.lasttrip(overview_table, "trip_nummer")['trip_nummer'][0] != 0:
                overview_trip = self.lasttrip(overview_table, "trip_nummer")['trip_nummer'][0]

            diff = raw_trip - overview_trip
            if diff == 0:
                print("no new values")
                sys.exit()
            elif diff < int(processes):
                print(f"less than {int(processes)} thread")
                thread_count = int(diff)
            else:
                thread_count = int(processes)
            p3 = threading.Thread(target=self.trip_handler, args=(thread_count,))
            p3.start()
            time.sleep(10)

            for i in range(int(thread_count)):
                threading.Thread()
                threadsrunning.append(threading.Thread(target=self.overview, args=(i,)))
                threadsrunning[i].start()

        else:
            print("unknown program")


ha = HaTool()
ha.programs(program="trips")
time.sleep(5)
ha.programs("overview", processes=int(ha.threads))
