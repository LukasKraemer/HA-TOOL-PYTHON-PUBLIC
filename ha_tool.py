# -*- coding: utf-8 -*-
# HA - Tool version 2.1
# Lukas Krämer
# MIT License
# 2021

import os
import re
from shutil import move as move
from sys import exit as sys_exit
import threading
from time import sleep as sleep
from datetime import datetime
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv, dotenv_values


class HaTool:
    _fred = threading.Thread()  # thread management
    _lock = threading.Lock()  # thread lock
    _engine = None  # Database connection

    _config = dotenv_values(".env")  # Env vars
    _raw_data_table = _config["table_raw"]  # table raw uploaded
    _overview_table = _config["table_overview"]  # summary table
    _log_table = _config["created_trips_table"]  # log filename in db
    _path = _config["PathToTripData"]  # path to the target path for txt data
    _threads = _config["process"]  # number of processes

    def __init__(self):
        """check if System is ready configure"""
        load_dotenv()
        self._config = dotenv_values(".env")
        if not (os.path.isdir(self._path)):
            os.makedirs(self._path)
        self._login_value()
        self._todo_trips = []
        self._task_list = None

    def _login_value(self):
        """Connection to the Database and log """
        db_user = self._config["DB_USERNAME"]
        db_passwd = self._config["DB_PASSWORD"]
        db_ip = self._config["DB_HOST"]
        db_schema = self._config["DB_SCHEMA"]
        # [driver]://[username][password]@[IP]/[Schema in DB]
        db_uri = f'mysql+pymysql://{db_user}:{db_passwd}@{db_ip}:3306/{db_schema}'
        self._engine = sqlalchemy.create_engine(db_uri)  # connect to Database

        try:
            now = datetime.now()
            data = {'username': [db_user], "time": [now.strftime("%d/%m/%Y, %H:%M:%S")], "Remote": [db_ip],
                    "OS": ["RPI"]}
            pd.DataFrame(data).to_sql("python_log", con=self._engine, if_exists='append')
        except Exception:
            print("----------------\n\n Error while logging in Database\n\n----------------")
            sys_exit()

    def _get_last_trip(self, table, trip_id="trip_counter"):
        """return last trip on the Database"""
        try:
            return pd.read_sql_query(f'SELECT {trip_id} FROM {table} ORDER BY {trip_id} DESC limit 1;',
                                     con=self._engine)
        except Exception:
            print(f'last trip Error \n{table} \n{trip_id}\n--------------------')
            return pd.DataFrame({trip_id: [0]})

    def _get_last_trip_number(self):
        """return the number of the last recorded Trip"""
        try:
            start_trip_number = int(self._get_last_trip(self._overview_table, "trip_number").at[0, 'trip_number'])

            target_trip_number = self._get_last_trip(self._raw_data_table).at[0, 'trip_counter']
            if target_trip_number == start_trip_number:
                print("all uploaded")
                return -1
            else:
                return start_trip_number
        except Exception:
            print("Error")
            return 0

    def _getMissiongSummaryTrips(self):
        ids = []
        try:
            values = pd.read_sql_query(f'''SELECT trip_counter
                                            FROM {self._raw_data_table}
                                            WHERE {self._raw_data_table}.trip_counter NOT IN
                                            (SELECT  {self._overview_table}.trip_number FROM  {self._overview_table})
                                            group by trip_counter''',
                                       con=self._engine)

            for index, row in values.iterrows():
                ids.append(row['trip_counter'])
        except Exception:
            print("Summary not founded")
            values = pd.read_sql_query(f'''SELECT trip_counter FROM rawData order by trip_counter
                                        desc limit 1''', con=self._engine)

            for i in range(values['trip_counter'][0], 0, -1):
                ids.append(i)
        finally:
            return ids

    def _trip_handler(self, number_of_processes):
        """manage the Summary Calculator"""

        tasks = self._task_list
        # value = self._get_last_trip_number()

        for i in range(number_of_processes):
            self._todo_trips.append(tasks.pop())
        run = True
        while run:
            for i in range(number_of_processes):
                if self._todo_trips[i] == "next":
                    self._todo_trips[i] = tasks.pop()

            if len(tasks) == 0:
                run = False
        print("everything started")
        sys_exit()

    def _duplicate_check(self, filename):
        """check if file exist in Database"""
        try:
            trip_list = pd.read_sql_query(f'SELECT filename FROM {self._log_table};', con=self._engine)
            # Check if filename is registered in database
            for index, row in trip_list.iterrows():
                if row['filename'] == str(filename):
                    print("found duplicate")
                    return True
            return False

        except Exception:
            print("duplicate error")
            return False

    def _upload_trips_raw(self):
        """upload all txt files to DB"""
        path = self._path

        try:  # normal
            self._get_last_trip_number()
            counter = pd.read_sql_query(
                f"SELECT trip_counter FROM {self._raw_data_table} ORDER BY trip_counter DESC limit 1;",
                con=self._engine)  # get last trip number from Database
            finished = int(counter.at[0, 'trip_counter'])  # last trip number from Database
        except Exception:
            finished = 0

        regex = re.compile("Trip_20[1-3][0-9]-[0-2][0-9]-[0-3][0-9]_[0-3][0-9]-[0-9][0-9]-[0-9][0-9].txt")
        for file in os.listdir(path):
            if regex.match(file):
                values_of_file = pd.read_csv(path + file, sep='\t')

                if not self._duplicate_check(file):
                    finished = finished + 1
                else:
                    continue

                values_of_file['trip_counter'] = pd.DataFrame(
                    {'trip_counter': [finished for _ in range(len(values_of_file.index))]})
                values_of_file.to_sql(self._raw_data_table, con=self._engine, if_exists='append', index='counter')

                if not (os.path.isdir(path + "archive/")):
                    os.makedirs(path + "archive/")
                move(path + file, path + 'archive/')  # move finished file to archive

                trip_log = {'filename': [str(file)],
                            'Datum': [datetime.now().strftime("%d/%m/%Y, %H:%M:%S")]
                            }
                pd.DataFrame(trip_log).to_sql(self._log_table, con=self._engine, if_exists='append')
                del values_of_file
        sys_exit()

    @staticmethod
    def _dataframe_difference(df1, df2):
        """Find rows which are different between two DataFrames."""
        comparison_df = df1.merge(df2,
                                  indicator=True,
                                  how='outer')
        return comparison_df[comparison_df['_merge'] != 'both']

    def _calc_summary(self, process_id):
        """gen _calc_summary trip by trip"""

        try:
            if self._todo_trips[process_id] == "finished":
                sys_exit()
            timeout = 0
            while self._todo_trips[process_id] == "next":
                sleep(1)
                if timeout >= 12:
                    sys_exit()
                timeout += 1

            query = f"""
            SELECT * FROM {self._raw_data_table}
            WHERE trip_counter = {self._todo_trips[process_id]} ORDER BY time asc; """
            trip_values_database = pd.read_sql_query(query, self._engine)

            number_lines = trip_values_database.shape[0]
            if number_lines == 0:
                self._todo_trips[process_id] = "finished"
                exit()
            elif number_lines <= 20:
                self._todo_trips[process_id] = "next"
                self._calc_summary(process_id)
                return
            df4 = pd.DataFrame(columns=['soc'])

            for x in range(0, number_lines):  # remove all 0 from the Dataset
                if trip_values_database.at[x, 'soc'] != 0:
                    soc_val = float(trip_values_database.at[x, 'soc'])
                    df4 = df4.append({'soc': soc_val}, ignore_index=True)
            last_row = int(number_lines - 1)
            if df4.shape[0] != 0:
                c_soc_start = df4.at[0, "soc"]
                c_soc_end = trip_values_database['soc'][number_lines - 1]
            else:
                c_soc_start = 0
                c_soc_end = 0

            consumption_average = float(trip_values_database['tripfuel'][last_row]) / 10 / float(
                trip_values_database['trip_dist'][last_row])  # Consumption km / h

            ev_proportion = (float(trip_values_database['trip_ev_dist'][last_row]) * 100) / float(
                trip_values_database['trip_dist'][last_row])  # proportion of the usage of the electric engine

            driving_stop = float(trip_values_database['trip_nbs'][last_row]) - float(
                trip_values_database['trip_mov_nbs'][last_row])  # time of standing

            # dataset for Database
            regex = r"[0-2][0-9]:[0-5][0-9]"
            summary_value = {'trip_number': trip_values_database['trip_counter'][1],
                             'day': pd.to_datetime(trip_values_database['Date'][0]).date(),
                             'time_Begins': re.match(regex, trip_values_database['Time'][0].replace(" ", ""))[0],
                             'time_End': re.match(regex, trip_values_database['Time'][last_row].replace(" ", ""))[0],
                             'km_start': trip_values_database['odo'][0],
                             'km_end': trip_values_database['odo'][last_row],
                             'trip_length': round(trip_values_database['trip_dist'][last_row], 2),
                             'trip_length_ev': round(trip_values_database['trip_ev_dist'][last_row], 2),
                             'driving': round(trip_values_database['trip_nbs'][last_row] / 60, 2),
                             'driving_ev': round(trip_values_database['trip_ev_nbs'][last_row] / 60, 2),
                             'driving_move': round(trip_values_database['trip_mov_nbs'][last_row] / 60, 4),
                             'driving_stop': round(int(driving_stop) / 60, 4),
                             'fuel': round(float(trip_values_database['tripfuel'][last_row]), 0),
                             'outside_temp': round(float(trip_values_database['ambient_temp'].max()), 2),
                             'outside_temp_average': round(float(trip_values_database['ambient_temp'].mean()), 2),
                             'soc_average': round(float(trip_values_database['soc'].mean()), 2),
                             'soc_minimum': round(float(df4['soc'].min()), 2),
                             'soc_maximal': round(float(trip_values_database['soc'].max()), 2),
                             'soc_start': round(float(c_soc_start), 2),
                             'soc_end': round(float(c_soc_end), 2),
                             'consumption_average': round(float(consumption_average), 2),
                             'ev_proportion': [int(ev_proportion)],
                             'speed_average': int(trip_values_database['speed_obd'].mean()),
                             'speed_max': [trip_values_database['speed_obd'].max()],
                             'soc_change': round(int(c_soc_end) - int(c_soc_start), 2),
                             'rotation_speed_average': round(trip_values_database['ice_rpm'].mean(), 0),
                             'rotation_speed_max': [trip_values_database['ice_rpm'].max()],
                             'engine load_average': round(trip_values_database['ice_load'].mean(), 0),
                             'engine_load_max': [trip_values_database['ice_load'].max()],
                             'battery_temp_max': round(trip_values_database['battery_temp'].max(), 2),
                             'battery_temp_average': round(trip_values_database['battery_temp'].mean(), 2),
                             'battery_temp_min': round(trip_values_database['battery_temp'].min(), 2),
                             'engine_cooling_temperature_max': round(trip_values_database['ice_temp'].max(), 2),
                             'engine_cooling_temperature_average': round(trip_values_database['ice_temp'].mean(), 2),
                             'engine_cooling_temperature_min': round(trip_values_database['ice_temp'].min(), 2),
                             'electric_motor_temp_max': round(trip_values_database['mg_temp'].max(), 2),
                             'electric_motor_temp_average': round(trip_values_database['mg_temp'].mean(), 2),
                             'electric_motor_temp_min': round(trip_values_database['mg_temp'].min(), 2),
                             'inverter_motor_temp_max': round(trip_values_database['inverter_temp'].max(), 2),
                             'inverter_motor_temp_average': round(trip_values_database['inverter_temp'].mean(), 2),
                             'inverter_motor_temp_min': round(trip_values_database['inverter_temp'].min(), 2),
                             'indoor_temp_max': round(trip_values_database['inhaling_temp'].max(), 2),
                             'indoor_temp_average': round(trip_values_database['inhaling_temp'].mean(), 2),
                             'indoor_temp_min': round(trip_values_database['inhaling_temp'].min(), 2)
                             }

            overview_frame = pd.DataFrame(data=summary_value)
            del trip_values_database
            del summary_value

            self._lock.acquire()
            overview_frame.to_sql(self._overview_table,
                                  index= False,
                                  con=self._engine,
                                  if_exists='append')
            self._lock.release()
            del overview_frame

            self._todo_trips[process_id] = "next"
            self._calc_summary(process_id=process_id)

        except ZeroDivisionError:
            self._todo_trips[process_id] = "next"
            self._calc_summary(process_id=process_id)

        print("Overview finished")

    def start(self, program):
        """run the start with all parameter"""
        number_of_processes = self._threads
        if program == "trips":
            p1 = threading.Thread(target=self._upload_trips_raw)
            p1.start()
            p1.join(300)

        elif program == "calc_summary":
            self._task_list = self._getMissiongSummaryTrips()
            diff = len(self._task_list)

            thread_count = 0
            if diff == 0:
                print("no new values")
                sys_exit()
            elif diff < int(number_of_processes):
                print(f"less than {int(number_of_processes)} thread")
                thread_count = int(diff)
            else:
                thread_count = int(number_of_processes)
            threading.Thread(target=self._trip_handler, args=(thread_count,)).start()

            timeout = 0
            while timeout <= 15:
                if len(self._todo_trips) == thread_count:
                    break
                sleep(0.5)
            for i in range(int(thread_count)):
                threading.Thread(target=self._calc_summary, args=(i,)).start()

        else:
            print("unknown program")


if __name__ == "__main__":
    ha = HaTool()
    ha.start("trips")
    ha.start("calc_summary")
