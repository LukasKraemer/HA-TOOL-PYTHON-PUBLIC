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
from datetime import datetime
import pandas as pd
import sqlalchemy
import pymysql
from dotenv import load_dotenv, dotenv_values


class HaTool:
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
        if not self.login_value():
            sys_exit()
        self._todo_trips = []
        self._task_list = None

    def login_value(self):
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
            return True
        except Exception:
            print("----------------\n\n Error while logging in Database\n\n----------------")
            return False

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
                return -1
            else:
                return start_trip_number
        except Exception:
            print("Error")
            return 0

    def _getMissingSummaryTrips(self):
        ids = []
        try:
            values = pd.read_sql_query(f'''SELECT trip_counter
                                            FROM {self._raw_data_table}
                                            WHERE {self._raw_data_table}.trip_counter NOT IN
                                            (SELECT  {self._overview_table}.trip_number FROM  {self._overview_table})
                                            group by trip_counter''',
                                       con=self._engine)
            if os.getenv("ignoreList") is not None:
                founded = values['trip_counter'].tolist()
                ignore_list = list(map(int, os.getenv("ignoreList").split(" ")))
                ids = [x for x in founded if x not in ignore_list]
            else:
                ids = values['trip_counter'].tolist()

        except Exception:
            print("Summary not founded")
            values = pd.read_sql_query(f'''SELECT trip_counter FROM {self._raw_data_table} order by trip_counter
                                        desc limit 1''', con=self._engine)
            for i in range(values['trip_counter'][0], 0, -1):
                ids.append(i)
        finally:
            return ids

    def _trip_handler(self, number_of_processes):
        """manage the Summary Calculator"""
        tasks = self._task_list

        threads = [None] * number_of_processes
        for i in range(int(number_of_processes)):
            next_Trip = tasks.pop()
            threads[i] = threading.Thread(target=self._calc_summary, args=(next_Trip,))
            threads[i].start()

        while len(threads) != 0:
            for idx, val in enumerate(threads):
                if not val.is_alive():
                    threads.remove(val)
                    try:
                        next_Trip = tasks.pop()
                        new_thread = threading.Thread(target=self._calc_summary, args=(next_Trip,))
                        new_thread.start()
                        threads.append(new_thread)
                    except IndexError:
                        pass
        print("finish")
        sys_exit()

    def _duplicate_check(self, filename):
        """check if file exist in Database"""
        trip_list = pd.read_sql_query(
            f'SELECT count(filename) as founded FROM {self._log_table} where filename = "{filename}";',
            con=self._engine)
        return trip_list['founded'].iloc[0]

    def _upload_trips_raw(self, filelist):
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

        for file in filelist:
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

            trip_log = {'index': [finished],
                        'filename': [str(file)],
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

    def get_overview_data_from_database(self, trip_id):
        try:
            query = f"""SELECT * FROM {self._overview_table}
                    WHERE trip_number = {trip_id}; """
            return pd.read_sql_query(query, self._engine)
        except Exception:
            return pd.DataFrame()

    def get_raw_data_from_database(self, trip_id):
        try:
            query = f"""SELECT * FROM {self._raw_data_table}
                    WHERE trip_counter = {trip_id} ORDER BY time; """
            return pd.read_sql_query(query, self._engine)
        except Exception:
            return pd.DataFrame()

    def _calc_summary(self, trip_id):
        """gen _calc_summary trip by trip"""
        raw_data = self.get_raw_data_from_database(trip_id)
        overview_value = self.create_overview_value(raw_data)
        self._lock.acquire()
        overview_value.to_sql(self._overview_table,
                              index=False,
                              con=self._engine,
                              if_exists='append')
        self._lock.release()

    @staticmethod
    def create_overview_value(raw_data):
        number_lines = raw_data.shape[0]
        if number_lines == 0:
            return
        elif number_lines <= 20:
            return
        df4 = pd.DataFrame(columns=['soc'])
        df4 = []
        for x in range(0, number_lines):  # remove all 0 from the Dataset
            if raw_data.at[x, 'soc'] != 0:
                soc_val = float(raw_data.at[x, 'soc'])
                df4.append(soc_val)
        last_row = int(number_lines - 1)
        if len(df4) != 0:
            c_soc_start = float(df4[0])
            c_soc_end = float(df4[len(df4) - 1])
        else:
            c_soc_start = 0
            c_soc_end = 0
        soc_min = 100
        for i in df4:
            if float(i) < float(soc_min):
                soc_min = float(i)

        try:
            consumption_average = float(raw_data['tripfuel'][last_row]) / 10 / float(
                raw_data['trip_dist'][last_row])  # Consumption km / h

            ev_proportion = (float(raw_data['trip_ev_dist'][last_row]) * 100) / float(
                raw_data['trip_dist'][last_row])  # proportion of the usage of the electric engine

            driving_stop = float(raw_data['trip_nbs'][last_row]) - float(
                raw_data['trip_mov_nbs'][last_row])  # time of standing

            # dataset for Database
            summary_value = {'trip_number': raw_data['trip_counter'][1],
                             'day': pd.to_datetime(raw_data['Date'][0]).date(),
                             'time_Begins': str(raw_data['Time'][0])[-8:-3],
                             'time_End': str(raw_data['Time'][last_row])[-8:-3],
                             'km_start': raw_data['odo'][0],
                             'km_end': raw_data['odo'][last_row],
                             'trip_length': round(raw_data['trip_dist'][last_row], 2),
                             'trip_length_ev': round(raw_data['trip_ev_dist'][last_row], 2),
                             'driving': round(raw_data['trip_nbs'][last_row] / 60, 2),
                             'driving_ev': round(raw_data['trip_ev_nbs'][last_row] / 60, 2),
                             'driving_move': round(raw_data['trip_mov_nbs'][last_row] / 60, 4),
                             'driving_stop': round(int(driving_stop) / 60, 4),
                             'fuel': round(float(raw_data['tripfuel'][last_row]), 0),
                             'outside_temp': round(float(raw_data['ambient_temp'].max()), 2),
                             'outside_temp_average': round(float(raw_data['ambient_temp'].mean()), 2),
                             'soc_average': round(float(raw_data['soc'].mean()), 2),
                             'soc_minimum': round(float(soc_min), 2),
                             'soc_maximal': round(float(raw_data['soc'].max()), 2),
                             'soc_start': round(float(c_soc_start), 2),
                             'soc_end': round(float(c_soc_end), 2),
                             'consumption_average': round(float(consumption_average), 2),
                             'ev_proportion': [int(ev_proportion)],
                             'speed_average': int(raw_data['speed_obd'].mean()),
                             'speed_max': [raw_data['speed_obd'].max()],
                             'soc_change': round(int(c_soc_end) - int(c_soc_start), 2),
                             'rotation_speed_average': round(raw_data['ice_rpm'].mean(), 0),
                             'rotation_speed_max': [raw_data['ice_rpm'].max()],
                             'engine load_average': round(raw_data['ice_load'].mean(), 0),
                             'engine_load_max': [raw_data['ice_load'].max()],
                             'battery_temp_max': round(raw_data['battery_temp'].max(), 2),
                             'battery_temp_average': round(raw_data['battery_temp'].mean(), 2),
                             'battery_temp_min': round(raw_data['battery_temp'].min(), 2),
                             'engine_cooling_temperature_max': round(raw_data['ice_temp'].max(), 2),
                             'engine_cooling_temperature_average': round(raw_data['ice_temp'].mean(), 2),
                             'engine_cooling_temperature_min': round(raw_data['ice_temp'].min(), 2),
                             'electric_motor_temp_max': round(raw_data['mg_temp'].max(), 2),
                             'electric_motor_temp_average': round(raw_data['mg_temp'].mean(), 2),
                             'electric_motor_temp_min': round(raw_data['mg_temp'].min(), 2),
                             'inverter_motor_temp_max': round(raw_data['inverter_temp'].max(), 2),
                             'inverter_motor_temp_average': round(raw_data['inverter_temp'].mean(), 2),
                             'inverter_motor_temp_min': round(raw_data['inverter_temp'].min(), 2),
                             'indoor_temp_max': round(raw_data['inhaling_temp'].max(), 2),
                             'indoor_temp_average': round(raw_data['inhaling_temp'].mean(), 2),
                             'indoor_temp_min': round(raw_data['inhaling_temp'].min(), 2)
                             }
        except Exception as e:
            print(e)
            summary_value = {}
        finally:
            return pd.DataFrame(data=summary_value)

    def start(self, program):
        """run the start with all parameter"""
        number_of_processes = self._threads
        if program == "trips":
            new_files = []
            new_file = False
            regex = re.compile("Trip_20[1-3]\d-[0-2]\d-[0-3]\d_[0-3]\d-\d\d-\d\d.txt")
            for file in os.listdir(self._path):
                if regex.match(file):
                    new_files.append(file)
                    new_file = True

            if new_file:
                p1 = threading.Thread(target=self._upload_trips_raw, args=(new_files,))
                p1.start()
                p1.join(300)

        elif program == "calc_summary":
            self._task_list = self._getMissingSummaryTrips()
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
        else:
            print("unknown program")


if __name__ == "__main__":
    ha = HaTool()
    ha.start("trips")
    ha.start("calc_summary")
