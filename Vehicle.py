from datetime import datetime, timedelta
import pandas
import pandas as pd
import json
import warnings


class Vehicle:
    def __init__(self, vehicle_id=None, read_database=None, write_database=None, logger=None):
        self.active_set_points = None
        self.set_points = None
        self.active_sensors = None
        self.open_events = None
        self.open_vehicle_events = None
        self.vehicle_type = None
        self.fleet_id = None
        self.vehicle_id = vehicle_id
        self.read_database = read_database
        self.write_database = write_database
        self.sensors = None
        self.offsets = None
        self.fleet_name = None
        self.fleet_vehicle_id = None
        self.meta_data_id = None
        self.logger = logger

    def get_vehicle_id(self, unique_id):
        if self.vehicle_id is None:
            vehicle_id_query = "select vehicle_id from meta_data where unique_id = '{0}' and active = 1".format(
                unique_id)
            vehicle_id_result = self.read_database.run_query(vehicle_id_query)

            self.vehicle_id = vehicle_id_result[0][0]

        return self.vehicle_id

    def get_vehicle_type(self):
        if self.vehicle_type is None:
            query = "SELECT vehicle_type FROM vehicle_meta_data WHERE vehicle_id = {0}".format(self.vehicle_id)
            vehicle_type_result = self.read_database.run_query(query)
            self.vehicle_type = vehicle_type_result[-1][0]
        return self.vehicle_type

    def get_sensors_and_setpoints(self, active=False, exclude_pump=True):
        return self.get_sensors_and_set_points_with_parameters(active, exclude_pump, self.set_points)

    def get_sensors_and_set_points_with_parameters(self, active, exclude_pump, set_point_attribute):
        if set_point_attribute is None:
            query = self.sensor_set_point_query_generator(active, exclude_pump)

            self.sensors = self.read_database.run_query(query)
            # Returns a dataframe that contains a table of unique_id's in the first column and set_points in the 2nd column
            set_point_attribute = pd.DataFrame(self.sensors, columns=['unique_id', 'set_point'])
        return set_point_attribute

    def sensor_set_point_query_generator(self, active, exclude_pump):
        query = "SELECT unique_id, set_point FROM meta_data WHERE vehicle_id = {0}".format(self.vehicle_id)
        #dont forget spaces are important when concatenating strings
        if active is True:
            query = query + " and active = 1"
        if exclude_pump is True:
            query = query + " and type != 'P'"
        return query

    def get_active_sensors_and_setpoints(self, active=True, exclude_pump=True):
        return self.get_sensors_and_set_points_with_parameters(active, exclude_pump, self.active_set_points)

    def get_sensor_pressure_offsets(self,start_of_analysis_date):
        if self.offsets is None:
            unique_id_string = "','".join(self.get_active_sensors())
            query = "select date,pressure_offset,unique_id from leak_detection_pressure_offsets where unique_id in ('{0}') and date = '{1}'".format(unique_id_string, start_of_analysis_date)
            offsets_result = self.read_database.run_query(query)
            self.offsets = pd.DataFrame(offsets_result, columns=['date', 'pressure_offset', 'unique_id'])
        return self.offsets

    def get_max_and_min_setpoints(self):
        # this is a stub, and we will add in functionality for this once we decide on how to implement the max
        # and min setpoints lookup
        setpoints_lookup = [100, 100, 100, 100, 110, 110]
        return max(setpoints_lookup), min(setpoints_lookup)

    def get_cycle_number(self):
        query = "SELECT cycle_number FROM meta_data WHERE vehicle_id = {}".format(self.vehicle_id)
        self.cycle_number = self.read_database.connection.execute(query).first().cycle_number
        return self.cycle_number

    def get_zip_files_in_json_ETL_format(self, bucket='apollo-endpoint-production', only_configs=True, event_id=None, time_window_begin=None, time_window_end=datetime.now()):
        """
        Description: Returns a list of all a cycle_number's configuration zip files in the format to be fed into the ETL
        Use this to repeatedly feed these configs into the ETL, recreating the configuration the cycle # should have.

        :param bucket: bucket to find the GW zip files in
        :param only_configs: True= only pull zip files containing configurations. False = pull ALL zip files for this GW
        :param event_id: it will pull the zip files 3 days prior to that event_id, and 1 day after
        :param time_window_begin: Alternative to event_id, you can give it a time window in which to pull zip files.  Leaving this (and event_id) as None will default to starting with the first zip file the GW sent
        :param time_window_end: The end of the time window.  Defaults to now()
        :return: list_of_json: list of json strings, where each json item is an ETL input

        The following code is an example of how to iterate through several configurations in the ETL using this code:
        vehicle = Vehicle(vehicle_id, grace)
        inputs = vehicle.get_zip_files_in_json_ETL_format()
        for input in inputs:
            ETL.apollo_etl('etl_test', connection, json.loads(input)['event'])
        """
        self.read_database.create_connection()
        cycle_number = self.get_cycle_number()

        if time_window_begin:
            time_window_for_query = "AND file_upload_time BETWEEN '{0}' AND '{1}'".format(time_window_begin, time_window_end)
        elif event_id:
            event_timestamp = self.get_event_id_timestamp(event_id)
            six_days_before_event_ts = event_timestamp - timedelta(days=6)
            two_days_after_event_ts = event_timestamp + timedelta(days=2)
            time_window_for_query = "AND file_upload_time BETWEEN '{0}' AND '{1}'".format(six_days_before_event_ts, two_days_after_event_ts)
        else:
            time_window_for_query = ""

        # Pulls just zip files with configurations in them
        configs_query = "SELECT DISTINCT(csv_file_name) FROM config_meta_data \
                                    JOIN file_meta_data fmd on config_meta_data.file_meta_data_id = fmd.id \
                                    WHERE fmd.cycle_number = {0} ORDER BY timestamp".format(cycle_number)

        # Pulls all zip files from the beginning (or within the given time window if there is one)
        not_just_configs_query = "SELECT DISTINCT(csv_file_name) FROM file_meta_data fmd " \
                    "WHERE fmd.cycle_number = {0} {1} ORDER BY file_upload_time".format(cycle_number, time_window_for_query)

        # Grab configurations first for any event_id or time window, since this is what will setup the truck so we can run other zip files
        pull_configs = only_configs is True or event_id is not None or time_window_begin is not None

        if pull_configs:
            files = self.read_database.run_query(configs_query)
        else:  # Grab everything, not just configurations
            files = self.read_database.run_query(not_just_configs_query)

        # if there is a given time window from an event_id or time_window_begin, then pull the data in that time window
        grab_zips_in_time_window = event_id is not None or time_window_begin is not None
        if grab_zips_in_time_window:
            files.extend(self.read_database.run_query(not_just_configs_query))

        list_of_json = []
        list_of_files = []

        for file in files:
            list_of_files.append(file)
            list_of_json.append({'event': {'Records': [{'s3': {'bucket': {'name': '{}'.format(bucket)},
                                                               'object': {'key': 'default'}}}]},
                                 'context': {}})
        for i, file in enumerate(files):
            list_of_json[i]['event']['Records'][0]['s3']['object']['key'] = list_of_files[i].csv_file_name
            list_of_json[i] = json.dumps(list_of_json[i])
        return list_of_json

    def get_open_events(self, unique_id):
        # Returns list of open events

        # TODO Create MySQL view that enables easier and faster querying of OPEN events
        get_events = "SELECT event_id, max(event_status_id) as max_event_status_id FROM event_table " \
                     "JOIN event_status USING(event_id) WHERE unique_id = '{0}' GROUP BY event_id".format(unique_id)

        events = pd.read_sql(get_events, self.read_database.connection)
        if len(events) > 0:
            list_of_events = events['max_event_status_id'].to_list()
            list_of_events = [str(event_status_id) for event_status_id in list_of_events]
            filter_open_events = "SELECT event_table.event_id, event_table.unique_id, event_table.event_type,severity,event_status.status,event_status.ts_created as status_created_at, " \
                                 "pressure_date " \
                                 "FROM event_table JOIN event_status USING(event_id) WHERE event_status_id IN ('{0}') " \
                                 "AND status in ('OPEN','SUSPECTED')".format("','".join(list_of_events))

            open_events = pandas.read_sql(filter_open_events,self.read_database.connection)
            if open_events.empty:
                return None
            else:
                return open_events
        else:
            return None

    def get_custom_underinflation_thresholds(self,logger):

        fleet_id = self.get_fleet_id()

        select_cap = "SELECT settings FROM custom_alert_parameters " \
                     "WHERE scope_type = 'ACCOUNT' AND scope_id = '{0}' LIMIT 1".format(fleet_id)
        custom_results = self.read_database.run_query(select_cap)
        result = None
        if len(custom_results) > 0:
            result = self.find_underinflation_settings(custom_results[0])
        if result is not None:
            return result
        global_query = "SELECT settings FROM custom_alert_parameters WHERE scope_type = 'GLOBAL' LIMIT 1"
        global_default = self.read_database.run_query(global_query)
        result = self.find_underinflation_settings(global_default[0])
        if result is None:
            logger.log_warning("unable to retrieve alert parameters from DB for fleet_id {0}".format(fleet_id))
            result = {'critical': 0.6, 'major': 0.8, 'minor': 0.85}
        return result

    def find_underinflation_settings(self,row):
        if row is not None:
            list_of_settings = json.loads(row[0])
            for setting in list_of_settings:
                if setting['type'] == 'UNDERINFLATION':
                    del setting['type']
                    return setting

    def get_fleet_id(self):
        if not self.fleet_id:
            fleet_id_query = "select fleet_id from vehicle_meta_data where vehicle_id = {0} and archived = 0".format(self.vehicle_id)
            fleet_id_result = self.read_database.run_query(fleet_id_query)
            self.fleet_id = fleet_id_result[0][0]

        return self.fleet_id

    def set_all_meta_data_to_inactive(self):
        stmt = 'UPDATE meta_data ' \
               'SET active = 0 ' \
               'WHERE vehicle_id = {}'.format(self.vehicle_id)
        self.write_database.run_statement(stmt)

    def filter_open_events_by_types(self, event_types):
        filtered_open_events = None
        if self.open_vehicle_events is not None:
            filtered_open_events = self.open_vehicle_events[self.open_vehicle_events.event_type.isin(event_types)]
            if filtered_open_events.empty:
                filtered_open_events = None
        return filtered_open_events

    def get_open_ui_events(self):
        self.populate_open_vehicle_events()
        event_types = ["UI"]
        return self.filter_open_events_by_types(event_types)

    def get_open_leak_events(self):
        self.populate_open_vehicle_events()
        event_types = ["LEAK"]
        return self.filter_open_events_by_types(event_types)

    def get_open_ui_leak_events(self):
        self.populate_open_vehicle_events()
        event_types = ["UI_LEAK"]
        return self.filter_open_events_by_types(event_types)

    def get_open_leak_and_ui_leak_events(self):
        self.populate_open_vehicle_events()
        event_types = ["UI_LEAK", "LEAK"]
        return self.filter_open_events_by_types(event_types)

    def get_open_vehicle_events(self):
        self.populate_open_vehicle_events()
        return self.open_vehicle_events

    def populate_open_vehicle_events(self):
        if self.open_vehicle_events is None:
            unique_id_string = "','".join(self.get_active_sensors())
            open_event_query ="select event_table.event_id, event_status_id, event_table.unique_id, event_table.event_type,severity,es.status,es.ts_created as status_created_at from event_table join event_status es on event_table.event_id = es.event_id where unique_id in ('{}')".format(unique_id_string)
            vehicle_events = pd.read_sql(open_event_query,con=self.read_database.connection)
            max_status_ids = vehicle_events.groupby("event_id").event_status_id.max().to_list()
            max_events = vehicle_events[vehicle_events.event_status_id.isin(max_status_ids)]
            self.open_vehicle_events = max_events[max_events.status=="OPEN"]

    def get_active_sensors(self):
        if self.active_sensors is None:
            self.active_sensors = self.get_active_sensors_and_setpoints().unique_id.to_list()
        return self.active_sensors

    def get_fleet_name_for_vehicle(self):
        if self.fleet_name is None:
            query = "SELECT fleet_name FROM fleet_meta_data WHERE fleet_id = {0}".format(self.fleet_id)
            fleet_name_result = self.read_database.run_query(query)
            self.fleet_name = fleet_name_result[-1][0]
        return self.fleet_name

    def get_fleet_vehicle_id(self):
        if self.fleet_id:
            fleet_vehicle_id_query = "select fleet_vehicle_id from vehicle_meta_data where " \
                                     "vehicle_id = {0} and archived = 0".format(
                self.vehicle_id)
            fleet_vehicle_id_result = self.read_database.run_query(fleet_vehicle_id_query)
            self.fleet_vehicle_id = fleet_vehicle_id_result[0][0]

        return self.fleet_vehicle_id

    def get_meta_data_id(self, unique_id):
        if unique_id:
            meta_data_query = "SELECT MAX(id) FROM meta_data " \
                              "WHERE vehicle_id = {0} and " \
                              "unique_id = '{1}' and active = 1".format(self.vehicle_id, unique_id)
            meta_data_id_result = self.read_database.run_query(meta_data_query)
            self.meta_data_id = meta_data_id_result[0][0]

            return self.meta_data_id

    def get_event_id_timestamp(self, event_id):
        """
        Returns the pressure_date for the event_id given
        :param event_id: event_id in question
        :return: event_timestamp: datetime format of pressure_date.  Returns None if the event_id isn't found
        """

        query = "SELECT pressure_date FROM event_table WHERE event_id = {}".format(event_id)
        event_timestamp = self.read_database.run_query(query)
        if event_timestamp:
            event_timestamp = event_timestamp[0].pressure_date
            return event_timestamp
        else:
            return None

    def get_sensor_wheel_position(self, unique_id):
        md_id = self.get_meta_data_id(unique_id)
        query = "SELECT CONCAT(side, axle, position, type) as wheel_position FROM meta_data WHERE id = {}".format(md_id)
        wheel_position = self.read_database.run_query(query)
        if wheel_position:
            wheel_position = wheel_position[0].wheel_position
            return wheel_position
        else:
            return None

