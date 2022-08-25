from datetime import datetime
from unittest import TestCase
from Vehicle import Vehicle
from Database import Localhost
import pandas as pd


class TestVehicle(TestCase):
    def setUp(self):
        self.db = Localhost('vehicle_test')
        self.db.setupDb('../_database_setup/vehicle_db.sql')
        self.vehicle = Vehicle(1, self.db, self.db)

    def tearDown(self):
        self.db.cleanUpDB()
        pass

    def test_get_vehicle_by_vehicle_id(self):
        self.assertEqual(self.vehicle.vehicle_id, 1)

    def test_get_vehicle_type(self):
        self.assertEqual(self.vehicle.get_vehicle_type(), "buckaroo")

    def test_get_sensors_and_setpoints(self):
        sensors = self.vehicle.get_sensors_and_setpoints()
        self.assertEqual(10, sensors.shape[0])
        self.assertEqual(100, sensors[sensors['unique_id'] == '3421_1F077A']['set_point'][0])

    def test_get_active_sensors(self):
        self.db.run_statement("""INSERT INTO vehicle_test.meta_data (position, type, side, axle, set_point, cycle_number, sensor_number, md_id,
                                    unique_id, fleet_name, vehicle_id, active, halo_id, sensor_attribute_id, tire_make,
                                    tire_diameter, tire_width, tire_aspect_ratio, tire_load_rating, tire_id, created_at,
                                    deactivated_at)
VALUES ('O', 'T', 'L', 3, 100, 3421, '9DEC42', null, '3421_9DEC42', null, 1, 0, null, null, null, null, null, null,
        null, null, '2021-09-01 16:25:22', null); """)
        sensors = self.vehicle.get_active_sensors_and_setpoints()
        self.assertEqual(10, sensors.shape[0])
        self.assertEqual(100, sensors[sensors['unique_id'] == '3421_1F077A']['set_point'][0])

    def test_get_sensor_pressure_offsets(self):
        add_a_row = "INSERT INTO vehicle_test.leak_detection_pressure_offsets (date, pressure_offset, pressure_count, unique_id) VALUES('2020-12-06', -1, 288, '3421_9DEC42');"
        self.db.run_statement(add_a_row)
        offsets = self.vehicle.get_sensor_pressure_offsets(start_of_analysis_date='2020-11-30')
        self.assertEqual(5, offsets.shape[0])
        self.assertEqual(-1.0, offsets[offsets['unique_id'] == '3421_9DAD06']['pressure_offset'].item())

    def test_get_max_and_min_setpoint(self):
        max, min = self.vehicle.get_max_and_min_setpoints()
        self.assertEqual(110, max)
        self.assertEqual(100, min)

    def test_get_cycle_number(self):
        cycle_number = self.vehicle.get_cycle_number()
        self.assertEqual(3421, cycle_number)

    def test_get_events(self):
        open_events = self.vehicle.get_open_events('3421_1F077A')
        open_list = open_events.loc[0:].values.tolist()
        self.assertEqual([2202672, '3421_1F077A', 'LEAK', 'CRITICAL','OPEN'],
                         open_list[0][:-2])

    def test_get_fleet_id(self):
        fleet_id = self.vehicle.get_fleet_id()
        self.assertEqual(1, fleet_id)
        # the second call does not go to the DB
        fleet_id = self.vehicle.get_fleet_id()
        self.assertEqual(1, fleet_id)

    def test_global_ui_threshold(self):
        self.db.populate_db('../_database_setup/custom_alert_params.sql')
        from logging import Logger
        logger = Logger("mush")
        params = self.vehicle.get_custom_underinflation_thresholds(logger)
        self.assertEqual(0.85, params['minor'])
        self.assertEqual(0.8, params['major'])
        self.assertEqual(0.6, params['critical'])

    def test_custom_ui_threshold(self):
        self.db.populate_db('../_database_setup/custom_alert_params.sql')
        from logging import Logger
        logger = Logger("mush")
        self.vehicle.fleet_id = 33254
        params = self.vehicle.get_custom_underinflation_thresholds(logger)
        self.assertEqual(0.91, params['minor'])
        self.assertEqual(0.82, params['major'])
        self.assertEqual(0.68, params['critical'])

    def test_set_all_meta_data_to_inactive(self):
        self.db.populate_db('../_database_setup/sql_inserts_meta_data_3_cycles_2_days_lite.sql')
        self.vehicle.vehicle_id = 2527
        self.vehicle.set_all_meta_data_to_inactive()
        results = pd.read_sql('SELECT * FROM meta_data', self.db.connection)
        self.assertFalse(results[results['vehicle_id'] == 2527]['active'].any(), "Not setting active rows to inactive")
        self.assertEqual(14, results.loc[(results['vehicle_id'] == 2508) & (results['active'] == 1), :].count().id, "vehicle_id 2508 should be untouched, but it has a different number of active sensors")

    def test_get_vehicle_id(self):
        from Logger import Datadog
        logger = Datadog()

        vehicle = Vehicle(read_database=self.db, write_database=self.db, logger=logger)
        self.db.populate_db('../_database_setup/sql_inserts_meta_data_3_cycles_2_days_lite.sql')
        vehicle_id = vehicle.get_vehicle_id(unique_id='2122_21448B')
        self.assertEqual(2527, vehicle_id)

    def test_get_event_timestamp(self):
        vehicle = Vehicle(read_database=self.db, write_database=self.db)
        self.db.populate_db('../_database_setup/test_get_event_timestamp.sql')
        event_id = 1
        timestamp = vehicle.get_event_id_timestamp(event_id)
        should_be_timestamp = datetime.strptime('2021-11-02 09:26:00', '%Y-%m-%d %H:%M:%S')
        self.assertEqual(should_be_timestamp, timestamp)

    def test_get_event_timestamp_returns_none_when_no_event_id_found(self):
        vehicle = Vehicle(read_database=self.db, write_database=self.db)
        event_id = 1
        timestamp = vehicle.get_event_id_timestamp(event_id)
        self.assertIsNone(timestamp)

    def test_get_zip_files_in_json_etl_format_only_configs(self):
        self.db.populate_db('../_database_setup/sql_inserts_file_meta_data_cycle_3421.sql')
        vehicle = Vehicle(vehicle_id=1, read_database=self.db, write_database=self.db)
        zips = vehicle.get_zip_files_in_json_ETL_format(only_configs=True)
        self.assertEqual(2, len(zips), "Not gathering the right zip files when it should only get zip files with configs in them")

    def test_get_zip_files_in_json_etl_format_with_time_window(self):
        self.db.populate_db('../_database_setup/sql_inserts_file_meta_data_cycle_3421.sql')
        vehicle = Vehicle(vehicle_id=1, read_database=self.db, write_database=self.db)
        zips = vehicle.get_zip_files_in_json_ETL_format(time_window_begin='2021-02-10 04:11:44', time_window_end='2021-02-10 23:56:38')
        # rudimentary way to check that the dates align correctly in these text files
        self.assertTrue('sensordata_2021_02_01' in zips[1], "There should be 2 zips from 2021-02-01 (the configurations) and no more from that day")
        self.assertTrue('sensordata_2021_02_10' in zips[2], "There should be 2 zips from 2021-02-01 (the configurations), and then it should jump to 2021-02-10 because of the time window")
        self.assertTrue('23:56:39' in zips[-1], "The zips are not ending at the end of the time window.")

    def test_get_zip_files_in_json_etl_format_with_event_id(self):
        self.db.populate_db('../_database_setup/sql_inserts_file_meta_data_cycle_3421.sql')
        vehicle = Vehicle(vehicle_id=1, read_database=self.db, write_database=self.db)
        zips = vehicle.get_zip_files_in_json_ETL_format(event_id=2084846)
        self.assertTrue('sensordata_2021_02_01' in zips[1], "There should be 2 zips from 2021-02-01 (the configurations) and no more from that day")
        self.assertTrue('sensordata_2021_02_15/C63DCE68B8ED866258040548411_17:36:48' in zips[-1], "The last zip file should be before 2021-02-15 17:38 because that's 2 days after the timestamp for the event_id")

    def test_finds_open_events(self):
        self.db.setupDb()
        self.db.populate_db('../_database_setup/sql_inserts_open_event.sql', disable_foreign_keys=True)
        vehicle = Vehicle(vehicle_id=1, read_database=self.db, write_database=self.db)
        events = vehicle.get_open_events(unique_id='1_22043F')
        self.assertIsNotNone(events, "Should be finding an open event for this sensor")
        self.assertEqual(1, events.shape[0], "Should only be returning a single dataframe row for this open event")
        self.assertEqual('UI', events.event_type[0], "Should be finding a UI")
        self.assertEqual(1, events.event_id[0], "Returning incorrect event_id")

    def test_gets_current_sensor_wheel_position(self):
        self.db.run_statement("INSERT INTO meta_data (position, type, side, axle, set_point, cycle_number, sensor_number, md_id, unique_id, fleet_name, vehicle_id, active, halo_id, sensor_attribute_id, tire_model, tire_make, tire_diameter, tire_width, tire_aspect_ratio, tire_load_rating, tire_id, created_at, deactivated_at) VALUES ('O', 'T', 'L', 2, 100, 3421, '9DEC42', null, '3421_9DEC42', null, 1, 0, null, null, null, null, null, null, null, null, null, '2022-02-16 13:28:17', null);")
        vehicle = Vehicle(vehicle_id=1, read_database=self.db, write_database=self.db)
        wheel_position = vehicle.get_sensor_wheel_position('3421_9DEC42')
        self.assertEqual('L3OT', wheel_position, "Not returning the correct wheel position. Should be returning the latest active meta_data row.")
