import datetime

from unittest import TestCase
from Vehicle import Vehicle
from Database import Localhost
import pandas as pd


class TestVehicleEvents(TestCase):
    def setUp(self):
        self.db = Localhost('vehicle_test')
        self.db.setupDb('../_database_setup/vehicle_db.sql')
        self.vehicle = Vehicle(1, self.db, self.db)

    def tearDown(self):
        self.db.cleanUpDB()

    def test_returns_none_when_no_events_exist(self):
        self.assertIsNone(self.vehicle.get_open_ui_events())

    def test_vehicle_can_get_active_unique_ids(self):

        unique_ids = self.vehicle.get_active_sensors()
        self.assertIsInstance(unique_ids,list)
        self.assertEqual(10,len(unique_ids))

        # test if it can go to the field on the second call
        unique_ids = self.vehicle.get_active_sensors()
        self.assertIsInstance(unique_ids, list)
        self.assertEqual(10, len(unique_ids))

    def test_it_can_get_all_the_open_events_for_a_vehicle(self):
        open_vehicle_events = self.vehicle.get_open_vehicle_events()
        self.assertIsNotNone(open_vehicle_events)
        self.assertTrue(open_vehicle_events[open_vehicle_events.status == "CLOSED"].empty)
        self.assertEqual(2202672,open_vehicle_events.event_id.item())
    def test_returns_open_ui_event(self):
        self.db.run_statement(
            "INSERT INTO event_table (event_id, unique_id, event_type, pressure_date, ts_created)" \
            "VALUES(2200474,'3421_1F0B31', 'UI', '2021-06-20 20:45:14', '2021-06-20 20:45:14');")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811773, 2200474, '2021-04-07 16:03:55', NULL, 'OPEN',NULL, 3);")
        ui_events = self.vehicle.get_open_ui_events()
        self.assertIsNotNone(ui_events)
        self.assertEqual(1, len(ui_events.unique_id.to_list()))
        self.assertIn('3421_1F0B31', ui_events.unique_id.to_list())

    def test_does_not_return_closed_ui_event(self):
        self.db.run_statement(
            "INSERT INTO event_table (event_id, unique_id, event_type, pressure_date, ts_created)" \
            "VALUES(2200474,'3421_1F0B31', 'UI', '2021-06-20 20:45:14', '2021-06-20 20:45:14');")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811773, 2200474, '2021-04-07 16:03:55', NULL, 'OPEN',NULL, 3);")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811774, 2200474, '2021-04-07 16:03:55', NULL, 'CLOSED',NULL, 3);")
        ui_events = self.vehicle.get_open_ui_events()
        self.assertIsNone(ui_events)

    def test_returns_open_leak_events(self):
        self.db.run_statement(
            "INSERT INTO event_table (event_id, unique_id, event_type, pressure_date, ts_created)" \
            "VALUES(2200474,'3421_1F0B31', 'LEAK', '2021-06-20 20:45:14', '2021-06-20 20:45:14');")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811773, 2200474, '2021-04-07 16:03:55', NULL, 'OPEN',NULL, 3);")
        leak_events = self.vehicle.get_open_leak_events()
        self.assertIsNotNone(leak_events)
        self.assertEqual(2, len(leak_events.unique_id.to_list()))
        self.assertIn('3421_1F0B31', leak_events.unique_id.to_list())

    def test_does_not_return_closed_leak_event(self):
        self.db.run_statement(
            "INSERT INTO event_table (event_id, unique_id, event_type, pressure_date, ts_created)" \
            "VALUES(2200474,'3421_1F0B31', 'LEAK', '2021-06-20 20:45:14', '2021-06-20 20:45:14');")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811773, 2200474, '2021-04-07 16:03:55', NULL, 'OPEN',NULL, 3);")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811774, 2200474, '2021-04-07 16:03:55', NULL, 'CLOSED',NULL, 3);")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811775, 2202672, '2021-04-07 16:03:55', NULL, 'CLOSED',NULL, 3);")
        leak_events = self.vehicle.get_open_leak_events()
        self.assertIsNone(leak_events)

    def test_returns_open_ui_leak_events(self):
        self.db.run_statement(
            "INSERT INTO event_table (event_id, unique_id, event_type, pressure_date, ts_created)" \
            "VALUES(2200474,'3421_1F0B31', 'UI_LEAK', '2021-06-20 20:45:14', '2021-06-20 20:45:14');")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811773, 2200474, '2021-04-07 16:03:55', NULL, 'OPEN',NULL, 3);")
        ui_leak_events = self.vehicle.get_open_ui_leak_events()
        self.assertIsNotNone(ui_leak_events)
        self.assertEqual(1, len(ui_leak_events.unique_id.to_list()))
        self.assertIn('3421_1F0B31', ui_leak_events.unique_id.to_list())
        self.assertEqual("UI_LEAK", ui_leak_events.event_type.item())

    def test_does_not_return_closed_ui_leak_event(self):
        self.db.run_statement(
            "INSERT INTO event_table (event_id, unique_id, event_type, pressure_date, ts_created)" \
            "VALUES(2200474,'3421_1F0B31', 'UI_LEAK', '2021-06-20 20:45:14', '2021-06-20 20:45:14');")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811773, 2200474, '2021-04-07 16:03:55', NULL, 'OPEN',NULL, 3);")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811774, 2200474, '2021-04-07 16:03:55', NULL, 'CLOSED',NULL, 3);")
        leak_events = self.vehicle.get_open_ui_leak_events()
        self.assertIsNone(leak_events)

    def test_returns_open_leak_and_ui_leak_events(self):
        self.db.run_statement(
            "INSERT INTO event_table (event_id, unique_id, event_type, pressure_date, ts_created)" \
            "VALUES(2200474,'3421_1F0B31', 'UI_LEAK', '2021-06-20 20:45:14', '2021-06-20 20:45:14');")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811773, 2200474, '2021-04-07 16:03:55', NULL, 'OPEN',NULL, 3);")
        ui_leak_events = self.vehicle.get_open_leak_and_ui_leak_events()
        self.assertIsNotNone(ui_leak_events)
        self.assertEqual(2, len(ui_leak_events.unique_id.to_list()))
        self.assertIn('3421_1F0B31', ui_leak_events.unique_id.to_list())
        self.assertIn('3421_1F0B31', ui_leak_events.unique_id.to_list())
        self.assertEqual(["LEAK","UI_LEAK"], ui_leak_events.event_type.to_list())

    def test_does_not_return_closed_leak_and_ui_leak_event(self):
        self.db.run_statement(
            "INSERT INTO event_table (event_id, unique_id, event_type, pressure_date, ts_created)" \
            "VALUES(2200474,'3421_1F0B31', 'LEAK', '2021-06-20 20:45:14', '2021-06-20 20:45:14');")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811773, 2200474, '2021-04-07 16:03:55', NULL, 'OPEN',NULL, 3);")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811774, 2200474, '2021-04-07 16:03:55', NULL, 'CLOSED',NULL, 3);")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811775, 2202672, '2021-04-07 16:03:55', NULL, 'CLOSED',NULL, 3);")
        self.db.run_statement(
            "INSERT INTO event_table (event_id, unique_id, event_type, pressure_date, ts_created)" \
            "VALUES(2200475,'3421_1F0B31', 'UI_LEAK', '2021-06-20 20:45:14', '2021-06-20 20:45:14');")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811776, 2200475, '2021-04-07 16:03:55', NULL, 'OPEN',NULL, 3);")
        self.db.run_statement(
            "INSERT INTO event_status (event_status_id, event_id, ts_created, severity, status, event_input_variables, severity_order) VALUES (1811777, 2200475, '2021-04-07 16:03:55', NULL, 'CLOSED',NULL, 3);")
        leak_events = self.vehicle.get_open_leak_and_ui_leak_events()
        self.assertIsNone(leak_events)
