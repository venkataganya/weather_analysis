import unittest
from unittest.mock import patch, MagicMock
from app import app


class TestFlaskApi(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()

    def test_api_get_weather_form_data(self):
        with patch('db_write.database_read_write.DbWrite.get_transformed_data') as mock_db:
            mock_db.return_value = {'key': 'value'}
            response = self.app.get('/api/weather/1/100')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json, {'key': 'value'})

    def test_api_get_weather_agg_data(self):
        with patch('db_write.database_read_write.DbWrite.get_aggregate_data') as mock_db:
            mock_db.return_value = {'key': 'value'}
            response = self.app.get('/api/weather/stats/1/100')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json, {'key': 'value'})

    def test_api_get_transf_count(self):
        with patch('db_write.database_read_write.DbWrite.get_count_transf') as mock_db:
            mock_db.return_value = {'key': 'value'}
            response = self.app.get('/api/weather/transf/count')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json, {'key': 'value'})

    def test_api_get_aggr_count(self):
        with patch('db_write.database_read_write.DbWrite.get_count_agg') as mock_db:
            mock_db.return_value = {'key': 'value'}
            response = self.app.get('/api/weather/agg/count')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json, {'key': 'value'})

    def test_api_get_weather_form_data_yr_reg(self):
        with patch('db_write.database_read_write.DbWrite.get_transformed_data_by_region_date') as mock_db:
            mock_db.return_value = {'key': 'value'}
            response = self.app.get('/api/weather/filter/USC00336196/1985-04-10')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json, {'key': 'value'})

    def test_api_get_weather_agg_data_yr_reg(self):
        with patch('db_write.database_read_write.DbWrite.get_aggregate_data_by_region_year') as mock_db:
            mock_db.return_value = {'key': 'value'}
            response = self.app.get('/api/weather/stats/filter/1995/USC00132977')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json, {'key': 'value'})
