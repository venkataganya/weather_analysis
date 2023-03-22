import os
import unittest
from unittest.mock import MagicMock, patch
from extract_data.extract import get_data, get_data_save, parse_html

class TestGetData(unittest.TestCase):
    
    @patch('my_module.requests.request')
    def test_get_data(self, mock_request):
        # Mock the request object and set its return value to a MagicMock object with a text attribute
        mock_request.return_value = MagicMock(text='Response text')
        
        # Call the get_data function with the URL
        url = 'https://github.com/corteva/code-challenge-template/tree/main/wx_data'
        response_text = get_data(url)
        
        # Check that the mocked request was called with the expected parameters
        mock_request.assert_called_once_with('GET', url, headers={}, data={}, timeout=10)
        
        # Check that the returned text matches the mocked response text
        self.assertEqual(response_text, 'Response text')


class TestGetDataSave(unittest.TestCase):
    
    @patch('my_module.requests.get')
    def test_get_data_save(self, mock_get):
        # Mock the response object and set its return value to a sample CSV content
        mock_response = MagicMock(content=b'date\tmax_temp_C\tmin_temp_C\tprecipitation_MM\n2020-01-01\t10\t1\t0\n')
        mock_get.return_value = mock_response
        
        # Call the get_data_save function with a sample file name
        file_names = ['USC00200001.csv']
        get_data_save(file_names)
        
        # Check that the mocked get request was called with the expected URL
        mock_get.assert_called_once_with('https://raw.githubusercontent.com/corteva/code-challenge-template/main/wx_data/USC00200001.csv', timeout=10)
        
        # Check that the parsed CSV was saved to the expected file location
        save_directory = 'raw_files/USC00200001.csv'
        self.assertTrue(os.path.isfile(save_directory))
        
        # Remove the test file after the test
        os.remove(save_directory)
        
    def test_parse_html(self):
        # Create a sample HTML string
        html_string = '<html><body><a href="/wx_data/USC00200001.csv">USC00200001.csv</a></body></html>'
        
        # Call the parse_html function with the sample HTML string
        file_names = parse_html(html_string)
        
        # Check that the file name was parsed correctly
        self.assertEqual(file_names, ['USC00200001.csv'])

if __name__ == '__main__':
    unittest.main()

