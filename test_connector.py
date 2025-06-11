import unittest
from unittest.mock import patch, MagicMock
from connector import DatabaseConnector

class TestDatabaseConnector(unittest.TestCase):

    @patch('connector.jaydebeapi.connect')
    @patch('builtins.open')
    @patch('json.load')
    def test_singleton_and_execute_query(self, mock_json_load, mock_open, mock_connect):
        # Подменяем конфиг
        mock_json_load.return_value = {
            "databases": {
                "testdb": {
                    "db_type": "TestDB",
                    "driver_class": "test.Driver",
                    "driver_path": "/path/to/test.jar",
                    "url": "jdbc:test://localhost:1234/db",
                    "user": "user",
                    "password": "pass"
                }
            }
        }

        # Мокаем jaydebeapi.connect
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Настроим курсор
        mock_cursor.description = [('col1',), ('col2',)]
        mock_cursor.fetchall.return_value = [(1, 'data1'), (2, 'data2')]

        # Инициализация дважды должна вернуть один и тот же экземпляр (Singleton)
        connector1 = DatabaseConnector('fake_path.json', 'testdb')
        connector2 = DatabaseConnector('fake_path.json', 'testdb')
        self.assertIs(connector1, connector2)

        # Тест execute_query
        result = connector1.execute_query("SELECT * FROM table")
        self.assertEqual(result, [(1, 'data1'), (2, 'data2')])
        mock_cursor.execute.assert_called_with("SELECT * FROM table")

        # Тест execute_query_df
        df = connector1.execute_query_df("SELECT * FROM table")
        self.assertEqual(list(df.columns), ['col1', 'col2'])
        self.assertEqual(df.iloc[0]['col1'], 1)
        self.assertEqual(df.iloc[1]['col2'], 'data2')

        # Проверка вызова close_connection
        connector1.close_connection()
        mock_conn.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
