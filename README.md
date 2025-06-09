# Python JDBC Коннектор для Баз Данных (Singleton) с использованием jaydebeapi

Надежный, потокобезопасный Singleton коннектор для баз данных на Python, поддерживающий несколько СУБД через JDBC с использованием [jaydebeapi](https://pypi.org/project/jaydebeapi/).  
Поддерживает PostgreSQL, Greenplum, IBM DB2, Netezza, MySQL и может быть расширен для других баз данных через настраиваемые JDBC драйверы.

---

## Особенности

- Потокобезопасный паттерн Singleton для повторного использования одного экземпляра подключения к базе данных на процесс  
- Поддержка нескольких баз данных через JDBC и настраиваемые драйверы  
- Выполнение SQL-запросов с возвратом сырых результатов или DataFrame pandas для удобного анализа данных  
- Надежная обработка соединений с автоматическим восстановлением и подробным логированием  
- Конфигурация через файл: все параметры подключения хранятся в `db_config.json`  
- Логирование выводится в консоль и файл для отслеживания  
- Включает модульные тесты с использованием моков для надежного тестирования без реальных подключений к БД

---

## Установка

Требуется Python 3.7+  
Установите зависимости с помощью pip:

```bash
pip install jaydebeapi pandas
```

---

## Структура Проекта
```
.
├── connector.py          # Класс DatabaseConnector, реализующий Singleton с логированием и методами запросов
├── logger_config.py      # Настройка логгера с обработчиками для консоли и файла
├── run_connector.py      # Пример скрипта, демонстрирующего использование подключения
├── test_connector.py     # Модульные тесты с моками
├── db_config.json        # JSON конфигурация для параметров подключения к базе данных (JDBC драйверы, URL, учетные данные)
├── README.md             # Эта документация
└── .gitignore            # Файлы/папки, которые нужно игнорировать в Git
```
---

## Использование
### Конфигурация
Отредактируйте db_config.json, чтобы добавить данные для подключения к вашей базе данных. Пример:
```json
{
  "databases": {
    "postgresql": {
      "db_type": "PostgreSQL",
      "driver_class": "org.postgresql.Driver",
      "driver_path": "/path/to/postgresql-42.6.0.jar",
      "url": "jdbc:postgresql://localhost:5432/mydatabase",
      "user": "myuser",
      "password": "mypassword"
    }
  }
}
```
### Запуск Примера

```bash
python run_connector.py
```
Этот скрипт подключается к настроенной базе данных и выполняет тестовый запрос, отображая версию БД.

### Использование DatabaseConnector в Вашем Коде
```python
from connector import DatabaseConnector

connector = DatabaseConnector('db_config.json', 'postgresql')
df = connector.execute_query_df('SELECT * FROM your_table LIMIT 10;')
print(df)
connector.close_connection()
```

---

## Тестирование
Запустите модульные тесты (с использованием моков, без реального подключения к БД) с помощью:
```bash
python -m unittest test_connector.py
```

## Логирование
Логи выводятся как в консоль, так и в файл app.log по умолчанию.
Записи логов включают временную метку, уровень, имя логгера и сообщение.

---
