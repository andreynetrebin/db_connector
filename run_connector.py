from connector import DatabaseConnector
from connector import get_logger

logger = get_logger("RunConnector")

def main():
    # Путь к файлу конфигурации и ключ базы
    config_file = 'db_config.json'
    db_key = 'db2'

    try:
        connector = DatabaseConnector(config_file, db_key)
        # df = connector.execute_query_df("SELECT version();")
        df = connector.execute_query("""
            SELECT
                doc.id
                ,file.body_xml
            FROM    
                RSMASTER.DOCUMENT AS DOC
                LEFT JOIN RSMASTER.DOCUMENT_CONTENT AS DOC_CONT ON DOC.ID = DOC_CONT.R_DOC
                LEFT JOIN RSMASTER.FILES AS FILE ON DOC_CONT.R_FILE = FILE.ID
            WHERE 
                DOC.ID IN (
            7217584739
                );    
            """)
        logger.info(f"Database version:\n{df}")
    except Exception as e:
        logger.error(f"Error during database operations: {e}")
    finally:
        connector.close_connection()

if __name__ == "__main__":
    main()
