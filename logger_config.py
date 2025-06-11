import logging
import os

def get_logger(name: str, log_file: str = 'app.log') -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)

        # Обработчик для вывода в консоль
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # Обработчик для записи в файл
        if not os.path.exists(log_file):
            with open(log_file, 'w'):
                pass  # Создаем файл, если он не существует

        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        logger.propagate = False
    return logger
