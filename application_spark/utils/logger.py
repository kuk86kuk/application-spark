from datetime import datetime

class SparkLogger:
    def __init__(self, env: str, step_name: str):
        self.env = env
        self.step_name = step_name
        self.start_time = None
        self.BORDER = "=" * 60
        self.SUB_BORDER = "-" * 50
        self.HEADER = """
  _____  _    _ ______ _____  
 |  __ \| |  | |  ____|  __ \ 
 | |  | | |  | | |__  | |__) |
 | |  | | |  | |  __| |  ___/ 
 | |__| | |__| | |____| |     
 |_____/ \____/|______|_|     
"""

    def print_header(self):
        """Красивое отображение заголовка этапа"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(self.BORDER)
        print(self.HEADER)
        print(f"\n>>> {self.step_name.upper()} <<<")
        print(f"Environment: {self.env}")
        print(f"Timestamp: {timestamp}")
        print(self.BORDER)
        self.start_time = datetime.now()

    def print_step_info(self, action: str):
        """Информация о выполняемом шаге"""
        print(f"\n{self.SUB_BORDER}")
        print(f"STAGE: {self.step_name}")
        print(f"ACTION: {action}")
        print(f"{self.SUB_BORDER}\n")

    def print_stats(self):
        """Вывод статистики выполнения"""
        if self.start_time:
            duration = datetime.now() - self.start_time
            print(f"\n{self.SUB_BORDER}")
            print(f"EXECUTION TIME: {duration.total_seconds():.2f} sec")
            print(f"STATUS: COMPLETED")
            print(self.SUB_BORDER)