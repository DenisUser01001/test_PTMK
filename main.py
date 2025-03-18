# pip install "psycopg[binary]"

import sys
from datetime import datetime
import time

import random
import psycopg


class MyApp:
    """В задании используется локальный сервер postgres"""
    def __init__(self):
        self.conn = psycopg.connect(dbname="postgres",
                                    user="postgres",
                                    password="Qwerty11",
                                    host="localhost",
                                    port="5432")
        self.cursor = self.conn.cursor()

    def employees_table_creator(self):
        """Метод создания таблицы с параметрами сотрудников.
        Если таблица не существует, то в схеме public создаёт таблицу employees."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.employees
                (
                employeeid serial primary key,
                employeename varchar(70) not null,
                employeedateofbirth date not null,
                employeesex varchar(10) not null default 'unknown'
                )
                            """)
        self.conn.commit()

    def employee_adder(self, employee_name, employee_dateofbirth, employee_sex):
        """Метод добавляет в БД строку с заданными параметрами сотрудника"""
        employee = Employee(employee_name, datetime.strptime(employee_dateofbirth, "%Y-%m-%d"), employee_sex)
        employee.employee_to_db(self.cursor)
        self.conn.commit()

    def employees_from_db(self, alternative=None, log_filename="log.txt"):
        """Метод работает в 2 режимах:
        1 режим: Выводит всех сотрудников с уникальными сочетаниями ключей ИМЯ и ДАТА РОЖДЕНИЯ;
        2 режим: Выводит всех записи о сотрудниках, чьё имя начинается на F и пол указан, как MALE.
        Во втором режиме работы выводится информация о времени обработки запроса в БД, а также создаётся отчёт
        о времени выполнения, который записывается в файл log.txt.
        """
        start_time = time.time()
        if not alternative:
            select_stmt = ("""
                                SELECT DISTINCT ON (employeename, employeedateofbirth) employeename, employeedateofbirth, employeesex
                                FROM public.employees
                                ORDER BY employeename, employeedateofbirth
                            """)
        else:
            select_stmt = ("""
                               SELECT employeename, employeedateofbirth, employeesex
                               FROM public.employees
                               WHERE employeesex = 'male' AND employeename LIKE 'F%'
                               ORDER BY employeename, employeedateofbirth
                          """)
        self.cursor.execute(select_stmt)
        data = self.cursor.fetchall()
        elapsed_time = time.time() - start_time
        lines_read = 0
        for emp in data:
            lines_read += 1
            age = Employee(emp[0], emp[1], emp[2]).employee_age_culculator()
            print(f"Сотрудник: {emp[0]}, дата рождения: {emp[1]}, пол: {emp[2]}, возраст: {age}")
        if alternative:
            # Создание индекса по совместным полям запроса: ИМЯ, ПОЛ.
            self.cursor.execute("CREATE INDEX idx_employeename_employeesex ON public.employees(employeename, employeesex)")
            self.conn.commit()
            print(f"Из базы данных получено {lines_read} строк, время получения данных: {elapsed_time},"
                  f"отчёт о выполнении сохранён в файл {log_filename}.")
            with open(log_filename, 'a', encoding='utf-8') as file:
                file.write(f'Current time:{datetime.now()}, lines read from DB:{lines_read},'
                           f' elapsed time:{elapsed_time}\n')

    def employees_generator_to_db(self, n=None, f=None):
        """Метод записывает в базу данных N случайных пользователй со случайными именами, дата рождения и полом,
        а также записывает F случайных пользователей со случайными именами, начинающимися на букву F и полом MALE"""
        employees = []
        for i in range(n):
            emp_name, emp_birthdate, emp_sex = self.get_random_name_and_birthdate()
            employees.append(Employee(emp_name, emp_birthdate, emp_sex))
        for i in range(f):
            emp_name, emp_birthdate, emp_sex = self.get_random_name_and_birthdate(input_sex="male", input_letter="F")
            employees.append(Employee(emp_name, emp_birthdate, emp_sex))
        Employee.employees_to_db(self.cursor, employees)
        self.conn.commit()

    @staticmethod
    def get_random_name_and_birthdate(input_letter=None, input_sex=None):
        """Метод образует связки из случайных имен, пола и возраста; имена берутся случайно из словаря, где по полу
        и букву указаны несколько вариантов имён на каждую букву."""
        names_dict = {
            'male': {
                'A': ['Adam', 'Aaron', 'Alan', 'Arthur', 'Andrew'],
                'B': ['Brian', 'Bradley', 'Brandon', 'Benjamin', 'Bruce'],
                'C': ['Charles', 'Christopher', 'Caleb', 'Cameron', 'Colin'],
                'D': ['David', 'Daniel', 'Dylan', 'Derek', 'Donald'],
                'E': ['Edward', 'Eric', 'Ethan', 'Evan', 'Eli'],
                'F': ['Frank', 'Frederick', 'Felix', 'Finn', 'Floyd'],
                'G': ['George', 'Gabriel', 'Gavin', 'Gregory', 'Grant'],
                'H': ['Henry', 'Harold', 'Hector', 'Hugo', 'Howard'],
                'I': ['Isaac', 'Ian', 'Ivan', 'Igor', 'Ilya'],
                'J': ['John', 'James', 'Jacob', 'Joseph', 'Jason'],
                'K': ['Kevin', 'Kyle', 'Keith', 'Kenneth', 'Kurt'],
                'L': ['Liam', 'Lucas', 'Logan', 'Louis', 'Lawrence'],
                'M': ['Michael', 'Matthew', 'Mark', 'Martin', 'Mason'],
                'N': ['Noah', 'Nathan', 'Nicholas', 'Nolan', 'Neil'],
                'O': ['Oliver', 'Owen', 'Oscar', 'Omar', 'Orlando'],
                'P': ['Peter', 'Patrick', 'Paul', 'Philip', 'Preston'],
                'Q': ['Quentin', 'Quincy', 'Quinn', 'Quinton', 'Quinlan'],
                'R': ['Robert', 'Richard', 'Ryan', 'Raymond', 'Roger'],
                'S': ['Samuel', 'Steven', 'Sean', 'Scott', 'Spencer'],
                'T': ['Thomas', 'Timothy', 'Tyler', 'Travis', 'Trevor'],
                'U': ['Ulysses', 'Uriel', 'Uriah', 'Ulrich', 'Urban'],
                'V': ['Victor', 'Vincent', 'Vance', 'Vernon', 'Vaughn'],
                'W': ['William', 'Walter', 'Wayne', 'Wesley', 'Warren'],
                'X': ['Xavier', 'Xander', 'Xerxes', 'Xenos', 'Xavi'],
                'Y': ['Yusuf', 'Yosef', 'Yehuda', 'Yuri', 'Yves'],
                'Z': ['Zachary', 'Zane', 'Zion', 'Zander', 'Zack']
            },
            'female': {
                'A': ['Anna', 'Alice', 'Amelia', 'Ava', 'Aria'],
                'B': ['Bella', 'Brooke', 'Brianna', 'Bethany', 'Bianca'],
                'C': ['Chloe', 'Charlotte', 'Clara', 'Catherine', 'Cynthia'],
                'D': ['Diana', 'Danielle', 'Daisy', 'Delilah', 'Dakota'],
                'E': ['Emma', 'Ella', 'Emily', 'Evelyn', 'Eleanor'],
                'F': ['Faith', 'Fiona', 'Felicity', 'Florence', 'Frances'],
                'G': ['Grace', 'Georgia', 'Gabriella', 'Gianna', 'Giselle'],
                'H': ['Hannah', 'Hailey', 'Harper', 'Hazel', 'Holly'],
                'I': ['Isabella', 'Ivy', 'Iris', 'Isla', 'Iliana'],
                'J': ['Jessica', 'Julia', 'Jasmine', 'Jade', 'Josephine'],
                'K': ['Katherine', 'Kayla', 'Kylie', 'Kennedy', 'Kiara'],
                'L': ['Lily', 'Layla', 'Leah', 'Luna', 'Lucy'],
                'M': ['Mia', 'Madison', 'Maya', 'Mila', 'Molly'],
                'N': ['Nora', 'Natalie', 'Naomi', 'Nina', 'Nadia'],
                'O': ['Olivia', 'Olena', 'Ophelia', 'Olive', 'Octavia'],
                'P': ['Penelope', 'Peyton', 'Paige', 'Phoebe', 'Piper'],
                'Q': ['Quinn', 'Queenie', 'Quincy', 'Quiana', 'Quintessa'],
                'R': ['Ruby', 'Riley', 'Rebecca', 'Rachel', 'Rose'],
                'S': ['Sophia', 'Scarlett', 'Stella', 'Samantha', 'Savannah'],
                'T': ['Taylor', 'Tiffany', 'Tessa', 'Trinity', 'Talia'],
                'U': ['Uma', 'Ursula', 'Ulani', 'Unity', 'Ulyssa'],
                'V': ['Victoria', 'Valentina', 'Violet', 'Vivian', 'Vanessa'],
                'W': ['Willow', 'Wendy', 'Winter', 'Wren', 'Whitney'],
                'X': ['Xena', 'Ximena', 'Xsandra', 'Xenia', 'Xia'],
                'Y': ['Yara', 'Yasmin', 'Yvonne', 'Yolanda', 'Yvette'],
                'Z': ['Zoe', 'Zara', 'Zelda', 'Zuhra', 'Zinnia']
            }
        }
        if not input_sex:
            random_sex = random.choice(['male', 'female'])
        else:
            random_sex = input_sex
        if not input_letter:
            random_first_letter = random.choice(list(names_dict[random_sex].keys()))
        else:
            random_first_letter = input_letter

        randon_name = random.choice(list(names_dict[random_sex][random_first_letter]))
        random_year = random.randint(1950, 2006)
        random_month = random.randint(1, 12)
        if random_month == 2:
            if (random_year % 4 == 0 and random_year % 100 != 0) or (random_year % 400 == 0):
                random_day = random.randint(1, 29)
            else:
                random_day = random.randint(1, 28)
        elif random_month in [4, 6, 9, 11]:
            random_day = random.randint(1, 30)
        else:
            random_day = random.randint(1, 31)
        random_birthdate = f"{random_year}-{random_month:02d}-{random_day:02d}"

        return randon_name, datetime.strptime(random_birthdate, "%Y-%m-%d"), random_sex

    def close(self):
        self.cursor.close()
        self.conn.close()


class Employee:
    def __init__(self, employee_name, employee_dateofbirth, employee_sex):
        self.employee_name = employee_name
        self.employee_dateofbirth = employee_dateofbirth
        self.employee_sex = employee_sex

    def employee_to_db(self, cursor):
        """Метод делает запись строки в БД с параметрами одного сотрудника"""
        insert_stmt = (
            "INSERT INTO public.employees (employeename, employeedateofbirth, employeesex) "
            "VALUES (%s, %s, %s)"
        )
        data = (self.employee_name, self.employee_dateofbirth, self.employee_sex)
        cursor.execute(insert_stmt, data)

    def employee_age_culculator(self):
        """Метод расчёта возраста сотрудника исходя из текущей даты и даты рождения сотрудника."""
        today = datetime.today()
        age = today.year - self.employee_dateofbirth.year - ((today.month, today.day) < (self.employee_dateofbirth.month, self.employee_dateofbirth.day))
        return age

    @staticmethod
    def employees_to_db(cursor, employees: list):
        """Метод записывает в базу данных принимаемый список с данными сотрудников."""
        insert_stmt = (
            "INSERT INTO public.employees (employeename, employeedateofbirth, employeesex) "
            "VALUES (%s, %s, %s)"
        )
        data = [(i.employee_name, i.employee_dateofbirth, i.employee_sex) for i in employees]
        cursor.executemany(insert_stmt, data)


if __name__ == '__main__':
    app = MyApp()
    param = sys.argv[1]

    if param == "1":
        app.employees_table_creator()
    elif param == "2":
        if len(sys.argv) != 5:
            raise AttributeError("Wrong parameters on application startup,"
                                 " need 4 additional parameters(startup param, name, date, sex)")
        script_name, param, name, date, sex = sys.argv
        app.employee_adder(name, date, sex)
    elif param == "3":
        app.employees_from_db()
    elif param == "4":
        app.employees_generator_to_db(n=1000000, f=100)
    elif param == "5":
        app.employees_from_db(alternative=1)
# Предложение по оптимизации запросов к таблице БД:
# В связи с тем, что в БД используется только одна таблица
# (в принципе не возможна проблема N+1) и запрос делается по 2 хэшируемым полям этой таблицы, то можно создать индекс по
# двум полям - имени и наименованию пола, что позволит ускорить обращение к БД
# с одновременным запросом по указанным полям.
# Привожу выдержку из файла log.txt по факту создания индекса:
# Current time:2025-03-17 01:39:46.880431, lines read from DB:19529, elapsed time:0.20937371253967285
# Current time:2025-03-17 02:08:51.239587, lines read from DB:19529, elapsed time:0.12497425079345703

    else:
        sys.exit()

    app.close()

