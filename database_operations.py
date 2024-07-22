from dotenv import load_dotenv
import mysql.connector, os, sys
load_dotenv()


MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')


class Database_ops:
    def __init__(self) -> None:
         
        self.conn =  mysql.connector.connect(
            host=MYSQL_HOST,
            database=MYSQL_DATABASE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        self.cursor = self.conn.cursor()
        
    def create_table(self, query):

        try:
             self.cursor.execute(query)
             print(f"table is created successfully")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        # finally:
        #     self.cursor.close()
        #     self.conn.close()
            
    def insert_data(self, query):
        try:
            self.cursor.execute(query)
            self.conn.commit()
            
            print(f"record inserted")
        except mysql.connector.Error as err:
            print(f"Error : {err}")
        # finally:
        #     self.cursor.close()
        #     self.conn.close()

    def close_connection(self):
        self.conn.close()
        self.cursor.close()

