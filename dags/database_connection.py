import psycopg2
from create_user import main
import random
from datetime import datetime
class DatabaseConnection:
    def __init__(self, host , port , user , password , dbname ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.connection = None
        

    def connect(self):
        """Establish a database connection."""
        try:
            
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            )
            print("Database connection established.")
        except Exception as e:
            print(f"Failed to connect to database: {e}")
            
            
            

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print("Database connection closed.")
            
            
            
            
            
            
    def insert_data(self,data, table ):
        """Insert data into the specified table."""
        # if not self.connection:
        #     print("No database connection.")
        #     return
       

        placeholders = ', '.join(['%s'] * len(data))
        print(f"placeholders: {placeholders}")
        columns = ', '.join(data.keys())
        print(f"columns: {columns}")
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        print(f"sql: {sql}")
        

        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, list(data.values()))
            self.connection.commit()
            cursor.close()
            print(f"Data inserted into {table}.")
        except Exception as e:
            print(f"Failed to insert data: {e}")
            
            
            
## fetch customers and drivers from database link it with orders
    def get_ids(self,table = "driver",table2 = "customer",table3 = "restaurant" ):
        """Insert data into the specified table."""
        if not self.connection:
            print("No database connection.")
            return
       

        try:
            cursor = self.connection.cursor()
            
            sql_driver = f"SELECT driver_id FROM {table} ORDER BY RANDOM() LIMIT 1;"
            cursor.execute(sql_driver)
            driver_id = cursor.fetchone()[0]

            
            
            sql_customer = f"SELECT customer_id FROM {table2} ORDER BY RANDOM() LIMIT 1;"
            cursor.execute(sql_customer)
            customer_id = cursor.fetchone()[0]
            
            sql_restaurant = f"SELECT restaurant_id FROM {table3} ORDER BY RANDOM() LIMIT 1;"
            cursor.execute(sql_restaurant)
            restaurant_id = cursor.fetchone()[0]
            
            cursor.close()
            
            
            print(driver_id, customer_id, restaurant_id)
            return driver_id, customer_id, restaurant_id
        except Exception as e:
            print(f"Failed to fetch data: {e}")
            
            
    def generate_random_order(self,table = "orders" ):
        driver_id, customer_id, restaurant_id = self.get_ids()
        price = self.random_price()
        print(type(price))
        """Insert data into the specified table."""
        
        # if not self.connection:
        #     print("No database connection.")
        #     return
        sql = f"INSERT INTO {table} (driver_id, customer_id, restaurant_id,total_amount) VALUES (%s, %s, %s, %s)"
        
        
        select_amount= "SELECT wallet FROM customer WHERE customer_id = %s FOR UPDATE"
        


        try:
            
            
            cursor = self.connection.cursor()
            
            cursor.execute(select_amount, (customer_id,))
            wallet_amount = cursor.fetchone()[0]
            print(type(wallet_amount))
            
            if wallet_amount < price:
                self.connection.rollback()
                print(f"Insufficient funds for customer {customer_id}. Transaction rolled back.")
                cursor.close()
                return
                
            
            cursor.execute(sql, (driver_id, customer_id, restaurant_id, price))
            cursor.execute(f"UPDATE driver SET status = %s WHERE driver_id = %s", ('unavailable', driver_id))
            cursor.execute(f"UPDATE customer SET wallet = wallet - %s WHERE customer_id = %s", (price, customer_id))
            cursor.execute(f"UPDATE driver SET wallet = wallet + %s WHERE driver_id = %s", (price, driver_id))
            
            self.connection.commit()
            print("Transaction committed successfully.")
            cursor.close()
            
        except Exception as e:
            self.connection.rollback()
            print(f"Failed to insert data: {e}")
        
        
    def random_price(self):
        return round(random.uniform(5.0, 140.0), 2)
    
    
    
        
if __name__ == "__main__":
    obj = DatabaseConnection(host="localhost", port="5432", user="jahez", password="123456", dbname="jahezdb")
    obj.connect()
    while True:
        user, driver = main()
        obj.insert_data(user, table="customer")
        obj.insert_data(driver, table="driver")
        obj.generate_random_order()
        
        
    
        
        
        