import os
import sqlite3
import random
import math
import sys
from datetime import datetime, timedelta

from logger import logging
from exception import CustomException

# Class for generating data for multiple tables with SQLite integration to build ML models
class DataGeneratorConfig:
    database: str = os.path.join('data', 'FinSight360.db')
    credit_table: str = "credit_table"
    fraud_table: str = "fraud_table"
    loan_table: str = "loan_table"
    anti_money_launder: str = "anti_money_launder"
    cashflow: str = "cashflow"


class DataGenerator:
    def __init__(self):
        self.conn = sqlite3.connect(DataGeneratorConfig().database)
        self.cur = self.conn.cursor()
        self.rows = 10000
        self.batch_size = 1000

    def create_table(self, table_name: str, primary_key: str, column_names: list, foreign_key: str = None, foreign_table: str = None):
        try:
            columns = [f"{primary_key} INTEGER PRIMARY KEY AUTOINCREMENT"]
            for column in column_names:
                if column == foreign_key and foreign_table:
                    columns.append(f"{column} INTEGER, FOREIGN KEY ({column}) REFERENCES {foreign_table}({column})")
                else:
                    columns.append(f"{column} TEXT")
            columns_str = ", ".join(columns)
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
            self.cur.execute(query)
            logging.info(f"Table {table_name} created successfully!")
        except Exception as e:
            logging.error(f"Error creating table {table_name}: {e}")
            raise CustomException(e, sys)

    def bulk_insert(self, query: str, data: list):
        try:
            self.cur.executemany(query, data)
        except sqlite3.Error as e:
            logging.error(f"SQLite error during bulk insert: {e}")
            raise CustomException(e, sys)

    # Credit score table data generation
    def credit_score(self):
        try:
            columns = ["Age", "Income", "CreditHistory", "LoanAmount", "LoanTenure", "PreviousDefaults", "CreditScore"]
            self.create_table(table_name=DataGeneratorConfig.credit_table, primary_key="CustomerID", column_names=columns)

            data = []
            customer_id = 1000
            for _ in range(self.rows):
                customer_id += 1
                age = random.randint(18, 60)
                income = random.randint(10000, 200000)
                credit_history = random.choice(['Good', 'Good', 'Fair', 'Fair', 'Poor'])
                loan_amount = random.randint(1, 100) * random.choice([10000, 100000, 1000000]) if income >= 15000 else random.randint(1, 100) * 10000
                loan_tenure = math.floor(loan_amount / math.floor(income / 4)) + 1
                previous_defaults = random.choice(['No', 'No', 'No', 'Yes', 'No'])
                credit_score = random.randint(300, 850)

                data.append((customer_id, age, income, credit_history, loan_amount, loan_tenure, previous_defaults, credit_score))

                if len(data) >= self.batch_size:
                    self.bulk_insert(f'''
                        INSERT INTO {DataGeneratorConfig.credit_table} 
                        (CustomerID, Age, Income, CreditHistory, LoanAmount, LoanTenure, PreviousDefaults, CreditScore)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', data)
                    data = []

            if data:
                self.bulk_insert(f'''
                    INSERT INTO {DataGeneratorConfig.credit_table} 
                    (CustomerID, Age, Income, CreditHistory, LoanAmount, LoanTenure, PreviousDefaults, CreditScore)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', data)

            self.conn.commit()
            logging.info("Credit score data generation completed successfully!")
        except Exception as e:
            logging.error(f"Error generating credit score data: {e}")
            raise CustomException(e, sys)

    # Fraud table data generation
    def fraud(self):
        try:
            columns = ["TransactionDate", "Amount", "MerchantName", "CardPresent", "ExpenseCategory", "Fraudulent"]
            self.create_table(table_name=DataGeneratorConfig.fraud_table, primary_key="TransactionID", column_names=columns)

            data = []
            for _ in range(self.rows):
                transaction_date = datetime.now() - timedelta(days=random.randint(0, 365))
                amount = round(random.uniform(1, 10000), 2)
                merchant = random.choice(["Amazon", "Walmart", "Target", "Best Buy", "Costco"])
                card_present = random.choice(["Yes", "No"])
                category = random.choice(["Groceries", "Electronics", "Clothing", "Travel", "Entertainment"])
                fraudulent = "Yes" if random.random() < 0.05 else "No"

                data.append((transaction_date.strftime("%Y-%m-%d"), amount, merchant, card_present, category, fraudulent))

                if len(data) >= self.batch_size:
                    self.bulk_insert(f'''
                        INSERT INTO {DataGeneratorConfig.fraud_table} 
                        (TransactionDate, Amount, MerchantName, CardPresent, ExpenseCategory, Fraudulent)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', data)
                    data = []

            if data:
                self.bulk_insert(f'''
                    INSERT INTO {DataGeneratorConfig.fraud_table} 
                    (TransactionDate, Amount, MerchantName, CardPresent, ExpenseCategory, Fraudulent)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', data)

            self.conn.commit()
            logging.info("Fraud data generation completed successfully!")
        except Exception as e:
            logging.error(f"Error generating fraud data: {e}")
            raise CustomException(e, sys)

    # Loan table data generation
    def loan(self):
        try:
            columns = ["ApplicantIncome", "CoapplicantIncome", "LoanAmount", "LoanTerm", "CreditHistory", "PropertyArea", "LoanStatus"]
            self.create_table(table_name=DataGeneratorConfig.loan_table, primary_key="LoanID", column_names=columns)

            data = []
            for _ in range(self.rows):
                applicant_income = random.randint(1000, 10000)
                coapplicant_income = random.randint(0, 8000)
                loan_amount = random.randint(100, 5000) * 100
                loan_term = random.choice([60, 180, 360])
                credit_history = random.choice([0, 1])
                property_area = random.choice(["Urban", "Semiurban", "Rural"])
                loan_status = "Approved" if random.random() < 0.7 else "Rejected"

                data.append((applicant_income, coapplicant_income, loan_amount, loan_term, credit_history, property_area, loan_status))

                if len(data) >= self.batch_size:
                    self.bulk_insert(f'''
                        INSERT INTO {DataGeneratorConfig.loan_table} 
                        (ApplicantIncome, CoapplicantIncome, LoanAmount, LoanTerm, CreditHistory, PropertyArea, LoanStatus)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', data)
                    data = []

            if data:
                self.bulk_insert(f'''
                    INSERT INTO {DataGeneratorConfig.loan_table} 
                    (ApplicantIncome, CoapplicantIncome, LoanAmount, LoanTerm, CreditHistory, PropertyArea, LoanStatus)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', data)

            self.conn.commit()
            logging.info("Loan data generation completed successfully!")
        except Exception as e:
            logging.error(f"Error generating loan data: {e}")
            raise CustomException(e, sys)

    # Anti-money laundering table data generation
    def anti_money_launder(self):
        try:
            columns = ["TransactionDate", "Amount", "Sender", "Receiver", "TransactionType", "RiskLevel"]
            self.create_table(table_name=DataGeneratorConfig.anti_money_launder, primary_key="TransactionID", column_names=columns)

            data = []
            for _ in range(self.rows):
                transaction_date = datetime.now() - timedelta(days=random.randint(0, 365))
                amount = round(random.uniform(1000, 500000), 2)
                sender = f"Sender_{random.randint(1, 1000)}"
                receiver = f"Receiver_{random.randint(1, 1000)}"
                transaction_type = random.choice(["Domestic", "International"])
                risk_level = random.choice(["Low", "Medium", "High"])

                data.append((transaction_date.strftime("%Y-%m-%d"), amount, sender, receiver, transaction_type, risk_level))

                if len(data) >= self.batch_size:
                    self.bulk_insert(f'''
                        INSERT INTO {DataGeneratorConfig.anti_money_launder} 
                        (TransactionDate, Amount, Sender, Receiver, TransactionType, RiskLevel)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', data)
                    data = []

            if data:
                self.bulk_insert(f'''
                    INSERT INTO {DataGeneratorConfig.anti_money_launder} 
                    (TransactionDate, Amount, Sender, Receiver, TransactionType, RiskLevel)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', data)

            self.conn.commit()
            logging.info("Anti-money laundering data generation completed successfully!")
        except Exception as e:
            logging.error(f"Error generating anti-money laundering data: {e}")
            raise CustomException(e, sys)

    # Cash flow table data generation
    def cashflow(self):
        try:
            columns = ["Date", "Inflow", "Outflow", "NetFlow", "Category", "Description"]
            self.create_table(table_name=DataGeneratorConfig.cashflow, primary_key="TransactionID", column_names=columns)

            data = []
            for _ in range(self.rows):
                date = datetime.now() - timedelta(days=random.randint(0, 365))
                inflow = random.uniform(0, 10000)
                outflow = random.uniform(0, inflow)
                netflow = inflow - outflow
                category = random.choice(["Salary", "Business", "Investment", "Other"])
                description = f"Transaction_{random.randint(1, 10000)}"

                data.append((date.strftime("%Y-%m-%d"), inflow, outflow, netflow, category, description))

                if len(data) >= self.batch_size:
                    self.bulk_insert(f'''
                        INSERT INTO {DataGeneratorConfig.cashflow} 
                        (Date, Inflow, Outflow, NetFlow, Category, Description)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', data)
                    data = []

            if data:
                self.bulk_insert(f'''
                    INSERT INTO {DataGeneratorConfig.cashflow} 
                    (Date, Inflow, Outflow, NetFlow, Category, Description)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', data)

            self.conn.commit()
            logging.info("Cash flow data generation completed successfully!")
        except Exception as e:
            logging.error(f"Error generating cash flow data: {e}")
            raise CustomException(e, sys)

if __name__ == "__main__":
    try:
        data_generator = DataGenerator()
        data_generator.credit_score()
        data_generator.fraud()
        data_generator.loan()
        data_generator.anti_money_launder()
        data_generator.cashflow()
    finally:
        data_generator.conn.close()