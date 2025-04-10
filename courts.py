import os
import csv
import camelot
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import re
import smtplib
import traceback
import textwrap
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.keys import Keys
import requests
from datetime import date, datetime

'''
This file contains classes for different courts , there is a parent class called Super_Class which has following methods:
1.__init__ - intializes webscraper and set database params
2.get_connection - initializes connection with database return a connection object
3.get_user_detail - retrieves user data from database returns a list of user info
4.extract_csv - extracts tabular data in from pdf and returns a pandas dataframe
5.send_email - sends emails to users in the database
6.process_csv - this function reads a csv of predefined format and then processes it.

Every court can implement there own version of these methods but with same name and should have similar return types. They must also implement a methos fetch_list to scrape list from the particular court's website and also clean_csv to clean the csv extracted from the pdf , the cleaned pdf should be have these headers to use the parent class's process_csv function:
"Sr.No.", "Case Number", "Main Parties", "Petitioner Advocate", "Respondent Advocate"
if its not in this format then they should implement there own process_csv function like the case of Allahabad court,
'''


class Super_Class:
    def __init__(self):
        load_dotenv()
        options = Options()
        options.use_chromium = True
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--ignore-ssl-errors")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36") 
        self.options = options
        
        self.db_params = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT")
        }

        if not os.path.exists("temp_csv"):
            os.mkdir("temp_csv")
        if not os.path.exists("temp_pdf"):
            os.mkdir("temp_pdf")

    def get_connection(self):
        conn = psycopg2.connect(**self.db_params)
        return conn

    def extract_csv(self, pdf_file, page_range):
        try:
            tables = camelot.read_pdf(pdf_file, pages=page_range, flavor="stream")
            
            if tables.n > 0:
                merged_df = pd.concat([t.df for t in tables], ignore_index=True)
                return merged_df
            else:
                print(f"No tables found in pages: {page_range}")
                return None
        except Exception as e:
            print(f"Error in extracting pages {page_range}: {e}")
            return None

    def get_user_detail(self, case_no):

        conn = self.get_connection()
        curr = conn.cursor()
        try:
            curr.execute("""
                SELECT l.name, l.email 
                FROM lawyers l 
                JOIN lawyers_cases lc ON l.lawyer_id = lc.lawyer_id 
                JOIN cases c ON lc.case_id = c.case_id 
                WHERE c.case_number = %s
            """, (case_no,))
            result = curr.fetchall()
            return result if result else None
        finally:
            curr.close()
            conn.close()

    def send_email(self, detail, case_number, petitioner_advocates, respondent_advocates, main_parties):
        
        subject = "Case Notification"
        message = f"""Dear {detail[0]},

            {case_number} has been mentioned in today's Cause List.

            Main Parties:
            {textwrap.fill(main_parties, width=80)}

            Petitioner Advocates:
            {textwrap.fill(', '.join(petitioner_advocates) if isinstance(petitioner_advocates, list) else petitioner_advocates, width=80)}

            Respondent Advocates:
            {textwrap.fill(', '.join(respondent_advocates) if isinstance(respondent_advocates, list) else respondent_advocates, width=80)}
            """
        
        msg = MIMEMultipart()
        msg['From'] = os.getenv("EMAIL")
        msg['To'] = detail[1]
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))
        
        try:
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(os.getenv("EMAIL"), os.getenv("EMAIL_PASSWORD"))
                server.sendmail(os.getenv("EMAIL"), detail[1], msg.as_string())
                print(f"Email sent to {detail[1]}")
        except Exception as e:
            print(f"Failed to send email to {detail[1]}: {e}")

    def process_csv(self, csv_path):

        try:
            with open(csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    case_number = row['Case Number'] if row['Case Number'] else ""
                    main_parties = row['Main Parties'] if row['Main Parties'] else ""
                    petitioner_advocates = row['Petitioner Advocate'].split(';') if row['Petitioner Advocate'] else []
                    respondent_advocates = row['Respondent Advocate'].split(';') if row['Respondent Advocate'] else []
                    details = self.get_user_detail(case_number)
                    if details:
                        for detail in details:
                            self.send_email(detail, case_number, petitioner_advocates, respondent_advocates, main_parties)
        except Exception as e:
            print(f"Error : {e}")


class Gauhati_court(Super_Class):
    def fetch_list(self):
        driver = None
        conn = None
        curr = None
        try:
            driver = webdriver.Edge(options=self.options)
            conn = self.get_connection()
            curr = conn.cursor()
            curr.execute("SELECT last_list_date FROM courts WHERE court_id = 1")
            last_date = curr.fetchall()[0][0]
            if last_date == None:
                last_date = date.today()
            else:
                last_date = datetime.strptime(last_date.strip(),"%d/%m/%Y").date()

            URL = "https://ghconline.gov.in/index.php/consolidated-cause-list/"
            driver.get(URL)
            list_table = driver.find_element(By.XPATH, "/html/body/div[1]/div[3]/div/div/main/div/article/div/div/table/tbody")
            list_rows = list_table.find_elements(By.TAG_NAME, "tr")
            new_date = list_rows[1].find_element(By.TAG_NAME, "td").text
            pdf_names = []
            for row in list_rows[1:]:
                daily_list = row.find_element(By.TAG_NAME, "td")
                list_date = daily_list.text[:10]
                list_link = daily_list.find_element(By.TAG_NAME, "a").get_attribute("href")
                if(last_date < datetime.strptime(list_date.strip(),"%d/%m/%Y").date()):
                    response = requests.get(list_link)
                    pdf_file = rf"temp_pdf\{list_date.replace('/','_')}.pdf"
                    with open(pdf_file,'wb') as f:
                        f.write(response.content)
                    pdf_names.append(pdf_file)
                else:
                    break
                print(list_date + " " + list_link)

            if len(pdf_names) != 0:
                curr.execute("UPDATE courts SET last_list_date = %s WHERE court_id = 1",(new_date,))
                print(pdf_names)
                return pdf_names
            return None
        except Exception as e:
            print(f"Error : {e}")
            return None
        finally:
            if driver:
                driver.quit()
            if curr:
                conn.commit()
                curr.close()
                conn.close()
        

    def clean_csv(self, input_file):
        try:
            headers = ["Sr.No.", "Case Number", "Main Parties", "Petitioner Advocate", "Respondent Advocate"]
            with open(input_file, 'r', encoding='utf-8') as f:
                data = list(csv.reader(f))
            
            cleaned_data = [headers]
            
            current_case = None
            i = 0
            
            while i < len(data):
                row = data[i]
                
                if row[0] and row[0].strip().isdigit():
                    if current_case:
                        cleaned_data.append(current_case)
                    
                    current_case = [
                        row[0].strip(),
                        row[1].strip(),
                        "",
                        "",
                        "" 
                    ]
                    
                    if len(row) > 2 and row[2]:
                        current_case[2] = row[2].strip()
                    if len(row) > 3 and row[3]:
                        current_case[3] = row[3].strip()
                    if len(row) > 4 and row[4]:
                        current_case[4] = row[4].strip()
                
                elif row[0] == "" and current_case:
                    if len(row) > 2 and row[2] and "Versus" in row[2]:
                        pass
                    elif len(row) > 1 and row[1] and "WITH" in row[1]:
                        pass
                    elif len(row) > 1 and row[1] and "in " in row[1]:
                        if current_case[1]:
                            current_case[1] += "; " + row[1].strip()

                    elif len(row) > 2 and row[2]:
                        if current_case[2]:
                            if "THE " in row[2] or any(word in current_case[2].upper() for word in ["VERSUS", "VS", "V."]):
                                current_case[2] += " vs " + row[2].strip()
                            else:
                                current_case[2] += " " + row[2].strip()
                        else:
                            current_case[2] = row[2].strip()
                    
                    if len(row) > 3 and row[3]:
                        if current_case[3]:
                            current_case[3] += "; " + row[3].strip()
                        else:
                            current_case[3] = row[3].strip()
                    
                    if len(row) > 4 and row[4]:
                        if current_case[4]:
                            current_case[4] += "; " + row[4].strip()
                        else:
                            current_case[4] = row[4].strip()
                
                i += 1
            
            if current_case:
                cleaned_data.append(current_case)
            
            with open(input_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(cleaned_data)
            
            return True
        except Exception as e:
            print(e)
            return False

class Allahabad_court(Super_Class):
    def fetch_list(self):
        driver = None
        conn = None
        curr = None
        try:
            URL = "https://www.allahabadhighcourt.in/causelist/indexA.html"
            driver = webdriver.Edge(options=self.options)
            driver.get(URL)

            conn = self.get_connection()
            curr = conn.cursor()
            curr.execute("SELECT last_list_date FROM courts WHERE court_id = 2")
            last_date = curr.fetchall()[0][0]
            if last_date is None:
                last_date = date.today()
            else:
                last_date = datetime.strptime(last_date.strip(), "%d-%m-%Y").date()

            input_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div/div/main/section/div/div[2]/form/input"))
            )
            input_btn.send_keys(Keys.RETURN)

            dates = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div/div/main/section[2]/div/div/form/div[1]/select"))
            )
            dates = Select(dates)
            date_list = dates.options
            new_date = date_list[0].text
            pdf_names = []

            for i in range(len(date_list)):
                driver.get(URL)

                input_btn = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "/html/body/div/div/main/section/div/div[2]/form/input"))
                )
                input_btn.send_keys(Keys.RETURN)

                dates = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "/html/body/div/div/main/section[2]/div/div/form/div[1]/select"))
                )
                dates = Select(dates)
                date_list = dates.options
                d = date_list[i]
                d_text = d.text.strip()
                d_date = datetime.strptime(d_text, "%d-%m-%Y").date()

                if last_date < d_date:
                    dates.select_by_value(d_text)

                    by_court = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "/html/body/div/div/main/section[2]/div/div/form/div[2]/input"))
                    )
                    by_court.click()

                    # Submit the form
                    submit_btn = driver.find_element(By.XPATH, "/html/body/div/div/main/section[2]/div/div/form/input[2]")
                    submit_btn.send_keys(Keys.RETURN)

                    final_submit = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "/html/body/div/div/main/section[2]/div/div/input[4]"))
                    )
                    final_submit.send_keys(Keys.RETURN)

                    list_download_url = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "/html/body/div/div/main/section[2]/div/div[1]/div/div/ul/li/a"))
                    )
                    response = requests.get(list_download_url.get_attribute('href'))
                    pdf_file = rf"temp_pdf/{d_text.replace('-', '_')}.pdf"
                    pdf_names.append(pdf_file)

                    with open(pdf_file, "wb") as f:
                        f.write(response.content)
                else:
                    break

            if pdf_names:
                curr.execute("UPDATE courts SET last_list_date = %s WHERE court_id = 2", (new_date,))
                return pdf_names
            return None

        except Exception as e:
            print(f"Error : {e}")
            print(traceback.extract_tb(e.__traceback__))
            return None

        finally:
            if driver:
                driver.quit()
            if curr:
                conn.commit()
                curr.close()
                conn.close()


    def clean_csv(self, input_file, encoding="utf-8"):
        try:
            with open(input_file, newline='', encoding=encoding) as f:
                reader = csv.reader(f)
                rows = list(reader)
            
            if not rows:
                print("The CSV file is empty.")
                return pd.DataFrame()
            
            max_len = max(len(row) for row in rows)
            padded_rows = [row + [''] * (max_len - len(row)) for row in rows]
            
            merged_rows = []
            current = None
            
            for row in padded_rows:
                if row[0].strip():
                    if current is not None:
                        merged_rows.append(current)
                    current = row.copy()
                else:
                    if current is None:
                        current = row.copy()
                    else:
                        for i, cell in enumerate(row):
                            cell = cell.strip()
                            if cell:
                                if current[i].strip():
                                    current[i] = current[i].strip() + " " + cell
                                else:
                                    current[i] = cell
            if current is not None:
                merged_rows.append(current)
            
            df = pd.DataFrame(merged_rows)

            df = df.dropna(axis=1, how='all')
            df = df.loc[:, (df != '').any(axis=0)]
            
            column_names = df.columns.tolist()

            cleaned_rows = []
            for _, row in df.iterrows():
                non_empty_values = [val for val in row if val is not None and str(val).strip() != '']
                cleaned_rows.append(non_empty_values)

            max_len = max(len(row) for row in cleaned_rows)
            padded_cleaned_rows = [row + [''] * (max_len - len(row)) for row in cleaned_rows]

            cleaned_df = pd.DataFrame(padded_cleaned_rows)

            num_original_cols = len(column_names)
            if len(cleaned_df.columns) > num_original_cols:
                cleaned_df = cleaned_df.iloc[:, :num_original_cols]

            if len(cleaned_df.columns) == len(column_names):
                cleaned_df.columns = column_names
            if input_file:
                cleaned_df = cleaned_df.drop(index=range(0,5),axis="index")
                cleaned_df.to_csv(input_file, index=False, encoding=encoding)
                print(f"Cleaned CSV saved to {input_file}")
            
            return True
        except Exception as e:
            print(f"Error : {e}")
            return None
    
    def process_csv(self, csv_path):
        with open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = list(csv.reader(csvfile))
            for row in reader[1:]:
                i = 0
                case_number = None
                main_parties = None
                petitoner_adv = None
                respondent_adv = None
                size = len(row)
                while i < size:
                    if re.search(r"([A-Z0-9]+)/(\d+)/(\d{4})",row[i]):
                        case_number = re.search(r"([A-Z0-9]+)/(\d+)/(\d{4})",row[i]).group(0)
                        break
                    i += 1
                i += 1
                main_parties = ''.join(row[i]) if i < size else ""
                i += 1
                petitoner_adv = ''.join(row[i]) if i < size else ""
                i += 1
                respondent_adv = ''.join(row[i]) if i < size else ""
                details = self.get_user_detail(case_number)
                if details:
                    for detail in details:
                        self.send_email(detail, case_number, petitoner_adv, respondent_adv, main_parties)   

