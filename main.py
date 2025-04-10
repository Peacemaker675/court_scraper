import multiprocessing
import pandas as pd
import pdfplumber
import inspect
import courts
import os
import logging

'''
    This is the main file for running the scraping tasks , it creates a instance of class from courts.py and executes its functions in the following order : fetch_list->extract_csv->clean_csv->process_csv, also to speed up the process and fully utilize the resources it uses multiprocessing to make the extraction of data from pdf faster.
'''

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# creates class instances and calls extract_csv function to extract tabular data from pdf
def extract_csv_wrapper(class_name, pdf_file, page_range):

    try:
        court_class = getattr(courts, class_name)
        court_instance = court_class()
        return court_instance.extract_csv(pdf_file, page_range)
    except Exception as e:
        logging.error(f"Error processing {pdf_file} on pages {page_range}: {e}")
        return None

# gets the total page count in a pdf 
def get_page_count(pdf_file):

    try:
        with pdfplumber.open(pdf_file) as pdf:
            return pdf.metadata.get("Pages", len(pdf.pages))
    except Exception as e:
        logging.error(f"Error reading PDF {pdf_file}: {e}")
        return 0

# divides the pdf into parts of 50 pages and processes them parallely
def process_pdf(court_name, pdf_file, chunk_size=50):

    num_pages = get_page_count(pdf_file)
    if num_pages < 2:
        logging.warning(f"Skipping {pdf_file}, insufficient pages.")
        return

    page_ranges = [f"{start}-{min(start + chunk_size - 1, num_pages)}" for start in range(2, num_pages, chunk_size)]
    
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.starmap(extract_csv_wrapper, [(court_name, pdf_file, page) for page in page_ranges])
    
    valid_results = [df for df in results if df is not None]
    if valid_results:
        final_df = pd.concat(valid_results, ignore_index=True)
        pdf_file_name = os.path.basename(pdf_file)  # Extract filename
        csv_path = os.path.join("temp_csv", f"{pdf_file_name[:-4]}.csv")  # Ensure correct extension handling
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        final_df.to_csv(csv_path, index=False)
        
        logging.info(f"Extraction complete! Saved as {csv_path}")
        
        court_instance = getattr(courts, court_name)()
        court_instance.clean_csv(csv_path)
        court_instance.process_csv(csv_path)
    else:
        logging.warning(f"No valid data extracted from {pdf_file}")

# the main function to process data of each court
def main():
    try:
        court_classes = {name: obj for name, obj in inspect.getmembers(courts, inspect.isclass) if name.lower().endswith("court")}
        
        for court_name, court_class in court_classes.items():
            logging.info(f"Processing court: {court_name}")
            court_instance = court_class()
            pdf_files = court_instance.fetch_list()
            
            if pdf_files:
                for pdf_file in pdf_files:
                    if pdf_file:
                        process_pdf(court_name, pdf_file)
            else:
                logging.warning(f"No PDF lists found for {court_name}")
                
        # cleaning files after work to save space
        if os.path.exists("temp_csv"):
            for file in os.listdir("temp_csv"):
                file_path = os.path.join("temp_csv", file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        if os.path.exists("temp_pdf"):
            for file in os.listdir("temp_pdf"):
                file_path = os.path.join("temp_pdf", file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
    except Exception as e:
        logging.error(f"Critical error: {e}")

if __name__ == "__main__":
    main()
