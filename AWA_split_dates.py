import os
import pandas as pd
import shutil
from copy import deepcopy
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import re

# Get absolute path of script
DATASETS_DIR = os.path.dirname(os.path.abspath(__file__))

def validate_date_format(date_str):
    """Validate if date format is YYYYMMDD"""
    if not re.match(r'^\d{8}$', date_str):
        return False
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False


def get_user_input():
    """Get all user input parameters"""
    # Get inception date
    while True:
        inception_date = input("Please enter inception date (format: YYYYMMDD): ").strip()
        if validate_date_format(inception_date):
            inception_date = datetime.strptime(inception_date, '%Y%m%d')
            break
        else:
            print("Error: Invalid date format, please use YYYYMMDD format")
    
    # Get folder name to process
    while True:
        folder_name = input("Enter the folder name only (no full path required): ").strip()
        folder_path = os.path.join(DATASETS_DIR, folder_name)
        if folder_name in os.listdir(DATASETS_DIR):
            # Extract date from folder name as end date
            date_match = re.match(r'^(\d{4}-\d{2}-\d{2})', folder_name)
            if date_match:
                end_date = datetime.strptime(date_match.group(1), '%Y-%m-%d')
                break
            else:
                print("Error: Folder name should start with YYYY-MM-DD format date")
        else:
            print(f"Error: Folder {folder_path} does not exist")
    
    # Get split frequency
    frequency_options = ['daily', 'monthly', 'quarterly', 'annually']
    while True:
        frequency = input("Please select split frequency (daily/monthly/quarterly/annually): ").strip().lower()
        if frequency in frequency_options:
            break
        else:
            print(f"Error: Please choose one of the following options: {', '.join(frequency_options)}")
    
    return inception_date, end_date, folder_name, frequency


def get_period_end_date(start_date, frequency):
    """Get period end date based on frequency"""
    if frequency == 'daily':
        return start_date
    elif frequency == 'monthly':
        # Get last day of the month
        next_month = start_date.replace(day=28) + timedelta(days=4)
        return next_month - timedelta(days=next_month.day)
    elif frequency == 'quarterly':
        # Get last day of the quarter
        quarter = (start_date.month - 1) // 3 + 1
        if quarter == 1:
            return datetime(start_date.year, 3, 31)
        elif quarter == 2:
            return datetime(start_date.year, 6, 30)
        elif quarter == 3:
            return datetime(start_date.year, 9, 30)
        else:
            return datetime(start_date.year, 12, 31)
    elif frequency == 'annually':
        return datetime(start_date.year, 12, 31)


def get_next_period_end(current_date, frequency):
    """Get end date of next period"""
    if frequency == 'daily':
        return current_date + timedelta(days=1)
    elif frequency == 'monthly':
        return current_date + relativedelta(months=1)
    elif frequency == 'quarterly':
        return current_date + relativedelta(months=3)
    elif frequency == 'annually':
        return current_date + relativedelta(years=1)


def create_folder_structure(inception_date, end_date, frequency):
    """Create folder structure and return all involved folder names"""
    folders_to_process = []
    current_date = inception_date
    
    while current_date <= end_date:
        period_end = get_period_end_date(current_date, frequency)
        
        # If period end date exceeds final date, use final date
        if period_end > end_date:
            period_end = end_date
        
        folder_name = period_end.strftime('%Y-%m-%d')
        folder_path = os.path.join(DATASETS_DIR, folder_name)
        folders_to_process.append(folder_name)
        
        # Create main 
        # If folder already exists, don't replace it, maybe do it later
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        # Create subfolders
        # If folder already exists, don't replace it, maybe do it later
        subfolders = ['context_data', 'perf', 'portfolio']
        for subfolder in subfolders:
            subfolder_path = os.path.join(folder_path, subfolder)
            if not os.path.exists(subfolder_path):
                os.makedirs(subfolder_path)
        
        # Move to next period
        if period_end >= end_date:
            break
        
        current_date = get_next_period_end(period_end, frequency)
    
    return folders_to_process


def load_csv_files(source_folder):
    """Load CSV files"""
    ho_path = os.path.join(DATASETS_DIR, source_folder, 'perf', 'Holdings.csv')
    tr_path = os.path.join(DATASETS_DIR, source_folder, 'perf', 'Transactions.csv')
    
    if not os.path.exists(ho_path):
        raise FileNotFoundError(f"Holdings.csv not found in {ho_path}")
    if not os.path.exists(tr_path):
        raise FileNotFoundError(f"Transactions.csv not found in {tr_path}")
    
    # Load CSV files
    df_ho = pd.read_csv(ho_path, dtype = {'Portfolio Code':str, 'Security Code':str, 'Valuation Date':str}, on_bad_lines = 'skip')
    df_tr = pd.read_csv(tr_path, dtype = {'Portfolio Code':str, 'Security Code':str, 'Transaction Date':str})
    
    # Get rid of any hidden characters in column Valuation Date and Transaction Date
    df_ho['Valuation Date'] = df_ho['Valuation Date'].astype(str).str.strip()
    df_tr['Transaction Date'] = df_tr['Transaction Date'].astype(str).str.strip()
    
    # Remove weird case of header lines appear multiple times (possibly because data is too large)
    df_ho = df_ho[df_ho['Valuation Date'] != 'Valuation Date']
    df_tr = df_tr[df_tr['Transaction Date'] != 'Transaction Date']
    
    return df_ho, df_tr


def filter_and_save_csv(df_ho, df_tr, inception_date, folder_date, folder_name):
    """Filter CSV data and save to corresponding folder"""
    # Use deepcopy to avoid affecting original dataframes
    df_ho_temp = deepcopy(df_ho)
    df_tr_temp = deepcopy(df_tr)
    
    # Convert date columns to datetime for comparison
    df_ho_temp['Valuation Date Datetime'] = pd.to_datetime(df_ho_temp['Valuation Date'], format='%m/%d/%Y')
    df_tr_temp['Transaction Date Datetime'] = pd.to_datetime(df_tr_temp['Transaction Date'], format='%m/%d/%Y')
    
    # Filter Holdings data
    df_ho_filtered = df_ho_temp[
        (df_ho_temp['Valuation Date Datetime'] >= inception_date) & 
        (df_ho_temp['Valuation Date Datetime'] <= folder_date)
    ].drop('Valuation Date Datetime', axis = 1)
    
    # Filter Transactions data
    df_tr_filtered = df_tr_temp[
        (df_tr_temp['Transaction Date Datetime'] >= inception_date) & 
        (df_tr_temp['Transaction Date Datetime'] <= folder_date)
    ].drop('Transaction Date Datetime', axis = 1)
    
    # Save filtered data
    perf_folder = os.path.join(DATASETS_DIR, folder_name, 'perf')
    ho_output_path = os.path.join(perf_folder, 'Holdings.csv')
    tr_output_path = os.path.join(perf_folder, 'Transactions.csv')
    
    df_ho_filtered.to_csv(ho_output_path, index=False)
    df_tr_filtered.to_csv(tr_output_path, index=False)


def copy_additional_folders(source_folder, target_folders):
    """Copy context_data and portfolio folders to all target folders"""
    source_context = os.path.join(DATASETS_DIR, source_folder, 'context_data')
    source_portfolio = os.path.join(DATASETS_DIR, source_folder, 'portfolio')
    
    for target_folder in target_folders:
        target_context = os.path.join(DATASETS_DIR, target_folder, 'context_data')
        target_portfolio = os.path.join(DATASETS_DIR, target_folder, 'portfolio')
        
        # Copy context_data folder
        # Here will replace the original subfolder
        if os.path.exists(source_context):
            if os.path.exists(target_context):
                shutil.rmtree(target_context)
            shutil.copytree(source_context, target_context)
            print(f"Copied context_data to {target_folder}")
        
        # Copy portfolio folder
        # Here will replace the original subfolder
        if os.path.exists(source_portfolio):
            if os.path.exists(target_portfolio):
                shutil.rmtree(target_portfolio)
            shutil.copytree(source_portfolio, target_portfolio)
            print(f"Copied portfolio to {target_folder}")


def main():
    """Main function"""
    try:
        print("CSV File Split Script Started")
        print("=" * 50)
        
        # Get user input
        inception_date, end_date, source_folder, frequency = get_user_input()
        
        print("\nConfiguration:")
        print(f"Inception Date: {inception_date.strftime('%Y-%m-%d')}")
        print(f"End Date: {end_date.strftime('%Y-%m-%d')}")
        print(f"Source Folder: {source_folder}")
        print(f"Frequency: {frequency}")
        
        # Create folder structure
        folders_to_process = create_folder_structure(inception_date, end_date, frequency)
        
        # Load CSV files
        df_ho, df_tr = load_csv_files(source_folder)
        
        # Process each folder
        print("\nProcessing CSV data...")
        for folder_name in folders_to_process:
            folder_date = datetime.strptime(folder_name, '%Y-%m-%d')
            filter_and_save_csv(df_ho, df_tr, inception_date, folder_date, folder_name)
        
        # Copy other folders
        print("Copying other subfolders...")
        copy_additional_folders(source_folder, folders_to_process)
        
        print("Run is done!")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()