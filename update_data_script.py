from urllib.parse import urljoin
import pandas as pd
import json
import requests
from bs4 import BeautifulSoup
import re
import sys
from datetime import datetime
import time
from pathlib import Path

def log_message(step, message, status="INFO"):
    """Helper function for consistent logging"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{status}] STEP {step}: {message}")

def step0_download_stfi_csv():
    """
    STEP 0: Download CSV file from SimpletoolsForInvestors
    """
    log_message(0, "Starting CSV download from STFI")
    
    url = "https://www.simpletoolsforinvestors.eu/documentivari.php"
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        log_message(0, f"Unable to fetch the page: {e}", "ERROR")
        sys.exit(1)

    soup = BeautifulSoup(response.content, 'html.parser')
    csv_link = None

    # Cerca in ogni riga TR
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) == 2:
            if "Rendimenti e durate calcolati End of Day" in tds[0].get_text(strip=True):
                a_tag = tds[1].find("a", href=True)
                if a_tag:
                    csv_link = urljoin(url, a_tag['href'])
                    break

    if csv_link:
        try:
            log_message(0, f"Found CSV link: {csv_link}")
            csv_response = requests.get(csv_link)
            csv_response.raise_for_status()
            with open("downloaded.csv", "wb") as f:
                f.write(csv_response.content)
            log_message(0, "CSV file successfully downloaded", "SUCCESS")
        except requests.RequestException as e:
            log_message(0, f"Error downloading CSV file: {e}", "ERROR")
            sys.exit(1)
    else:
        log_message(0, "CSV link not found", "ERROR")
        sys.exit(1)


def step1_csv_to_json(file_path):
    """
    STEP 1: Convert CSV file to JSON
    """
    log_message(1, "Starting CSV to JSON conversion")
    
    try:
        # Try first with ';' separator then with ','
        try:
            df = pd.read_csv(file_path, sep=";", decimal=",", on_bad_lines="skip", encoding="utf-8")
            log_message(1, "CSV successfully read using ';' separator")
        except Exception:
            df = pd.read_csv(file_path, sep=",", decimal=",", on_bad_lines="skip", encoding="utf-8")
            log_message(1, "CSV successfully read using ',' separator")
        
        # Remove columns that start with "unnamed"
        original_cols = len(df.columns)
        df = df.loc[:, ~df.columns.str.lower().str.startswith('unnamed')]
        removed_cols = original_cols - len(df.columns)
        if removed_cols > 0:
            log_message(1, f"Removed {removed_cols} 'unnamed' columns")
        
        # Automatically convert numeric columns
        numeric_conversions = 0
        for col in df.columns:
            original_dtype = df[col].dtype
            df[col] = pd.to_numeric(df[col], errors="ignore")
            if original_dtype != df[col].dtype:
                numeric_conversions += 1
        
        if numeric_conversions > 0:
            log_message(1, f"Converted {numeric_conversions} columns to numeric format")
        
        # Replace NaN with None
        df = df.where(pd.notnull(df), None)
        
        # Convert to JSON
        json_data = json.loads(df.to_json(orient="records", force_ascii=False))
        
        log_message(1, f"Conversion completed: {len(json_data)} records found", "SUCCESS")
        
        # Check if ISIN column exists
        if len(json_data) > 0:
            sample_keys = list(json_data[0].keys())
            isin_column = None
            for key in sample_keys:
                if 'ISIN' in key.upper():
                    isin_column = key
                    break
            
            if isin_column:
                log_message(1, f"ISIN column found: '{isin_column}'")
            else:
                log_message(1, "WARNING: No ISIN column found in CSV", "WARNING")
        
        return json_data
        
    except Exception as e:
        log_message(1, f"Error during CSV conversion: {e}", "ERROR")
        return None

def step2_fetch_isin_bondid_mapping():
    """
    STEP 2: Fetch ISIN -> bondId mapping from website
    """
    log_message(2, "Starting ISIN-bondId mapping retrieval")
    
    url = "https://www.simpletoolsforinvestors.eu/yieldtable.php?datatype=EOD"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # GET request to the page
        log_message(2, f"Sending request to: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        log_message(2, "Page successfully retrieved")
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table
        table_wrapper = soup.find(id="YieldTable")
        if not table_wrapper:
            raise ValueError("YieldTable not found")
        
        log_message(2, "YieldTable found, starting parsing")
        
        # Find all rows
        rows = table_wrapper.find_all('tr')
        log_message(2, f"Found {len(rows)} rows in table")
        
        # Dictionary for ISIN -> bondId mapping
        isin_bond_map = {}
        
        # Regex pattern to extract bondID
        bond_id_pattern = r'bondID=(\d+)'
        
        # Process each row
        for row in rows:
            cells = row.find_all('td')
            if cells:
                # First cell contains ISIN
                isin = cells[0].text.strip()
                
                # Look for cell with bondID link
                for cell in cells:
                    link = cell.find('a')
                    if link and 'href' in link.attrs:
                        href = link['href']
                        match = re.search(bond_id_pattern, href)
                        if match:
                            bond_id = match.group(1)
                            isin_bond_map[isin] = bond_id
                            break
        
        log_message(2, f"Mapping completed: {len(isin_bond_map)} ISIN-bondId pairs found", "SUCCESS")
        return isin_bond_map
        
    except requests.RequestException as e:
        log_message(2, f"Error retrieving page: {e}", "ERROR")
        return {}
    except Exception as e:
        log_message(2, f"Error processing data: {e}", "ERROR")
        return {}

def step3_get_issue_price(bond_id):
    """
    STEP 3: Retrieve issue price for a given bondId
    """
    xml_url = f"https://www.simpletoolsforinvestors.eu/data/definitions/{bond_id}.xml"
    backoff = 60  # seconds, doubles until a maximum
    max_backoff = 600
    
    while True:
        try:
            response = requests.get(xml_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "lxml-xml")
            issue_price_tag = soup.find("issueprice")
            
            if issue_price_tag:
                try:
                    # Convert to float, replacing comma with dot if needed
                    price_text = issue_price_tag.text.strip().replace(',', '.')
                    return float(price_text)
                except ValueError:
                    return None
            else:
                return None        
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 429:
                print(f"\nToo many requests for bondId {bond_id}. Pausing for {backoff} seconds...")
                for remaining in range(backoff, 0, -1):
                    print(f"\rResuming in: {remaining:02d}s", end="")
                    time.sleep(1)
                print("\nResuming requests...")
                backoff = min(backoff * 2, max_backoff)
                continue
            else:
                print(f"\nHTTP {status} while retrieving issue price for bondId {bond_id}")
                return None
        except Exception as e:
            print(f"\nError while retrieving issue price for bondId {bond_id}: {str(e)}")
            return None    
        # except Exception as e:
        #     if e.response.status_code == 429:  # Too Many Requests
        #         pause_minutes = 5
        #         print(f"\nToo many requests for bondId {bond_id}. Pausing for {pause_minutes} minutes...")
                
        #         # Show countdown
        #         for remaining in range(pause_minutes * 60, 0, -1):
        #             minutes = remaining // 60
        #             seconds = remaining % 60
        #             print(f"\rResuming in: {minutes:02d}:{seconds:02d}", end="")
        #             time.sleep(1)
                
        #         print("\nRiprendo le richieste...")
        #         continue  # Riprova dopo la pausa
                
        #     print(f"\nHTTP Error while retrieving issue price for bondId {bond_id}: {str(e)}")
        #     return None

def step3_enrich_with_issue_prices(json_data, isin_bond_map):
    """
    STEP 3: Enrich JSON with issue prices
    """
    log_message(3, "Starting issue prices retrieval")
    
    if not json_data or not isin_bond_map:
        log_message(3, "Missing data for issue prices retrieval", "WARNING")
        return json_data
    
    # Trova la colonna ISIN nel JSON
    if len(json_data) > 0:
        sample_keys = list(json_data[0].keys())
        isin_column = None
        for key in sample_keys:
            if 'ISIN' in key.upper():
                isin_column = key
                break
        
        if not isin_column:
            log_message(3, "ISIN column not found in JSON", "ERROR")
            return json_data
        
        log_message(3, f"Using column '{isin_column}' for ISIN")
        
        # Statistics counters
        total_records = len(json_data)
        bondid_added = 0
        price_found = 0
        price_not_found = 0
        
        # Enrich each record
        for i, record in enumerate(json_data):
            isin = record.get(isin_column)
            
            # Skip if the record already has a valid issue price
            if 'issueprice' in record and record['issueprice'] is not None:
                price_found += 1
                bondid_added += 1  # Since if it has an issue price, it must have a bondId
                continue
            
            if isin and isin in isin_bond_map:
                bond_id = isin_bond_map[isin]
                record['bondid'] = bond_id
                bondid_added += 1
                
                # Retrieve issue price
                print(f"\rRetrieving issue price for ISIN {isin} (bondId: {bond_id}) - Record {i+1}/{total_records}", end="")
                issue_price = step3_get_issue_price(bond_id)
                
                if issue_price:
                    record['issueprice'] = issue_price
                    price_found += 1
                else:
                    record['issueprice'] = None
                    price_not_found += 1
            else:
                record['bondid'] = None
                record['issueprice'] = None
                if isin:
                    log_message(3, f"BondId not found for ISIN: {isin} - Record {i+1}/{total_records}", "WARNING")
        
        # New line after status line
        print()
        
        # Final report
        log_message(3, f"Enrichment completed:", "SUCCESS")
        log_message(3, f"  - Total records: {total_records}")
        log_message(3, f"  - BondIds added: {bondid_added}")
        log_message(3, f"  - Issue prices found: {price_found}")
        log_message(3, f"  - Issue prices not found: {price_not_found}")
        
    return json_data

def load_existing_json(file_path):
    """
    Load existing JSON if present, otherwise return None
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

def merge_data(existing_data, new_data, isin_column):
    """
    Merge existing data with new data:
    - Keep static data (bondid, issueprice) from existing records if available
    - Use all other fields from new data (prices, yields, etc.)
    """
    if not existing_data:
        return new_data
        
    # Create a dictionary of existing data using ISIN as key
    existing_dict = {record[isin_column]: record for record in existing_data}
    
    # List of fields that are static (don't change over time)
    static_fields = ['bondid', 'issueprice']
    
    # Update or add new records
    for new_record in new_data:
        isin = new_record[isin_column]
        if isin in existing_dict:
            # Keep static fields from existing record if they exist and are not null
            for field in static_fields:
                if field in existing_dict[isin] and existing_dict[isin][field] is not None:
                    new_record[field] = existing_dict[isin][field]
    
    return new_data

def main():
    """
    Main function that orchestrates all steps
    """
    print("=" * 80)
    print("STARTING UNIFIED BOND DATA PROCESSING")
    print("=" * 80)
    
    # Configuration
    csv_file_path = "downloaded.csv"  # File downloaded from STFI
    output_file_path = "docs/output_enriched.json"

    # Ensure target folder exists (first run safety)
    Path(output_file_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Load existing JSON data for static fields
    existing_data = load_existing_json(output_file_path)
    if existing_data:
        log_message("MAIN", f"Found existing JSON file with {len(existing_data)} records")
    
    # Always download fresh CSV for updated prices
    print("\n" + "-" * 80)
    print("STEP 0: DOWNLOADING CSV FROM STFI")
    print("-" * 80)
    step0_download_stfi_csv()
    
    # Load existing data for potential merge
    existing_data = load_existing_json(output_file_path)
    if existing_data:
        log_message(0, f"Loaded {len(existing_data)} records from existing JSON file")
    
    # STEP 1: CSV to JSON
    print("\n" + "-" * 80)
    print("STEP 1: CSV TO JSON CONVERSION")
    print("-" * 80)
    json_data = step1_csv_to_json(csv_file_path)
    
    if not json_data:
        log_message(0, "Processing interrupted due to errors in STEP 1", "ERROR")
        sys.exit(1)
    
    # Find ISIN column
    isin_column = None
    if len(json_data) > 0:
        for key in json_data[0].keys():
            if 'ISIN' in key.upper():
                isin_column = key
                break
    
    if not isin_column:
        log_message(0, "ISIN column not found in CSV", "ERROR")
        sys.exit(1)
    
    # Merge with existing data to preserve static fields
    if existing_data:
        log_message(0, "Merging existing static data with new CSV data")
        json_data = merge_data(existing_data, json_data, isin_column)
    
    # STEP 2: Retrieve ISIN-bondId mapping only for records that need it
    print("\n" + "-" * 80)
    print("STEP 2: RETRIEVING ISIN-BONDID MAPPING")
    print("-" * 80)
    
    # Filter ISINs that need enrichment
    isin_to_process = set()
    for record in json_data:
        if ('bondid' not in record or record['bondid'] is None or 
            'issueprice' not in record or record['issueprice'] is None):
            isin_to_process.add(record[isin_column])
    
    if isin_to_process:
        log_message(2, f"Need to retrieve information for {len(isin_to_process)} ISINs")
        isin_bond_map = step2_fetch_isin_bondid_mapping()
    else:
        log_message(2, "No new ISINs to process")
        isin_bond_map = {}
    
    if not isin_bond_map:
        log_message(0, "No ISIN-bondId mapping found, continuing without enrichment", "WARNING")
    
    # STEP 3: Enrich with issue prices only for necessary records
    print("\n" + "-" * 80)
    print("STEP 3: ENRICHING WITH ISSUE PRICES")
    print("-" * 80)
    
    # Filter only records that need enrichment
    records_to_process = [
        record for record in json_data 
        if ('bondid' not in record or record['bondid'] is None or 
            'issueprice' not in record or record['issueprice'] is None)
    ]
    
    if records_to_process:
        log_message(3, f"Processing {len(records_to_process)} records that need enrichment")
        enriched_subset = step3_enrich_with_issue_prices(records_to_process, isin_bond_map)
        
        # Update original records with new data
        enriched_dict = {record[isin_column]: record for record in enriched_subset}
        for record in json_data:
            isin = record[isin_column]
            if isin in enriched_dict:
                record.update(enriched_dict[isin])
        
        enriched_json = json_data
    else:
        log_message(3, "No records need enrichment")
        enriched_json = json_data
    
    # Save final result
    print("\n" + "-" * 80)
    print("SAVING FINAL RESULT")
    print("-" * 80)
    
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(enriched_json, f, ensure_ascii=False, indent=2)
        log_message(0, f"Final JSON file saved to: {output_file_path}", "SUCCESS")
        
        # Final statistics
        if enriched_json and len(enriched_json) > 0:
            sample = enriched_json[0]
            log_message(0, f"Keys in final JSON: {list(sample.keys())}")
            
    except Exception as e:
        log_message(0, f"Error saving file: {e}", "ERROR")
        sys.exit(1)
    
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("=" * 80)

if __name__ == "__main__":
    main()
