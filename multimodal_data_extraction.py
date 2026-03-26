#current working version : 19/04

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from urllib.parse import urljoin
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create a directory to store downloaded images
if not os.path.exists("images"):
    os.makedirs("images")

# Function to download an image and return its local path
def download_image(img_url, idx, base_url, session):
    img_path = f"images/image{idx}.png"
    try:
        if not img_url.startswith(('http:', 'https:')):
            img_url = urljoin(base_url, img_url)

        logger.info(f"Downloading image from {img_url} to {img_path}...")
        response = session.get(img_url, timeout=15)
        response.raise_for_status()

        with open(img_path, 'wb') as f:
            f.write(response.content)

        # Verify the image was saved
        if os.path.exists(img_path) and os.path.getsize(img_path) > 0:
            logger.info(f"Successfully saved image to {img_path}")
            return img_path
        else:
            logger.error(f"Image file {img_path} is empty or does not exist after download")
            return img_path  # Return the path as a placeholder

    except Exception as e:
        logger.error(f"Error downloading image {img_url}: {str(e)}")
        return img_path  # Return the path as a placeholder

# Function to determine if a column is numeric
def is_numeric_column(column_data):
    total = 0
    numeric = 0
    for value in column_data:
        if value and not value.startswith('images/'):
            total += 1
            cleaned = re.sub(r'[^\d]', '', value)
            if cleaned and cleaned.isdigit():
                numeric += 1
    return total > 0 and (numeric / total) > 0.5

# Function to clean numeric values
def clean_numeric(value):
    if not value or value.startswith('images/'):
        return value
    cleaned = re.sub(r'[^\d,]', '', value).replace(',', '')
    return cleaned if cleaned.isdigit() else value

# Function to process a table row, handling rowspan and colspan
def process_row(cells, headers_len, img_counter, base_url, session):
    row_data = [""] * headers_len
    current_col = 0

    for cell in cells:
        colspan = int(cell.get('colspan', 1))
        rowspan = int(cell.get('rowspan', 1))  # Simplified rowspan handling

        img_tag = cell.find('img')
        if img_tag and img_tag.get('src'):
            img_url = img_tag['src']
            img_path = download_image(img_url, img_counter[0], base_url, session)
            row_data[current_col] = img_path
            img_counter[0] += 1
        else:
            cell_text = cell.get_text(strip=True)
            row_data[current_col] = cell_text if cell_text else f"images/image{img_counter[0]}.png"
            img_counter[0] += 1

        current_col += colspan
        if current_col >= headers_len:
            break

    return row_data

# Function to extract tables using BeautifulSoup
def extract_tables_from_page(soup, base_url, session):
    tables_data = []
    tables = soup.find_all('table')  # Find all tables, not limited to specific classes

    for table_idx, table in enumerate(tables):
        # Extract headers
        headers = [th.get_text(strip=True) for th in table.find_all('th')]
        if not headers:
            first_row = table.find('tr')
            if first_row:
                headers = [td.get_text(strip=True) for td in first_row.find_all('td')]
                rows_start = 1
            else:
                logger.warning(f"Table {table_idx + 1} has no headers, skipping.")
                continue
        else:
            rows_start = 0

        if not headers:
            continue

        # Extract rows
        rows = []
        img_counter = [1]
        for row in table.find_all('tr')[rows_start:]:
            cells = row.find_all('td')
            if cells:
                row_data = process_row(cells, len(headers), img_counter, base_url, session)
                if row_data.count("") < len(headers):  # Ensure the row has some data
                    rows.append(row_data)

        if rows:
            tables_data.append({'headers': headers, 'rows': rows, 'img_counter': img_counter[0]})

    return tables_data

# Main function to scrape tables from a URL
def scrape_tables_from_url(url, output_prefix="table"):
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        logger.info(f"Attempting to fetch {url}...")
        response = session.get(url, timeout=30)
        response.raise_for_status()
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        tables_data = extract_tables_from_page(soup, url, session)

        if not tables_data:
            logger.warning(f"No tables found on {url}")
            return

        for table_idx, table_data in enumerate(tables_data):
            headers = table_data['headers']
            rows = table_data['rows']
            img_counter = table_data['img_counter']

            df = pd.DataFrame(rows, columns=headers)
            numeric_columns = [col for col in df.columns if is_numeric_column(df[col])]

            for col in df.columns:
                if col in numeric_columns:
                    df[col] = df[col].apply(clean_numeric)
                else:
                    df[col] = df[col].apply(
                        lambda x: x if x.startswith('images/') else f"images/image{img_counter}.png"
                    )
                    img_counter += 1

            output_file = f"{output_prefix}_table_{table_idx + 1}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Table {table_idx + 1} from {url} saved to {output_file}")
            print(df)
            print()

    except Exception as e:
        logger.error(f"Error processing {url}: {str(e)}")

# Main function to process multiple URLs
def main():
    urls = [
        #"https://en.wikipedia.org/wiki/Bundesliga",
       # "https://en.wikipedia.org/wiki/Premier_League",
         "https://www.britannica.com/topic/Presidents-of-the-United-States-1846696"
        # Add any other URLs here or modify to accept user input
    ]

    for url in urls:
        logger.info(f"Processing {url}...")
        scrape_tables_from_url(url, output_prefix=url.split('/')[-1].lower() if '/' in url else "table")
        logger.info("-" * 50)

if __name__ == "__main__":
    main()

#tried urls :
#
#"https://en.wikipedia.org/wiki/Bundesliga",
       # "https://en.wikipedia.org/wiki/Premier_League",
   #      "https://www.britannica.com/topic/Presidents-of-the-United-States-1846696"
        #
#



#changes needed in the below version

!apt-get update
!apt-get install -y chromium-driver

!pip install google-generativeai selenium pandas Pillow requests beautifulsoup4 --quiet

import os
import re
import uuid
import time
import json

from urllib.parse import urljoin, urlparse

import google.generativeai as genai
import pandas as pd
import requests
from bs4 import BeautifulSoup
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions

# For displaying things in Colab
from IPython.display import display, Image as IPImage


try:
    from google.colab import userdata
    API_KEY = userdata.get('GEMINI_API_KEY')
    if not API_KEY:
        raise ValueError("API Key not found in Colab Secrets. Please add 'GEMINI_API_KEY' to Secrets.")
    print("Successfully loaded API Key from Colab Secrets.")
except ImportError:
    # Fallback if not in Colab or secrets issue - prompt user (less secure)
    print("Could not import Colab userdata. You might not be in a Colab environment.")
    print("Attempting to read API_KEY from environment variable (if set)...")
    API_KEY = os.getenv("GEMINI_API_KEY")
    if not API_KEY:
         # Or prompt: API_KEY = input("Please enter your Gemini API Key: ")
         raise ValueError("GEMINI_API_KEY environment variable not set and Colab Secrets unavailable.")

# Configure the Gemini client
genai.configure(api_key=API_KEY)

print("Gemini API configured.")

# Cell 3: Configuration and Output Directories

# --- Configuration ---

# Output directories (will be created in Colab's temporary storage)
OUTPUT_DIR = "extracted_data"
CSV_DIR = os.path.join(OUTPUT_DIR, "csv_output")
IMG_DIR = os.path.join(OUTPUT_DIR, "images")

# --- Optional: Mount Google Drive ---
# If you want to save results directly to your Google Drive, uncomment the following lines.
# You will be prompted to authorize Colab to access your Drive.
# Results will be saved in a folder named 'Colab_WebTable_Extractor' in your Drive root.

# from google.colab import drive
# drive.mount('/content/drive')
# DRIVE_OUTPUT_FOLDER = '/content/drive/MyDrive/Colab_WebTable_Extractor'
# OUTPUT_DIR = os.path.join(DRIVE_OUTPUT_FOLDER, "extracted_data")
# CSV_DIR = os.path.join(OUTPUT_DIR, "csv_output")
# IMG_DIR = os.path.join(OUTPUT_DIR, "images")
# print(f"Output will be saved to Google Drive: {OUTPUT_DIR}")


# Create output directories
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(IMG_DIR, exist_ok=True)

print(f"Output CSVs will be saved to: {CSV_DIR}")
print(f"Output Images will be saved to: {IMG_DIR}")

# --- Helper Functions (Including Colab WebDriver Setup) ---

def setup_driver():
    """Sets up the Selenium WebDriver for Colab."""
    print("Setting up Selenium WebDriver for Colab...")
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless")  # Must run headless in Colab
    chrome_options.add_argument("--no-sandbox") # Needed for Colab/Linux environments
    chrome_options.add_argument("--disable-dev-shm-usage") # Overcomes limited resource problems
    chrome_options.add_argument("--window-size=1920,1080") # Set a reasonable window size

    # Correct path for chromedriver installed via apt-get in Colab
    # Note: Sometimes this path might vary slightly depending on updates.
    # If this fails, try !ls /usr/lib/chromium-browser/ to check the exact name.
    chromedriver_path = "/usr/bin/chromedriver"

    try:
        # Use the explicit path to the chromedriver
        service = ChromeService(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("WebDriver setup complete.")
        return driver
    except Exception as e:
        print(f"Error setting up WebDriver: {e}")
        print(f"Tried to use chromedriver at: {chromedriver_path}")
        print("Ensure chromium-driver was installed correctly in Cell 1.")
        raise

def get_page_content_and_screenshot(url, driver):
    """Fetches HTML, takes a screenshot, and returns both."""
    print(f"Accessing URL: {url}")
    try:
        driver.get(url)
        # Increase sleep time slightly for potentially slower Colab network/page loads
        print("Waiting for page to load...")
        time.sleep(7) # Adjust if needed

        # Get HTML source
        html_content = driver.page_source
        print("HTML content fetched.")

        # Take Screenshot
        screenshot_path = os.path.join(OUTPUT_DIR, "screenshot.png")

        # Basic full-page screenshot attempt for Colab
        try:
            original_size = driver.get_window_size()
            required_width = driver.execute_script('return document.body.parentNode.scrollWidth')
            required_height = driver.execute_script('return document.body.parentNode.scrollHeight')
            driver.set_window_size(required_width, required_height)
            time.sleep(1) # Allow resize
            driver.find_element(webdriver.common.by.By.TAG_NAME,'body').screenshot(screenshot_path) # Screenshot element
            driver.set_window_size(original_size['width'], original_size['height']) # Reset size
            print(f"Screenshot saved to {screenshot_path}")
        except Exception as ss_err:
             print(f"Warning: Could not take full page screenshot ({ss_err}). Taking viewport screenshot.")
             driver.set_window_size(1920, 1080) # Reset to default if failed
             driver.save_screenshot(screenshot_path)
             print(f"Viewport screenshot saved to {screenshot_path}")


        return html_content, screenshot_path
    except Exception as e:
        print(f"Error fetching page content or taking screenshot: {e}")
        return None, None

def sanitize_filename(filename):
    """Removes invalid characters for filenames."""
    if not isinstance(filename, str):
        filename = str(filename) # Ensure it's a string
    if filename.startswith(('http://', 'https://')):
        filename = os.path.basename(urlparse(filename).path)
    sanitized = re.sub(r'[^\w\-_\.]', '_', filename)
    return sanitized[:100]

def download_image(img_url, base_url):
    """Downloads an image from a URL and saves it locally."""
    if not img_url or not isinstance(img_url, str):
        print(f"Skipping invalid image URL: {img_url}")
        return None

    try:
        absolute_img_url = urljoin(base_url, img_url)
        # Basic check for common problematic URLs (like data URIs)
        if absolute_img_url.startswith('data:image'):
             print(f"Skipping data URI image: {absolute_img_url[:50]}...")
             return None

        print(f"Attempting to download image: {absolute_img_url}")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(absolute_img_url, stream=True, timeout=20, headers=headers) # Increased timeout
        response.raise_for_status()

        content_type = response.headers.get('content-type')
        if not content_type or 'image' not in content_type:
            print(f"Warning: URL {absolute_img_url} did not return an image content-type ({content_type}). Skipping.")
            return None

        img_extension = os.path.splitext(urlparse(absolute_img_url).path)[1]
        if not img_extension or len(img_extension) > 5: # Basic sanity check for extension
             if content_type and 'image' in content_type:
                 mime_subtype = content_type.split('/')[-1].split(';')[0]
                 if mime_subtype:
                     img_extension = '.' + mime_subtype
                 else:
                     img_extension = '.jpg' # Fallback
             else:
                 img_extension = '.jpg'

        original_filename = os.path.basename(urlparse(absolute_img_url).path)
        sanitized_original = sanitize_filename(original_filename) if original_filename else "image"
        unique_id = uuid.uuid4().hex[:8]
        # Ensure extension starts with a dot
        if not img_extension.startswith('.'):
            img_extension = '.' + img_extension
        local_filename = f"img_{sanitized_original}_{unique_id}{img_extension}"
        local_filepath = os.path.join(IMG_DIR, local_filename)

        with open(local_filepath, 'wb') as f:
            for chunk in response.iter_content(8192): # Slightly larger chunk size
                f.write(chunk)

        print(f"Image downloaded successfully: {local_filepath}")
        # Optionally display the downloaded image in Colab
        try:
            display(IPImage(filename=local_filepath, width=100))
        except Exception as display_err:
            print(f"Could not display image preview: {display_err}")
        return local_filepath

    except requests.exceptions.MissingSchema:
         print(f"Error downloading image: Invalid URL format (maybe missing http/https?): {img_url}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading image {absolute_img_url}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred downloading {absolute_img_url}: {e}")
    return None

# --- Gemini Extraction Function (No changes needed from original) ---

def extract_tables_with_gemini(screenshot_path, url, html_content=None):
    """Uses Gemini Vision to extract tables from the screenshot."""
    print("\nPreparing request for Gemini API...")
    # Use a Gemini model that supports multimodal input
    # 'gemini-1.5-flash-latest' is often good value, or 'gemini-pro-vision'
    model = genai.GenerativeModel('gemini-1.5-flash-latest') # Or 'gemini-pro-vision'
    try:
        screenshot_image = Image.open(screenshot_path)
    except FileNotFoundError:
        print(f"Error: Screenshot file not found at {screenshot_path}")
        return None
    except Exception as e:
        print(f"Error opening screenshot image: {e}")
        return None

    prompt = f"""
    Analyze the provided screenshot of the webpage: {url}.
    Identify all data tables visible in the screenshot.
    For each table found:
    1. Extract the content of each cell (row by row, column by column). Treat the visually first row as potential headers unless it clearly isn't.
    2. If a cell contains primarily text, extract the text as accurately as possible. Include surrounding text if it seems part of the cell data.
    3. If a cell contains an image, identify the image's source URL (usually the 'src' attribute). Represent this cell's content as a JSON object: {{"type": "image", "src": "IMAGE_URL"}}. Use the most specific and complete URL available for the image. If you cannot determine the URL, represent it as {{"type": "image", "src": null}}.
    4. Structure the output as a single JSON object containing ONLY the key "tables". The value of "tables" MUST be a list, where each element represents ONE extracted table.
    5. Each table in the list MUST be represented as a list of lists, where each inner list is a row. Each element in the row list should be either the extracted text (string) or the JSON object for an image (as described in point 3).
    6. Pay close attention to table boundaries. Do not merge unrelated content into a table.
    7. Handle merged cells as best as possible by repeating the content or using null/empty strings for spanned-over cells in subsequent rows/columns. Focus on accurate cell-by-cell content mapping to the visual layout.
    8. Ignore tables used purely for page layout if they don't contain structured data. Focus on actual data tables.
    9. Provide ONLY the JSON output, starting with `{{` and ending with `}}`. Do not include any introductory text, markdown formatting (like ```json), or explanations.

    Example of the required strict JSON output format for a table with text and one image:
    {{
      "tables": [
        [
          ["Header A", "Header B"],
          ["Data 1", {{"type": "image", "src": "https://example.com/img.png"}}],
          ["Data 2", "More Text"]
        ]
      ]
    }}

    Extract the tables now based *only* on the provided screenshot image. Ensure the output is valid JSON.
    """

    request_content = [prompt, screenshot_image]

    print("Sending request to Gemini API... (This may take a moment)")
    try:
        # Configure safety settings if needed (optional, use defaults first)
        # safety_settings=[
        #     {
        #         "category": "HARM_CATEGORY_HARASSMENT",
        #         "threshold": "BLOCK_NONE",
        #     },
        #     ... more categories
        # ]
        response = model.generate_content(
            request_content,
            stream=False,
            # safety_settings=safety_settings # Uncomment to use custom safety settings
            generation_config=genai.types.GenerationConfig(
                # candidate_count=1, # Ensure only one response candidate
                temperature=0.1 # Lower temperature for more deterministic table extraction
            )
        )

        # Robustly access response text
        response_text = ""
        if response.parts:
             response_text = response.text
        elif hasattr(response, 'text'): # Handle potential variations in response object
             response_text = response.text
        else:
            # Handle cases where the response might be blocked or empty
            print("Warning: Gemini response seems empty or was potentially blocked.")
            if hasattr(response, 'prompt_feedback'):
                 print(f"Prompt Feedback: {response.prompt_feedback}")
            if hasattr(response, 'candidates') and response.candidates:
                 print(f"Candidate Finish Reason: {response.candidates[0].finish_reason}")
                 print(f"Safety Ratings: {response.candidates[0].safety_ratings}")
            else:
                print("No candidate data available in the response.")
            return None

        # Clean the response: Gemini might still sometimes wrap the JSON or add minor text.
        # Try finding JSON block first
        print("Raw Gemini Response Text (first 500 chars):", response_text[:500]) # Debugging
        json_str = response_text.strip()
        match = re.search(r"\{.*\}", json_str, re.DOTALL) # Find first '{' to last '}'
        if match:
            json_str = match.group(0)
        else:
            print("Warning: Could not find JSON structure '{...}' in the response. Attempting direct parse.")
            # No json block found, try parsing the stripped string directly

        print("Attempting to parse JSON...")
        extracted_data = json.loads(json_str)

        if "tables" not in extracted_data or not isinstance(extracted_data["tables"], list):
            print("Error: Gemini response JSON is missing the 'tables' list or has incorrect format.")
            print("Processed JSON String:", json_str)
            return None

        print(f"Successfully parsed {len(extracted_data['tables'])} table(s) from Gemini response.")
        return extracted_data["tables"]

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from Gemini response: {e}")
        print("Attempted to parse JSON string:")
        print(json_str) # Show the string that failed parsing
        return None
    except Exception as e:
        print(f"Error interacting with Gemini API or processing response: {e}")
        # Log safety feedback if available on error
        if hasattr(response, 'prompt_feedback'):
            print(f"Prompt Feedback: {response.prompt_feedback}")
        if hasattr(response, 'candidates') and response.candidates:
            print(f"Candidate Finish Reason: {response.candidates[0].finish_reason}")
            print(f"Safety Ratings: {response.candidates[0].safety_ratings}")
        return None

# Cell 4: Main Execution Logic

def main(url):
    """Main function to orchestrate the table extraction process."""
    if not url:
        print("Error: URL must be provided.")
        return

    parsed_url = urlparse(url)
    if not all([parsed_url.scheme, parsed_url.netloc]):
        print(f"Error: Invalid URL format: {url}")
        return

    driver = None
    try:
        driver = setup_driver()
        html_content, screenshot_path = get_page_content_and_screenshot(url, driver)

        if not screenshot_path:
            print("Failed to get page content or screenshot. Exiting.")
            return

        # Display screenshot in Colab
        print("\nDisplaying captured screenshot:")
        try:
            display(IPImage(filename=screenshot_path, width=600))
        except Exception as display_err:
             print(f"Could not display screenshot: {display_err}")


        # Extract tables using Gemini
        extracted_tables_data = extract_tables_with_gemini(screenshot_path, url) # Pass html_content if needed

        if not extracted_tables_data:
            print("No tables extracted or an error occurred with Gemini. Exiting.")
            return

        print(f"\nProcessing {len(extracted_tables_data)} extracted table(s)...")
        saved_files = [] # Keep track of generated files for zipping

        for i, table_data in enumerate(extracted_tables_data):
            print(f"\n--- Processing Table {i+1} ---")
            processed_table = []
            image_found_in_table = False

            if not isinstance(table_data, list):
                 print(f"Warning: Skipping malformed table {i+1}. Expected list, got {type(table_data)}")
                 continue

            for row_idx, row in enumerate(table_data):
                processed_row = []
                if not isinstance(row, list):
                    print(f"Warning: Skipping malformed row {row_idx} in table {i+1}. Expected list, got {type(row)}")
                    continue # Skip this row

                for col_idx, cell in enumerate(row):
                    if isinstance(cell, dict) and cell.get("type") == "image":
                        img_src = cell.get("src")
                        if img_src and isinstance(img_src, str): # Check if src is a valid string
                            print(f"Found image in cell ({row_idx}, {col_idx}): {img_src}")
                            image_found_in_table = True
                            local_image_path = download_image(img_src, url)
                            # Use relative path for CSV link if possible, makes zip file more portable
                            if local_image_path:
                                csv_image_link = os.path.relpath(local_image_path, start=CSV_DIR)
                                processed_row.append(csv_image_link)
                                saved_files.append(local_image_path) # Add image to list for zipping
                            else:
                                processed_row.append("[Image Download Failed]")
                        else:
                            print(f"Warning: Image detected in cell ({row_idx}, {col_idx}) but src URL is missing or invalid: {img_src}")
                            processed_row.append("[Image URL Missing/Invalid]")
                    elif isinstance(cell, (str, int, float, bool)): # Handle basic types
                        processed_row.append(str(cell)) # Convert all to string for consistency in CSV
                    else:
                        print(f"Warning: Unexpected cell content type in cell ({row_idx}, {col_idx}): {type(cell)}. Converting to string.")
                        processed_row.append(str(cell))
                processed_table.append(processed_row)

            # Save the processed table data to CSV using Pandas
            if processed_table:
                try:
                    # Basic header detection heuristic
                    has_header = False
                    if len(processed_table) > 1:
                        # Check if first row looks like typical header data (no images, not purely numbers maybe)
                        first_row_content = "".join(map(str, processed_table[0])).lower()
                        if not any(isinstance(c, str) and ("[image" in c.lower() or "failed" in c.lower() or "missing" in c.lower()) for c in processed_table[0]) \
                           and len(first_row_content) > 0:
                            has_header = True

                    if has_header:
                        df = pd.DataFrame(processed_table[1:], columns=processed_table[0])
                    else:
                        df = pd.DataFrame(processed_table)

                    # Sanitize table name for filename
                    url_path_part = sanitize_filename(urlparse(url).path or urlparse(url).netloc)
                    csv_filename = f"{url_path_part}_table_{i+1}.csv"
                    csv_filepath = os.path.join(CSV_DIR, csv_filename)

                    df.to_csv(csv_filepath, index=False)
                    saved_files.append(csv_filepath) # Add CSV to list for zipping
                    print(f"\nTable {i+1} saved to: {csv_filepath}")
                    if image_found_in_table:
                        print(f"-> Images for this table (if downloaded) are in: {IMG_DIR}")

                    # Display first few rows in Colab
                    print(f"Preview of Table {i+1}:")
                    display(df.head())

                except Exception as e:
                    print(f"Error saving Table {i+1} to CSV or displaying preview: {e}")
            else:
                print(f"Skipping empty or malformed Table {i+1}.")

        print("\nExtraction process finished.")
        if saved_files:
            print(f"Generated files saved in: {OUTPUT_DIR}")
            return saved_files
        else:
            print("No files were generated.")
            return []

    except Exception as e:
        print(f"\nAn critical error occurred during the main process: {e}")
        import traceback
        traceback.print_exc() # Print detailed traceback for debugging
        return []
    finally:
        if driver:
            print("Closing WebDriver...")
            driver.quit()

# Cell 5: Define Target URL and Run Extraction

# --- Specify the URL you want to process ---
target_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)" # Example URL with tables & flags
# target_url = "https://www.w3schools.com/html/html_tables.asp" # Example URL with simpler tables
# target_url = "YOUR_TARGET_URL_HERE" # <--- CHANGE THIS TO YOUR DESIRED URL

if 'target_url' in locals() and target_url != "YOUR_TARGET_URL_HERE":
    # Run the main extraction function
    generated_files = main(target_url)
else:
    print("Please set the 'target_url' variable in this cell.")
    generated_files = []

