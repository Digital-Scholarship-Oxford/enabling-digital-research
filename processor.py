import os
import pandas as pd
from tqdm import tqdm
from lxml import etree
import elementpath
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

start_time = time.time()

# Function to apply XPath 2.0 queries to an XML element in the TEI namespace
@lru_cache(maxsize=128)  # Cache results of XPath expressions
def extract_with_xpath(xml_element, xpath_expr):
    try:
        # Use elementpath for XPath 2.0 with lxml element
        result = elementpath.select(xml_element, xpath_expr, namespaces={'tei': 'http://www.tei-c.org/ns/1.0'})

        # Ensure the result is always a list
        if isinstance(result, bool):
            result = [result]  # Wrap the boolean in a list
        elif not isinstance(result, list):
            result = [result]  # Wrap single values in a list
        return result
    except Exception as e:
        print(f"XPath extraction failed. Offending XPath: {xpath_expr}. Error: {e}")
        return []

# Function to read XML files from a directory
def read_xml_files(directory, pattern=".xml"):
    xml_files = []
    try:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(pattern):
                    xml_files.append(os.path.join(root, file))
        return xml_files
    except Exception as e:
        print(f"Reading XML files in {directory} failed. Error: {e}")
        return []

# Function to parse an XML file and return its root element
@lru_cache(maxsize=128)  # Cache parsed XML trees
def parse_xml(file):
    try:
        tree = etree.parse(file)  # Parse with lxml
        root = tree.getroot()
        return root
    except Exception as e:
        print(f"Parsing {file} failed. Error: {e}")
        return None

# Function to process a single file's XPath extraction with progress updates
def process_file(config_name, config, df, auth_files, xpaths, headings, progress_bar):
    for xpath, heading, auth_file in zip(xpaths, headings, auth_files):
        auth_xml = authority.get(auth_file)
        df[heading] = extract_with_xpath(auth_xml, xpath)
        progress_bar.update(1)  # Update progress for each XPath processed
    return df

# Step 1: Read and parse XML authority files
authority_files = read_xml_files("authority")
authority = {}
for file in tqdm(authority_files, desc="Parsing authority files"):
    filename = os.path.splitext(os.path.basename(file))[0]
    authority[filename] = parse_xml(file)

# Step 2: Read and parse XML catalogue files
catalogue_files = read_xml_files("collections")
catalogue = {}
for file in tqdm(catalogue_files, desc="Parsing catalogue files"):
    filename = os.path.splitext(os.path.basename(file))[0]
    catalogue[filename] = parse_xml(file)

# Step 3: Read and parse CSV authority configuration files
auth_config_files = sorted(read_xml_files("config/auth", pattern=".csv"))
auth_config_list = {}
for file in tqdm(auth_config_files, desc="Parsing authority config files"):
    name = os.path.splitext(os.path.basename(file))[0]
    auth_config_list[name] = pd.read_csv(file, dtype=str)

# Step 4: Read and parse CSV collection configuration files
coll_config_files = sorted(read_xml_files("config/collection", pattern=".csv"))
coll_config_list = {}
for file in tqdm(coll_config_files, desc="Parsing collection config files"):
    name = os.path.splitext(os.path.basename(file))[0]
    coll_config_list[name] = pd.read_csv(file, dtype=str)

# Step 5: Create an empty DataFrame for each authority configuration file
auth_df_list = {
    name: pd.DataFrame(columns=config['heading'].tolist())
    for name, config in auth_config_list.items()
}

# Step 6: Create an empty DataFrame for each collection configuration file
coll_df_list = {
    name: pd.DataFrame(columns=config['heading'].tolist())
    for name, config in coll_config_list.items()
}

# Step 7: Extract data from the authority XML files based on the authority configuration files
with ThreadPoolExecutor() as executor:
    futures = []
    total_xpaths = sum(len(config['xpath']) for config in auth_config_list.values())  # Total number of XPath tasks
    with tqdm(total=total_xpaths, desc="Processing XPath extractions", dynamic_ncols=True) as progress_bar:
        for config_name, config in tqdm(auth_config_list.items(), desc="Authority progress"):
            df = auth_df_list[config_name]

            # Step 7.1 Extract relevant columns from the configuration file
            sections, headings, auth_files, xpaths = (
                config[col].tolist() for col in ["section", "heading", "auth_file", "xpath"]
            )

            # Step 7.2: Process each XPath expression in parallel
            futures.append(executor.submit(process_file, config_name, config, df, auth_files, xpaths, headings, progress_bar))

        # Wait for all futures to complete
        for future in as_completed(futures):
            future.result()  # This will ensure we process the DataFrame once each task is done

# # Step 7.3: Save the output to CSV files
# for config_name, df in auth_df_list.items():
#     output_file = f"output_{config_name}.csv"
#     df.to_csv(output_file, index=False)
#     print(f"Saved {output_file}")
#
# # Step 8: Optionally save collection DataFrames (if needed)
# for config_name, df in coll_df_list.items():
#     output_file = f"output_collection_{config_name}.csv"
#     df.to_csv(output_file, index=False)
#     print(f"Saved {output_file}")

end_time = time.time()
execution_time = end_time - start_time
print(f"Total execution time: {execution_time} seconds")
