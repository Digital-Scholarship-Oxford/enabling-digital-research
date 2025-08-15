from tqdm import tqdm
from _global_config import global_config
from processor_helpers import import_files, process_file, summarise_dataframes, save_as, save_as_xlsx

# Main function
def main():
    # Step 1: Set config variables from global_config
    auth_xml_path = global_config["auth_xml_path"]
    auth_config_path = global_config["auth_config_path"]
    auth_xml_recursive = global_config["auth_xml_recursive"]
    auth_config_recursive = global_config["auth_config_recursive"]
    coll_xml_path = global_config["coll_xml_path"]
    coll_config_path = global_config["coll_config_path"]
    coll_xml_recursive = global_config["coll_xml_recursive"]
    coll_config_recursive = global_config["coll_config_recursive"]
    auth_csv_output_dir = global_config["auth_csv_output_dir"]
    auth_json_output_dir = global_config["auth_json_output_dir"]
    auth_xlsx_output_dir = global_config["auth_xlsx_output_dir"]
    auth_output_filename = global_config["auth_output_filename"]
    coll_csv_output_dir = global_config["coll_csv_output_dir"]
    coll_json_output_dir = global_config["coll_json_output_dir"]
    coll_xlsx_output_dir = global_config["coll_xlsx_output_dir"]
    coll_output_filename = global_config["coll_output_filename"]
    separator_map = global_config["separator_map"]

    # Step 1: Import authority files
    tqdm.write("Importing authority files...")
    authority, auth_config_list, auth_df_list = import_files(
        xml_path=auth_xml_path,
        config_path=auth_config_path,
        xml_recursive=auth_xml_recursive,
        config_recursive=auth_config_recursive
    )

    # Step 2: Import collection files
    tqdm.write("Importing collection files...")
    catalogue, coll_config_list, coll_df_list = import_files(
        xml_path=coll_xml_path,
        config_path=coll_config_path,
        xml_recursive=coll_xml_recursive,
        config_recursive=coll_config_recursive
    )

    # Step 3: Extract data from the authority XML files based on the authority configuration files
    with tqdm(total=len(auth_config_list), desc="Authority progress", leave=True, position=0) as pbar:
        for config_name, config in auth_config_list.items():
            try: 
                config_name, processed_df = process_file(
                    file_type="authority",
                    config_name=config_name,
                    config=config,
                    xml_data=authority,
                    df_list=auth_df_list,
                    csv_output_dir = auth_csv_output_dir,
                    json_output_dir = auth_json_output_dir
                )
                auth_df_list[config_name] = processed_df
                pbar.update(1)
            except Exception as e:
                tqdm.write(f"Error processing authority file '{config_name}': {e}")
                continue

    # Step 4: Produce 'data_quality' tab/file for authority
    tqdm.write("Producing data quality summary for authority files...")
    try:
        data_quality_auth = summarise_dataframes(auth_df_list)
        save_as(data_quality_auth, auth_csv_output_dir, "data_quality_authority", "csv")
        save_as(data_quality_auth, auth_json_output_dir, "data_quality_authority", "json")
        auth_df_list["data_quality"] = data_quality_auth
    except Exception as e:
        tqdm.write(f"Error producing data quality summary for authority files: {e}")

    # Step 5: Save the DataFrame list to an .xlsx file with separate tabs
    save_as_xlsx(auth_df_list, auth_config_list, auth_xlsx_output_dir, auth_output_filename)

    # Step 6: Extract data from the collection XML files based on the collection configuration files
    with tqdm(total=len(coll_config_list), desc="Collection progress", leave=True, position=0) as pbar:
        for config_name, config in coll_config_list.items():
            try:
                config_name, processed_df = process_file(
                    file_type="collection",
                    config_name=config_name,
                    config=config,
                    xml_data=catalogue,
                    df_list=coll_df_list,
                    csv_output_dir=coll_csv_output_dir,
                    json_output_dir=coll_json_output_dir,
                    separator_map=separator_map,
                    lookup_df_list=auth_df_list
                )
                coll_df_list[config_name] = processed_df
                pbar.update(1)
            except Exception as e:
                tqdm.write(f"Error processing collection file '{config_name}': {e}")
                continue

    # Step 7: Produce 'data_quality' tab/file for collection
    tqdm.write("Producing data quality summary for collection files...")
    try:
        data_quality_coll = summarise_dataframes(coll_df_list)
        save_as(data_quality_coll, coll_csv_output_dir, "data_quality_collection", "csv")
        save_as(data_quality_coll, coll_json_output_dir, "data_quality_collection", "json")
        coll_df_list["data_quality"] = data_quality_coll
    except Exception as e:
        tqdm.write(f"Error producing data quality summary for collection files: {e}")

    # Step 8: Save the DataFrame list to an .xlsx file with separate tabs
    save_as_xlsx(coll_df_list, coll_config_list, coll_xlsx_output_dir, coll_output_filename)

# Run the function
if __name__ == "__main__":
    main()