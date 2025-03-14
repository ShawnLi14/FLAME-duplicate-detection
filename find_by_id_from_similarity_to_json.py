import pandas as pd
import json
import math

def replace_nan(obj):
    """
    Recursively replace NaN with None in a nested data structure.
    
    Parameters:
        obj: The data structure to process (dict, list, etc.).
    
    Returns:
        The processed data structure with NaN replaced by None.
    """
    if isinstance(obj, dict):
        return {k: replace_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan(item) for item in obj]
    elif isinstance(obj, float):
        return None if math.isnan(obj) else obj
    else:
        return obj

def find_by_id_from_similarity_csv(file_path, similarity_output_path, output_file):
    """
    Finds entries in the CSV file corresponding to each pair of IDs from the similarity output,
    writes results to a JSON file, and includes similarity percentages.

    Parameters:
        file_path (str): Path to the CoinFinds.csv file.
        similarity_output_path (str): Path to the pairwise similarity output file.
        output_file (str): Path to the output JSON file where results will be written.
    """
    try:
        # Read the similarity output file with utf-8 encoding
        try:
            similarity_df = pd.read_csv(similarity_output_path, encoding="utf-8")
            required_columns = {"FindID1", "FindID2", "Similarity (%)"}
            if not required_columns.issubset(similarity_df.columns):
                error_message = "The similarity output file does not have the required columns."
                print(error_message)
                with open(output_file, 'w', encoding="utf-8") as f:
                    json.dump({"error": error_message}, f, indent=4)
                return
        except FileNotFoundError:
            error_message = f"Similarity output file not found: {similarity_output_path}"
            print(error_message)
            with open(output_file, 'w', encoding="utf-8") as f:
                json.dump({"error": error_message}, f, indent=4)
            return

        # Read the CoinFinds.csv file with utf-8 encoding
        df = pd.read_csv(file_path, encoding="utf-8")

        # Check if the "ID" column exists
        if "ID" not in df.columns:
            error_message = "The 'ID' column does not exist in the CoinFinds.csv file."
            print(error_message)
            with open(output_file, 'w', encoding="utf-8") as f:
                json.dump({"error": error_message}, f, indent=4)
            return

        results = []

        # Process each pair of FindID1 and FindID2
        for _, row in similarity_df.iterrows():
            find_id1 = row.get("FindID1")
            find_id2 = row.get("FindID2")
            similarity_percentage = row.get("Similarity (%)")

            # Convert to appropriate types and handle NaN
            try:
                find_id1 = int(find_id1)
            except (ValueError, TypeError):
                find_id1 = None

            try:
                find_id2 = int(find_id2)
            except (ValueError, TypeError):
                find_id2 = None

            try:
                similarity_percentage = float(similarity_percentage)
                if math.isnan(similarity_percentage):
                    similarity_percentage = None
            except (ValueError, TypeError):
                similarity_percentage = None

            # Filter rows for the current pair
            if find_id1 is not None:
                matching_rows1 = df[df["ID"] == find_id1].to_dict(orient='records')
            else:
                matching_rows1 = None

            if find_id2 is not None:
                matching_rows2 = df[df["ID"] == find_id2].to_dict(orient='records')
            else:
                matching_rows2 = None

            # Create the manual verification link for the pair
            if find_id1 is not None and find_id2 is not None:
                manual_verification_link = f"http://csla100w.princeton.edu:82/?FindFocusIDs={find_id1},{find_id2}"
            else:
                manual_verification_link = None

            result = {
                "FindID1": find_id1,
                "FindID2": find_id2,
                "Similarity": similarity_percentage,
                "Entry1": matching_rows1 if matching_rows1 else None,
                "Entry2": matching_rows2 if matching_rows2 else None,
                "ManualVerificationLink": manual_verification_link
            }

            results.append(result)

        # Replace NaN with None recursively
        cleaned_results = replace_nan(results)

        # Write the results to a JSON file
        with open(output_file, 'w', encoding="utf-8") as f:
            json.dump(cleaned_results, f, indent=4)

        print(f"Results written to {output_file}")

    except FileNotFoundError:
        error_message = f"File not found: {file_path}"
        print(error_message)
        with open(output_file, 'w', encoding="utf-8") as f:
            json.dump({"error": error_message}, f, indent=4)
    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        with open(output_file, 'w', encoding="utf-8") as f:
            json.dump({"error": error_message}, f, indent=4)


if __name__ == "__main__":
    file_path = "CoinFinds.csv"  # Path to CoinFinds.csv
    similarity_output_path = "pairwise_similarity_output.csv"  # Path to similarity output file
    output_file = "id_lookup_output.json"  # Output JSON file path

    find_by_id_from_similarity_csv(file_path, similarity_output_path, output_file)
