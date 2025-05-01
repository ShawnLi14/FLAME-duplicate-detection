import pandas as pd

def find_entries_by_id(file_path, id_values, output_file):
    """
    Finds entries in the CSV file corresponding to the given list of IDs and writes the results to a file,
    including a link for manual verification.

    Parameters:
        file_path (str): Path to the CSV file.
        id_values (list of int): List of ID values to look up in the table.
        output_file (str): Path to the output file where results will be written.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Check if the "ID" column exists
        if "ID" not in df.columns:
            with open(output_file, 'w') as f:
                f.write("The 'ID' column does not exist in the file.\n")
            print("The 'ID' column does not exist in the file.")
            return

        # Filter rows based on the given IDs
        matching_rows = df[df["ID"].isin(id_values)]

        # Create the manual verification link
        ids_as_string = ",".join(map(str, id_values))
        manual_verification_link = f"http://csla100w.princeton.edu:82/?FindFocusIDs={ids_as_string}"

        with open(output_file, 'w') as f:
            if matching_rows.empty:
                f.write("No matching entries found for the provided IDs.\n")
                f.write(f"Manual verification link: {manual_verification_link}\n")
                print("No matching entries found for the provided IDs.")
            else:
                f.write("Matching entries:\n")
                f.write(matching_rows.to_string(index=False))
                f.write("\n\n")
                f.write(f"Manual verification link: {manual_verification_link}\n")
                print("Matching entries:")
                print(matching_rows)

            print(f"Manual verification link: {manual_verification_link}")

    except FileNotFoundError:
        with open(output_file, 'w') as f:
            f.write(f"File not found: {file_path}\n")
        print(f"File not found: {file_path}")
    except Exception as e:
        with open(output_file, 'w') as f:
            f.write(f"An error occurred: {e}\n")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    file_path = "CoinFinds.csv"
    output_file = "id_lookup_output.txt"  # Output file path

    # Prompt the user to enter a list of IDs
    try:
        id_input = input("Enter the ID values separated by commas: ")
        id_values = [int(id.strip()) for id in id_input.split(",")]
        find_entries_by_id(file_path, id_values, output_file)
    except ValueError:
        print("Invalid input. Please enter a list of numerical IDs separated by commas.")
