import pandas as pd

def find_exact_duplicates(file_path, column_names, extra_columns, output_file):
    """
    Identifies rows where all specified columns have identical values and writes each group of duplicates
    to an output file, including additional columns for context.

    Parameters:
        file_path (str): Path to the CSV file.
        column_names (list of str): List of column names to check for exact duplicates.
        extra_columns (list of str): Additional columns to include in the output.
        output_file (str): Path to the output file where results will be written.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Combine columns of interest (grouping + extras)
        all_columns = column_names + extra_columns

        # Check if all columns exist in the DataFrame
        for column in all_columns:
            if column not in df.columns:
                with open(output_file, 'w') as f:
                    f.write(f"Column '{column}' does not exist in the file.\n")
                return

        # Group rows by the specified columns and filter groups with more than one occurrence
        grouped = df.groupby(column_names)
        duplicates_found = False

        with open(output_file, 'w') as f:
            for group_key, group in grouped:
                if len(group) > 1:  # Only consider groups with more than one occurrence
                    duplicates_found = True
                    f.write(f"\nDuplicate group for values {group_key}:\n")
                    f.write(group[all_columns].to_string(index=False))
                    f.write("\n")

            if not duplicates_found:
                f.write("No exact duplicates found based on the specified columns.\n")

    except FileNotFoundError:
        with open(output_file, 'w') as f:
            f.write(f"File not found: {file_path}\n")
    except Exception as e:
        with open(output_file, 'w') as f:
            f.write(f"An error occurred: {e}\n")

if __name__ == "__main__":
    file_path = "CoinFinds.csv"
    column_names = ["cf_custom_x_coordinate", "cf_custom_y_coordinate", "cf_start_year", "cf_end_year", "cf_num_coins_found"]
    extra_columns = ["ID", "cf_excavation_name"]  # Additional columns to include in the output
    output_file = "duplicate_groups_output.txt"  # Output file path
    find_exact_duplicates(file_path, column_names, extra_columns, output_file)
