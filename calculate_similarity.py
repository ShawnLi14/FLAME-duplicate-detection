import pandas as pd

def find_conditioned_duplicates_with_join(
    coin_groups_path, coin_finds_path, same_columns, different_columns,
    extra_columns_finds, coin_finds_same_columns, output_file
):
    """
    Identifies groups of rows where the specified columns in CoinGroups.csv have identical values,
    while other columns are required to have different values. Additionally, checks that corresponding
    rows in CoinFinds.csv (joined on cfID and ID) have identical values in specified columns.
    Results are written to an output file.

    Parameters:
        coin_groups_path (str): Path to the CoinGroups.csv file.
        coin_finds_path (str): Path to the CoinFinds.csv file.
        same_columns (list of str): Columns in CoinGroups that must have identical values.
        different_columns (list of str): Columns in CoinGroups that must have different values within groups.
        extra_columns_finds (list of str): Additional columns from CoinFinds to include in the output.
        coin_finds_same_columns (list of str): Columns in CoinFinds that must be the same for duplicates.
        output_file (str): Path to the output file where results will be written.
    """
    try:
        # Read both CSV files
        coin_groups = pd.read_csv(coin_groups_path)
        coin_finds = pd.read_csv(coin_finds_path)

        # Validate that all columns in the input arrays exist, if the arrays are not empty
        all_coin_groups_columns = same_columns + different_columns
        for column in all_coin_groups_columns:
            if column not in coin_groups.columns:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"Column '{column}' does not exist in CoinGroups.csv.\n")
                return

        for column in coin_finds_same_columns + extra_columns_finds + ["ID"]:
            if column not in coin_finds.columns:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"Column '{column}' does not exist in CoinFinds.csv.\n")
                return

        # Merge CoinGroups and CoinFinds on cfID (CoinGroups) and ID (CoinFinds)
        merged = pd.merge(
            coin_groups, coin_finds, left_on="cfID", right_on="ID", how="inner"
        )

        # Handle the case where same_columns is empty (group entire dataset)
        grouped = merged.groupby(same_columns) if same_columns else [(None, merged)]
        duplicates_found = False

        with open(output_file, 'w', encoding='utf-8') as f:
            for group_key, group in grouped:
                # If different_columns is empty, skip variability check
                if different_columns:
                    unique_values_coin_groups = group[different_columns].nunique()
                    different_condition = (unique_values_coin_groups > 1).all()
                else:
                    different_condition = True

                # If coin_finds_same_columns is empty, treat it as always consistent
                if coin_finds_same_columns:
                    consistent_coin_finds = group[coin_finds_same_columns].nunique().eq(1).all()
                else:
                    consistent_coin_finds = True

                # Identify valid duplicate groups
                if different_condition and consistent_coin_finds:
                    duplicates_found = True
                    f.write(f"\nDuplicate group for values {group_key} (same columns in CoinGroups):\n")
                    # Include extra columns from CoinFinds in the output
                    selected_columns = same_columns + different_columns + extra_columns_finds + coin_finds_same_columns
                    f.write(group[selected_columns].to_string(index=False))
                    f.write("\n")

            if not duplicates_found:
                f.write("No duplicates found meeting the specified conditions.\n")

    except FileNotFoundError as e:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"File not found: {e.filename}\n")
    except Exception as e:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"An error occurred: {e}\n")

if __name__ == "__main__":
    coin_groups_path = "CoinGroups.csv"
    coin_finds_path = "CoinFinds.csv"
    same_columns = ["cg_num_coins", "cg_start_year", "cg_end_year"]  # Columns in CoinGroups that must be the same
    different_columns = ["cfID"]  # Columns in CoinGroups that must differ within groups
    extra_columns_finds = ["cf_excavation_name", "cf_start_year", "cf_end_year", "cf_num_coins_found"]  # Additional columns from CoinFinds to include in the output
    coin_finds_same_columns = ["cf_custom_x_coordinate", "cf_custom_y_coordinate"]  # Columns in CoinFinds that must be the same
    output_file = "conditioned_duplicate_groups_output.txt"  # Output file path

    find_conditioned_duplicates_with_join(
        coin_groups_path, coin_finds_path, same_columns, different_columns,
        extra_columns_finds, coin_finds_same_columns, output_file
    )
