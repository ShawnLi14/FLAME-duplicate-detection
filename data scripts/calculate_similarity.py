import pandas as pd
from itertools import combinations
from tqdm import tqdm


def calculate_similarity_between_pairs(
    coin_groups_path, coin_finds_path, similarity_columns_coin_groups,
    coin_finds_compare_columns, output_file
):
    """
    Efficiently calculates percent similarity between pairs of suspicious CoinFinds based on CoinGroups attribution,
    ensuring that output pairs share the same values for specified comparison columns.
    Excludes CoinFinds where 'cf_num_coins_found' is 0 or where no associated CoinGroups exist.

    Parameters:
        coin_groups_path (str): Path to the CoinGroups.csv file.
        coin_finds_path (str): Path to the CoinFinds.csv file.
        similarity_columns_coin_groups (list of str): Columns in CoinGroups to be used for similarity calculation.
        coin_finds_compare_columns (list of str): Columns in CoinFinds to identify suspicious duplicates.
        output_file (str): Path to the output file where results will be written.
    """
    try:
        # Read both CSV files
        coin_groups = pd.read_csv(coin_groups_path)
        coin_finds = pd.read_csv(coin_finds_path)

        # Ensure necessary columns exist
        for column in ["cfID"] + similarity_columns_coin_groups:
            if column not in coin_groups.columns:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"Column '{column}' does not exist in CoinGroups.csv.\n")
                return

        for column in ["ID", "cf_num_coins_found"] + coin_finds_compare_columns:
            if column not in coin_finds.columns:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"Column '{column}' does not exist in CoinFinds.csv.\n")
                return

        # Exclude rows where 'cf_num_coins_found' is 0
        coin_finds = coin_finds[coin_finds["cf_num_coins_found"] > 0]

        # Identify CoinFinds that have associated CoinGroups
        valid_coinfind_ids = coin_groups["cfID"].unique()
        coin_finds = coin_finds[coin_finds["ID"].isin(valid_coinfind_ids)]

        # Identify suspicious CoinFinds based on comparison columns
        coin_finds["CompareKey"] = coin_finds[coin_finds_compare_columns].astype(str).agg("_".join, axis=1)
        suspicious_groups = coin_finds.groupby("CompareKey").filter(lambda x: len(x) > 1)

        if suspicious_groups.empty:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("No suspicious CoinFinds identified for comparison.\n")
            return

        # Attribute CoinGroups rows to each suspicious CoinFind by ID
        coin_groups_by_find = coin_groups.groupby("cfID")
        suspicious_groups["AttributedGroups"] = suspicious_groups["ID"].map(
            lambda find_id: coin_groups_by_find.get_group(find_id)
            if find_id in coin_groups_by_find.groups
            else pd.DataFrame(columns=similarity_columns_coin_groups)
        )

        # Generate all unique pairs of suspicious CoinFinds
        pairs = combinations(suspicious_groups.index, 2)
        results = []

        # Progress bar for comparison
        total_pairs = len(suspicious_groups) * (len(suspicious_groups) - 1) // 2
        with tqdm(total=total_pairs, desc="Processing CoinFind pairs") as pbar:
            for idx1, idx2 in pairs:
                find1 = suspicious_groups.loc[idx1]
                find2 = suspicious_groups.loc[idx2]

                # Ensure the pair has the same comparison column values
                if not all(find1[col] == find2[col] for col in coin_finds_compare_columns):
                    pbar.update(1)
                    continue

                # Retrieve attributed CoinGroups for each CoinFind
                groups1 = find1["AttributedGroups"]
                groups2 = find2["AttributedGroups"]

                # Skip if either CoinFind has no attributed groups
                if groups1.empty or groups2.empty:
                    pbar.update(1)
                    continue

                groups1 = groups1[similarity_columns_coin_groups].drop_duplicates()
                groups2 = groups2[similarity_columns_coin_groups].drop_duplicates()

                # Compare CoinGroups rows and calculate similarity
                merged = pd.merge(
                    groups1,
                    groups2,
                    how="inner"
                )
                total_unique_rows = len(groups1) + len(groups2) - len(merged)
                similarity_percentage = (len(merged) / total_unique_rows) * 100 if total_unique_rows > 0 else 0

                if similarity_percentage > 0:  # Only output non-zero similarity
                    results.append((find1["ID"], find2["ID"], similarity_percentage))

                pbar.update(1)

        # Sort results by similarity percentage (highest to lowest)
        sorted_results = sorted(results, key=lambda x: x[2], reverse=True)

        # Write results to the output file
        with open(output_file, 'w', encoding='utf-8') as f:
            if sorted_results:
                f.write("FindID1,FindID2,Similarity (%)\n")
                for find_id1, find_id2, similarity in sorted_results:
                    f.write(f"{find_id1},{find_id2},{similarity:.2f}\n")
            else:
                f.write("No pairs with non-zero similarity to compare.\n")

    except FileNotFoundError as e:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"File not found: {e.filename}\n")
    except Exception as e:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"An error occurred: {e}\n")


if __name__ == "__main__":
    coin_groups_path = "CoinGroups.csv"
    coin_finds_path = "CoinFinds.csv"
    similarity_columns_coin_groups = ["cg_num_coins", "cg_start_year", "cg_end_year"]  # Columns in CoinGroups
    coin_finds_compare_columns = ["cf_custom_x_coordinate", "cf_custom_y_coordinate"]  # Columns in CoinFinds
    output_file = "pairwise_similarity_output.csv"  # Output file path

    calculate_similarity_between_pairs(
        coin_groups_path, coin_finds_path, similarity_columns_coin_groups,
        coin_finds_compare_columns, output_file
    )
