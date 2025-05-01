import pandas as pd
from itertools import combinations
from tqdm import tqdm

def has_shared_substring_of_length_n(str1, str2, n=3):
    """
    Returns True if there is a shared substring of length n or more between str1 and str2.
    A simple sliding check is used here.
    """
    if not str1 or not str2:
        return False
    
    # Ensure both strings are handled consistently (e.g., you may lowercase them if needed)
    for i in range(len(str1) - n + 1):
        sub = str1[i:i+n]
        if sub in str2:
            return True
    return False

def calculate_similarity_between_pairs(
    coin_groups_path,
    coin_finds_path,
    similarity_columns_coin_groups,
    coin_finds_compare_columns,
    x_col="cf_custom_x_coordinate",
    y_col="cf_custom_y_coordinate",
    tolerance_x=0.1,
    tolerance_y=0.1,
    min_coins_sus=10,  # new parameter for minimum coins required in each CoinFind
    output_file="pairwise_similarity_output.csv"
):
    """
    Efficiently calculates percent similarity between pairs of suspicious CoinFinds based on CoinGroups attribution,
    allowing for a coordinate tolerance when identifying suspicious groups.

    Additional conditions:
      - Both CoinFinds must have at least 'min_coins_sus' coins (cf_num_coins_found).
      - There must be a shared substring of length 3 or more in cf_publication_ref.
    """
    try:
        # Read both CSV files
        coin_groups = pd.read_csv(coin_groups_path)
        coin_finds = pd.read_csv(coin_finds_path)

        # Ensure necessary columns exist in CoinGroups
        required_coin_groups_columns = ["cfID"] + similarity_columns_coin_groups
        for column in required_coin_groups_columns:
            if column not in coin_groups.columns:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"Column '{column}' does not exist in CoinGroups.csv.\n")
                return

        # Ensure necessary columns exist in CoinFinds
        # Note: cf_publication_ref is required for the substring check
        required_coin_finds_columns = [
            "ID", 
            "cf_num_coins_found", 
            x_col, 
            y_col, 
            "cf_publication_ref"
        ] + coin_finds_compare_columns

        for column in required_coin_finds_columns:
            if column not in coin_finds.columns:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"Column '{column}' does not exist in CoinFinds.csv.\n")
                return

        # Exclude rows where 'cf_num_coins_found' is 0
        coin_finds = coin_finds[coin_finds["cf_num_coins_found"] > 0]

        # Identify CoinFinds that have associated CoinGroups
        valid_coinfind_ids = coin_groups["cfID"].unique()
        coin_finds = coin_finds[coin_finds["ID"].isin(valid_coinfind_ids)]

        if coin_finds.empty:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("No CoinFinds remaining after filtering and matching with CoinGroups.\n")
            return

        # Create binning columns for coordinate grouping
        coin_finds["bin_x"] = (coin_finds[x_col] / tolerance_x).apply(lambda x: int(x))
        coin_finds["bin_y"] = (coin_finds[y_col] / tolerance_y).apply(lambda x: int(x))

        # Grouping columns
        grouping_columns = coin_finds_compare_columns + ["bin_x", "bin_y"]

        # Identify suspicious CoinFinds by grouping (more than one find in the same group)
        suspicious_groups = coin_finds.groupby(grouping_columns).filter(lambda g: len(g) > 1)

        if suspicious_groups.empty:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("No suspicious CoinFinds identified for comparison.\n")
            return

        # Attribute CoinGroups rows to each CoinFind by ID
        coin_groups_by_find = coin_groups.groupby("cfID")

        def get_attributed_groups(find_id):
            if find_id in coin_groups_by_find.groups:
                return coin_groups_by_find.get_group(find_id)
            else:
                return pd.DataFrame(columns=similarity_columns_coin_groups)

        suspicious_groups["AttributedGroups"] = suspicious_groups["ID"].map(get_attributed_groups)

        # Generate all unique pairs of suspicious CoinFinds within each bin group
        all_pairs_indices = []
        for _, subdf in suspicious_groups.groupby(grouping_columns):
            indices = subdf.index.tolist()
            if len(indices) > 1:
                all_pairs_indices.extend(list(combinations(indices, 2)))

        results = []
        total_pairs = len(all_pairs_indices)

        with tqdm(total=total_pairs, desc="Processing CoinFind pairs") as pbar:
            for idx1, idx2 in all_pairs_indices:
                find1 = suspicious_groups.loc[idx1]
                find2 = suspicious_groups.loc[idx2]

                # Final tolerance check
                dx = abs(find1[x_col] - find2[x_col])
                dy = abs(find1[y_col] - find2[y_col])
                if (dx > tolerance_x) or (dy > tolerance_y):
                    pbar.update(1)
                    continue

                # Check minimum coins requirement
                if find1["cf_num_coins_found"] < min_coins_sus or find2["cf_num_coins_found"] < min_coins_sus:
                    pbar.update(1)
                    continue

                # Check if cf_publication_ref has a shared substring of length >= 3
                ref1 = str(find1.get("cf_publication_ref", ""))
                ref2 = str(find2.get("cf_publication_ref", ""))
                if not has_shared_substring_of_length_n(ref1, ref2, n=3):
                    pbar.update(1)
                    continue

                # Ensure exact match columns are equal
                if not all(find1[col] == find2[col] for col in coin_finds_compare_columns):
                    pbar.update(1)
                    continue

                # Retrieve attributed CoinGroups
                groups1 = find1["AttributedGroups"]
                groups2 = find2["AttributedGroups"]

                if groups1.empty or groups2.empty:
                    pbar.update(1)
                    continue

                # Keep only similarity columns and remove duplicates
                groups1 = groups1[similarity_columns_coin_groups].drop_duplicates()
                groups2 = groups2[similarity_columns_coin_groups].drop_duplicates()

                # Calculate similarity
                merged = pd.merge(groups1, groups2, how="inner")
                total_unique_rows = len(groups1) + len(groups2) - len(merged)
                similarity_percentage = (len(merged) / total_unique_rows) * 100 if total_unique_rows > 0 else 0

                if similarity_percentage > 0:
                    results.append((find1["ID"], find2["ID"], similarity_percentage))

                pbar.update(1)

        # Sort results by similarity percentage
        sorted_results = sorted(results, key=lambda x: x[2], reverse=True)

        # Write results to the output file (overwrite mode)
        with open(output_file, 'w', encoding='utf-8') as f:
            if sorted_results:
                f.write("FindID1,FindID2,Similarity (%)\n")
                for find_id1, find_id2, similarity in sorted_results:
                    f.write(f"{find_id1},{find_id2},{similarity:.2f}\n")
            else:
                f.write("No pairs with non-zero similarity to compare.\n")

        print(f"Results written to {output_file}")

    except FileNotFoundError as e:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"File not found: {e.filename}\n")
    except Exception as e:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"An error occurred: {e}\n")


if __name__ == "__main__":
    coin_groups_path = "CoinGroups.csv"
    coin_finds_path = "CoinFinds.csv"
    
    similarity_columns_coin_groups = ["cg_num_coins", "cg_start_year", "cg_end_year", "DenominationName", "Mint"]
    
    coin_finds_compare_columns = []
    
    # Example usage with min_coins_sus=10
    calculate_similarity_between_pairs(
        coin_groups_path=coin_groups_path,
        coin_finds_path=coin_finds_path,
        similarity_columns_coin_groups=similarity_columns_coin_groups,
        coin_finds_compare_columns=coin_finds_compare_columns,
        x_col="cf_custom_x_coordinate",
        y_col="cf_custom_y_coordinate",
        tolerance_x=0.1,
        tolerance_y=0.1,
        min_coins_sus=10,  # You can change this threshold as needed
        output_file="pairwise_similarity_output.csv"
    )
