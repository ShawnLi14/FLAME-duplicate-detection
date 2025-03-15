import pandas as pd
import json
import math
from itertools import combinations
from tqdm import tqdm
import firebase_admin
from firebase_admin import credentials, firestore

SERVICE_ACCOUNT_KEY_PATH = "C:\\Users\\shamb\\Downloads\\flame-duplicates-firebase-adminsdk-fbsvc-e825b279bf.json"

def initialize_firestore(service_account_key_path):
    """
    Initializes and returns a Firestore client.
    """
    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account_key_path)
        firebase_admin.initialize_app(cred)
    return firestore.client()

def export_to_firestore(data, collection_name="find_results"):
    """
    Exports a list of dictionaries to a Firestore collection.
    
    Parameters:
        data (list): List of dictionaries to export.
        collection_name (str): Name of the Firestore collection.
    """
    db = initialize_firestore(SERVICE_ACCOUNT_KEY_PATH)
    collection_ref = db.collection(collection_name)
    
    # Add each record as a document in the Firestore collection.
    for record in data:
        doc_ref = collection_ref.document()  # Auto-generates a unique document ID.
        doc_ref.set(record)
    print(f"Exported {len(data)} records to Firestore collection '{collection_name}'.")

def replace_nan(obj):
    """
    Recursively replace NaN with None in a nested data structure.
    """
    if isinstance(obj, dict):
        return {k: replace_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan(item) for item in obj]
    elif isinstance(obj, float):
        return None if math.isnan(obj) else obj
    else:
        return obj

def levenshtein_distance(s1, s2):
    """
    Compute the Levenshtein distance between two strings.
    """
    if s1 is None: s1 = ""
    if s2 is None: s2 = ""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    # Now, len(s1) >= len(s2)
    if len(s2) == 0:
        return len(s1)
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    return previous_row[-1]

def find_by_id_from_similarity_csv(file_path, similarity_output_path, output_file, coin_groups_path):
    """
    Finds entries in the CSV file corresponding to each pair of IDs from the similarity output,
    attaches the coin groups data for each coinfind, and writes the results to a JSON file.
    
    Returns:
        The cleaned lookup results as a list of dictionaries.
    """
    try:
        # Read the similarity output file.
        try:
            similarity_df = pd.read_csv(similarity_output_path, encoding="utf-8")
            required_columns = {"FindID1", "FindID2", "Similarity (%)"}
            if not required_columns.issubset(similarity_df.columns):
                error_message = "The similarity output file does not have the required columns."
                print(error_message)
                with open(output_file, 'w', encoding="utf-8") as f:
                    json.dump({"error": error_message}, f, indent=4)
                return None
        except FileNotFoundError:
            error_message = f"Similarity output file not found: {similarity_output_path}"
            print(error_message)
            with open(output_file, 'w', encoding="utf-8") as f:
                json.dump({"error": error_message}, f, indent=4)
            return None

        # Read the CoinFinds.csv file.
        df = pd.read_csv(file_path, encoding="utf-8")
        if "ID" not in df.columns:
            error_message = "The 'ID' column does not exist in the CoinFinds.csv file."
            print(error_message)
            with open(output_file, 'w', encoding="utf-8") as f:
                json.dump({"error": error_message}, f, indent=4)
            return None

        # Read the CoinGroups.csv file.
        coin_groups = pd.read_csv(coin_groups_path, encoding="utf-8")
        results = []
        # Process each pair.
        for _, row in similarity_df.iterrows():
            find_id1 = row.get("FindID1")
            find_id2 = row.get("FindID2")
            similarity_percentage = row.get("Similarity (%)")
            tags = row.get("Tags") if "Tags" in row else ""
            
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

            # Convert tags from semicolon-separated string to list (if present)
            if tags and isinstance(tags, str):
                tag_list = [tag.strip() for tag in tags.split(";") if tag.strip()]
            else:
                tag_list = []
                
            if find_id1 is not None:
                matching_rows1 = df[df["ID"] == find_id1].to_dict(orient='records')
                coin_groups1 = coin_groups[coin_groups["cfID"] == find_id1].to_dict(orient='records')
            else:
                matching_rows1 = None
                coin_groups1 = None

            if find_id2 is not None:
                matching_rows2 = df[df["ID"] == find_id2].to_dict(orient='records')
                coin_groups2 = coin_groups[coin_groups["cfID"] == find_id2].to_dict(orient='records')
            else:
                matching_rows2 = None
                coin_groups2 = None

            if find_id1 is not None and find_id2 is not None:
                manual_verification_link = f"http://csla100w.princeton.edu:82/?FindFocusIDs={find_id1},{find_id2}"
            else:
                manual_verification_link = None

            result = {
                "FindID1": find_id1,
                "FindID2": find_id2,
                "Similarity": similarity_percentage,
                "Tags": tag_list,
                "Entry1": matching_rows1 if matching_rows1 else None,
                "Entry2": matching_rows2 if matching_rows2 else None,
                "CoinGroups1": coin_groups1 if coin_groups1 else None,
                "CoinGroups2": coin_groups2 if coin_groups2 else None,
                "ManualVerificationLink": manual_verification_link
            }
            results.append(result)

        cleaned_results = replace_nan(results)
        with open(output_file, 'w', encoding="utf-8") as f:
            json.dump(cleaned_results, f, indent=4)

        print(f"ID lookup results written to {output_file}")
        return cleaned_results

    except FileNotFoundError:
        error_message = f"File not found: {file_path}"
        print(error_message)
        with open(output_file, 'w', encoding="utf-8") as f:
            json.dump({"error": error_message}, f, indent=4)
        return None
    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        with open(output_file, 'w', encoding="utf-8") as f:
            json.dump({"error": error_message}, f, indent=4)
        return None

def has_shared_substring_of_length_n(str1, str2, n=3):
    """
    Returns True if there is a shared substring of length n or more between str1 and str2.
    """
    if not str1 or not str2:
        return False
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
    min_coins_sus=5,
    output_file="pairwise_similarity_output.csv"
):
    """
    Calculates percent similarity between pairs of suspicious CoinFinds based on CoinGroups attribution,
    weighting by the number of coins in each coin group (using the "cg_num_coins" parameter)
    and adjusting the contribution of each similar coingroup by comparing the coin counts.
    Also computes "tags" for each pair based on additional criteria:
      - "Highly Similar": when the cf_name differ by less than 3 characters (Levenshtein distance)
        or the IDs differ by less than 5.
      - "Mixed Source": when one entry's cf_user is "CHRE" and the other's is "PAS UK Finds".
    Writes the similarity results (including tags) to a CSV file.
    """
    try:
        from itertools import combinations
        from tqdm import tqdm

        # Read the input CSV files.
        coin_groups = pd.read_csv(coin_groups_path)
        coin_finds = pd.read_csv(coin_finds_path)

        # Validate required columns.
        required_coin_groups_columns = ["cfID"] + similarity_columns_coin_groups + ["cg_num_coins"]
        for column in required_coin_groups_columns:
            if column not in coin_groups.columns:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"Column '{column}' does not exist in CoinGroups.csv.\n")
                return

        required_coin_finds_columns = [
            "ID", 
            "cf_num_coins_found", 
            x_col, 
            y_col, 
            "cf_publication_ref"
        ] + coin_finds_compare_columns
        for extra in ["cf_name", "cf_user"]:
            if extra not in coin_finds.columns:
                required_coin_finds_columns.append(extra)
        for column in required_coin_finds_columns:
            if column not in coin_finds.columns:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"Column '{column}' does not exist in CoinFinds.csv.\n")
                return

        coin_finds = coin_finds[coin_finds["cf_num_coins_found"] > 0]
        valid_coinfind_ids = coin_groups["cfID"].unique()
        coin_finds = coin_finds[coin_finds["ID"].isin(valid_coinfind_ids)]

        if coin_finds.empty:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("No CoinFinds remaining after filtering and matching with CoinGroups.\n")
            return

        # Create binning columns for grouping based on coordinates.
        coin_finds["bin_x"] = (coin_finds[x_col] / tolerance_x).apply(lambda x: int(x))
        coin_finds["bin_y"] = (coin_finds[y_col] / tolerance_y).apply(lambda x: int(x))
        grouping_columns = coin_finds_compare_columns + ["bin_x", "bin_y"]

        suspicious_groups = coin_finds.groupby(grouping_columns).filter(lambda g: len(g) > 1)
        if suspicious_groups.empty:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("No suspicious CoinFinds identified for comparison.\n")
            return

        # Map CoinGroups to each CoinFind.
        coin_groups_by_find = coin_groups.groupby("cfID")
        def get_attributed_groups(find_id):
            if find_id in coin_groups_by_find.groups:
                return coin_groups_by_find.get_group(find_id)
            else:
                return pd.DataFrame(columns=similarity_columns_coin_groups + ["cg_num_coins"])
        suspicious_groups["AttributedGroups"] = suspicious_groups["ID"].map(get_attributed_groups)

        # Generate all unique pairs within each bin group.
        all_pairs_indices = []
        for _, subdf in suspicious_groups.groupby(grouping_columns):
            indices = subdf.index.tolist()
            if len(indices) > 1:
                all_pairs_indices.extend(list(combinations(indices, 2)))

        results = []
        total_pairs = len(all_pairs_indices)
        with tqdm(total=total_pairs, desc="Processing CoinFind pairs") as pbar:
            for idx1, idx2 in all_pairs_indices:
                try:
                    find1 = suspicious_groups.loc[idx1]
                    find2 = suspicious_groups.loc[idx2]

                    dx = abs(find1[x_col] - find2[x_col])
                    dy = abs(find1[y_col] - find2[y_col])
                    if (dx > tolerance_x) or (dy > tolerance_y):
                        pbar.update(1)
                        continue

                    if find1["cf_num_coins_found"] < min_coins_sus or find2["cf_num_coins_found"] < min_coins_sus:
                        pbar.update(1)
                        continue

                    ref1 = str(find1.get("cf_publication_ref", ""))
                    ref2 = str(find2.get("cf_publication_ref", ""))
                    if not has_shared_substring_of_length_n(ref1, ref2, n=3):
                        pbar.update(1)
                        continue

                    if not all(find1[col] == find2[col] for col in coin_finds_compare_columns):
                        pbar.update(1)
                        continue
                    groups1 = find1["AttributedGroups"]
                    groups2 = find2["AttributedGroups"]
                    if groups1.empty or groups2.empty:
                        pbar.update(1)
                        continue

                    # Aggregate groups by the similarity columns and sum the coin counts.
                    groups1_weighted = groups1.groupby(similarity_columns_coin_groups, as_index=False).agg({"cg_num_coins": "sum"})
                    groups2_weighted = groups2.groupby(similarity_columns_coin_groups, as_index=False).agg({"cg_num_coins": "sum"})

                    total_weight1 = groups1_weighted["cg_num_coins"].sum()
                    total_weight2 = groups2_weighted["cg_num_coins"].sum()

                    merged = pd.merge(
                        groups1_weighted, groups2_weighted, how="inner", 
                        on=similarity_columns_coin_groups, suffixes=('_1', '_2')
                    )
                    # Compute per-group similarity factor:
                    merged["group_similarity_factor"] = merged.apply(
                        lambda row: (min(row["cg_num_coins_1"], row["cg_num_coins_2"]) / max(row["cg_num_coins_1"], row["cg_num_coins_2"])) 
                                    if max(row["cg_num_coins_1"], row["cg_num_coins_2"]) != 0 else 1, axis=1
                    )
                    merged["effective_intersection"] = merged.apply(
                        lambda row: min(row["cg_num_coins_1"], row["cg_num_coins_2"]) * row["group_similarity_factor"], axis=1
                    )
                    effective_intersection_weight = merged["effective_intersection"].sum()
                    # For union weight, use the unadjusted intersection.
                    unadjusted_intersection = merged.apply(lambda row: min(row["cg_num_coins_1"], row["cg_num_coins_2"]), axis=1).sum()
                    union_weight = total_weight1 + total_weight2 - unadjusted_intersection
                    effective_similarity = (effective_intersection_weight / union_weight) * 100 if union_weight > 0 else 0

                    # Determine tags.
                    tags = []
                    # Tag: "Highly Similar" if cf_name differ by < 3 characters OR IDs differ by < 5.
                    cf_name1 = str(find1.get("cf_name", ""))
                    cf_name2 = str(find2.get("cf_name", ""))
                    if levenshtein_distance(cf_name1, cf_name2) < 3 or abs(find1["ID"] - find2["ID"]) < 5:
                        tags.append("Highly Similar")
                    # Tag: "Mixed Source" if one cf_user is "CHRE" and the other is "PAS UK Finds".
                    cf_user1 = str(find1.get("cf_user", "")).strip()
                    cf_user2 = str(find2.get("cf_user", "")).strip()
                    if (cf_user1 == "CHRE" and cf_user2 == "PAS UK Finds") or (cf_user1 == "PAS UK Finds" and cf_user2 == "CHRE"):
                        tags.append("Mixed Source")

                    if effective_similarity > 0:
                        results.append((find1["ID"], find2["ID"], effective_similarity, ";".join(tags)))
                except Exception as e:
                    print(f"Error processing pair ({idx1}, {idx2}): {e}")
                finally:
                    pbar.update(1)

        sorted_results = sorted(results, key=lambda x: x[2], reverse=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            if sorted_results:
                f.write("FindID1,FindID2,Similarity (%),Tags\n")
                for find_id1, find_id2, similarity, tags in sorted_results:
                    f.write(f"{find_id1},{find_id2},{similarity:.2f},{tags}\n")
            else:
                f.write("No pairs with non-zero similarity to compare.\n")
        print(f"Pairwise similarity results written to {output_file}")

    except FileNotFoundError as e:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"File not found: {e.filename}\n")
    except Exception as e:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"An error occurred: {e}\n")

if __name__ == "__main__":
    # -----------------------------------------------------------------------------
    # File Paths and Parameters (Update these paths as needed)
    coin_groups_path = "CoinGroups.csv"
    coin_finds_path = "CoinFinds.csv"
    similarity_output_path = "pairwise_similarity_output.csv"  # Intermediate CSV file.
    id_lookup_output_file = "id_lookup_output.json"            # JSON output file.
    
    # Parameters for similarity calculation.
    similarity_columns_coin_groups = ["cg_start_year", "cg_end_year", "DenominationName", "Mint"]
    coin_finds_compare_columns = ["cf_single_find", "cf_hoard", "cf_vague"]  # Additional columns for exact match.
    
    # Process 1: Calculate similarity between CoinFind pairs.
    calculate_similarity_between_pairs(
        coin_groups_path=coin_groups_path,
        coin_finds_path=coin_finds_path,
        similarity_columns_coin_groups=similarity_columns_coin_groups,
        coin_finds_compare_columns=coin_finds_compare_columns,
        x_col="cf_custom_x_coordinate",
        y_col="cf_custom_y_coordinate",
        tolerance_x=0.1,
        tolerance_y=0.1,
        min_coins_sus=3,
        output_file=similarity_output_path
    )

    # Process 2: Look up CoinFind entries based on the similarity CSV and attach coin group data.
    id_lookup_results = find_by_id_from_similarity_csv(
        file_path=coin_finds_path,
        similarity_output_path=similarity_output_path,
        output_file=id_lookup_output_file,
        coin_groups_path=coin_groups_path
    )

    # Process 3: Export the lookup results to Firebase Firestore.
    if id_lookup_results:
        export_to_firestore(id_lookup_results, collection_name="find_results")
