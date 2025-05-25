import os
import io
import math
import json
import zipfile
import requests
import pandas as pd
from itertools import combinations
from tqdm import tqdm
import firebase_admin
from firebase_admin import credentials, firestore

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
SERVICE_ACCOUNT_KEY_PATH = os.getenv(
    "SERVICE_ACCOUNT_KEY_PATH",
    "C:\\Users\\shamb\\Downloads\\"
    "flame-duplicates-firebase-adminsdk-fbsvc-e825b279bf.json"
)
DOWNLOAD_URL = (
    "http://csla100w.princeton.edu:82/Home/DownloadCSV"
    "?region=null&subregion=null&metal=&denomination=&"
    "excavationsHoards=&year=325-750&showTotalValues=false&"
    "RulerID=null&datasets=6,1,4,2,5,3&VagueDates=0&ObjectIDs=&"
    "Reference=&Country=&YearFromExcavationDatesCS=&"
    "YearToExcavationDatesCS="
)
ZIP_FILENAME = "flame_data.zip"
EXTRACT_FOLDER = "flame_data"
COLLECTION_NAME = "find_results"

# -----------------------------------------------------------------------------
# DOWNLOAD & EXTRACT
# -----------------------------------------------------------------------------
def download_and_extract_zip(url, zip_path, extract_to):
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print(f"Downloaded ZIP to {zip_path}")

    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(path=extract_to)
    print(f"Extracted ZIP into folder {extract_to}")

# -----------------------------------------------------------------------------
# FIRESTORE HELPERS
# -----------------------------------------------------------------------------
def initialize_firestore(service_account_key_path):
    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account_key_path)
        firebase_admin.initialize_app(cred)
    return firestore.client()

def export_to_firestore_if_new(data, collection_name=COLLECTION_NAME):
    """
    Adds each record only if a document with the same FindID1_FindID2 ID doesn't exist.
    Uses a stable doc ID of "FindID1_FindID2".
    """
    db = initialize_firestore(SERVICE_ACCOUNT_KEY_PATH)
    col = db.collection(collection_name)
    added = 0
    for record in data:
        # build a unique document ID
        fid1 = record.get("FindID1")
        fid2 = record.get("FindID2")
        if fid1 is None or fid2 is None:
            # skip entries without both IDs
            continue
        doc_id = f"{fid1}_{fid2}"
        doc_ref = col.document(doc_id)
        if not doc_ref.get().exists:
            doc_ref.set(record)
            added += 1
    print(f"Added {added} new records to Firestore collection '{collection_name}'.")

# -----------------------------------------------------------------------------
# UTILITY FUNCTIONS
# -----------------------------------------------------------------------------
def replace_nan(obj):
    if isinstance(obj, dict):
        return {k: replace_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [replace_nan(v) for v in obj]
    if isinstance(obj, float):
        return None if math.isnan(obj) else obj
    return obj

def levenshtein_distance(s1, s2):
    if s1 is None: s1 = ""
    if s2 is None: s2 = ""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    prev = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            ins = prev[j + 1] + 1
            rem = curr[j] + 1
            sub = prev[j] + (c1 != c2)
            curr.append(min(ins, rem, sub))
        prev = curr
    return prev[-1]

def has_shared_substring_of_length_n(str1, str2, n=3):
    if not str1 or not str2:
        return False
    for i in range(len(str1) - n + 1):
        if str1[i : i + n] in str2:
            return True
    return False

# -----------------------------------------------------------------------------
# STEP 1: ID LOOKUP
# -----------------------------------------------------------------------------
def find_by_id_from_similarity_csv(
    file_path, similarity_output_path, output_file, coin_groups_path
):
    try:
        similarity_df = pd.read_csv(similarity_output_path, encoding="utf-8")
        required = {"FindID1", "FindID2", "Similarity (%)"}
        if not required.issubset(similarity_df.columns):
            raise ValueError("Similarity file missing required columns.")

        df = pd.read_csv(file_path, encoding="utf-8")
        if "ID" not in df.columns:
            raise ValueError("CoinFinds.csv missing 'ID' column.")

        coin_groups = pd.read_csv(coin_groups_path, encoding="utf-8")

        results = []
        for _, row in similarity_df.iterrows():
            a, b = row["FindID1"], row["FindID2"]
            sim = row["Similarity (%)"]
            tags = row.get("Tags", "")

            try: a = int(a)
            except: a = None
            try: b = int(b)
            except: b = None
            try:
                sim = float(sim)
                if math.isnan(sim): sim = None
            except:
                sim = None

            tag_list = [t.strip() for t in tags.split(";")] if isinstance(tags, str) else []

            rows_a = df[df["ID"] == a].to_dict(orient="records") if a else None
            rows_b = df[df["ID"] == b].to_dict(orient="records") if b else None
            groups_a = coin_groups[coin_groups["cfID"] == a].to_dict(orient="records") if a else None
            groups_b = coin_groups[coin_groups["cfID"] == b].to_dict(orient="records") if b else None

            link = (
                f"http://csla100w.princeton.edu:82/?FindFocusIDs={a},{b}"
                if a and b else None
            )

            results.append({
                "FindID1": a,
                "FindID2": b,
                "Similarity": sim,
                "Tags": tag_list,
                "Entry1": rows_a,
                "Entry2": rows_b,
                "CoinGroups1": groups_a,
                "CoinGroups2": groups_b,
                "ManualVerificationLink": link
            })

        cleaned = replace_nan(results)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=4)
        print(f"Wrote ID lookup JSON to {output_file}")
        return cleaned

    except Exception as e:
        err = str(e)
        print("Error in ID lookup:", err)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump({"error": err}, f, indent=4)
        return None

# -----------------------------------------------------------------------------
# STEP 2: PAIRWISE SIMILARITY
# -----------------------------------------------------------------------------
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
    output_file="pairwise_similarity_output.csv",
):
    try:
        coin_groups = pd.read_csv(coin_groups_path)
        coin_finds = pd.read_csv(coin_finds_path)

        for col in ["cfID"] + similarity_columns_coin_groups + ["cg_num_coins"]:
            if col not in coin_groups.columns:
                raise ValueError(f"CoinGroups.csv missing column '{col}'")
        required_cf = (
            ["ID", "cf_num_coins_found", x_col, y_col, "cf_publication_ref"]
            + coin_finds_compare_columns
            + ["cf_name", "cf_user"]
        )
        for col in required_cf:
            if col not in coin_finds.columns:
                raise ValueError(f"CoinFinds.csv missing column '{col}'")

        coin_finds = coin_finds[coin_finds["cf_num_coins_found"] > 0]
        valid_ids = set(coin_groups["cfID"])
        coin_finds = coin_finds[coin_finds["ID"].isin(valid_ids)]
        coin_finds["bin_x"] = (coin_finds[x_col] / tolerance_x).astype(int)
        coin_finds["bin_y"] = (coin_finds[y_col] / tolerance_y).astype(int)

        grouping = coin_finds_compare_columns + ["bin_x", "bin_y"]
        suspicious = coin_finds.groupby(grouping).filter(lambda g: len(g) > 1)
        if suspicious.empty:
            with open(output_file, "w") as f:
                f.write("No suspicious CoinFinds identified for comparison.\n")
            print("No bin groups with >1 find.")
            return

        grp = coin_groups.groupby("cfID")
        suspicious["AttributedGroups"] = suspicious["ID"].map(
            lambda i: grp.get_group(i) if i in grp.groups else pd.DataFrame([], columns=similarity_columns_coin_groups + ["cg_num_coins"])
        )

        pairs = []
        for _, sub in suspicious.groupby(grouping):
            idxs = sub.index.tolist()
            pairs.extend(combinations(idxs, 2))

        results = []
        with tqdm(total=len(pairs), desc="Processing CoinFind pairs") as pbar:
            for i1, i2 in pairs:
                f1 = suspicious.loc[i1]
                f2 = suspicious.loc[i2]
                dx, dy = abs(f1[x_col]-f2[x_col]), abs(f1[y_col]-f2[y_col])
                if dx>tolerance_x or dy>tolerance_y:
                    pbar.update()
                    continue
                if f1["cf_num_coins_found"]<min_coins_sus or f2["cf_num_coins_found"]<min_coins_sus:
                    pbar.update()
                    continue
                if not has_shared_substring_of_length_n(str(f1["cf_publication_ref"]), str(f2["cf_publication_ref"]), 3):
                    pbar.update()
                    continue
                if any(f1[c]!=f2[c] for c in coin_finds_compare_columns):
                    pbar.update()
                    continue

                g1 = f1["AttributedGroups"]
                g2 = f2["AttributedGroups"]
                if g1.empty or g2.empty:
                    pbar.update()
                    continue

                w1 = g1.groupby(similarity_columns_coin_groups, as_index=False).agg({"cg_num_coins":"sum"})
                w2 = g2.groupby(similarity_columns_coin_groups, as_index=False).agg({"cg_num_coins":"sum"})
                total1, total2 = w1["cg_num_coins"].sum(), w2["cg_num_coins"].sum()

                merged = pd.merge(w1, w2, on=similarity_columns_coin_groups, how="inner", suffixes=("_1","_2"))
                merged["factor"] = merged.apply(
                    lambda r: (min(r["cg_num_coins_1"],r["cg_num_coins_2"])/max(r["cg_num_coins_1"],r["cg_num_coins_2"]))
                    if max(r["cg_num_coins_1"],r["cg_num_coins_2"])>0 else 1, axis=1
                )
                merged["effect"] = merged.apply(
                    lambda r: min(r["cg_num_coins_1"],r["cg_num_coins_2"])*r["factor"], axis=1
                )
                eff_int = merged["effect"].sum()
                unadj_int = merged.apply(lambda r: min(r["cg_num_coins_1"],r["cg_num_coins_2"]), axis=1).sum()
                union = total1 + total2 - unadj_int
                sim_pct = (eff_int/union)*100 if union>0 else 0

                tags = []
                if levenshtein_distance(str(f1["cf_name"]), str(f2["cf_name"]))<3 or abs(f1["ID"]-f2["ID"])<5:
                    tags.append("Highly Similar")
                u1,u2 = str(f1["cf_user"]).strip(), str(f2["cf_user"]).strip()
                if {u1,u2}=={"CHRE","PAS UK Finds"}:
                    tags.append("Mixed Source")

                if sim_pct>0:
                    results.append((f1["ID"], f2["ID"], sim_pct, ";".join(tags)))
                pbar.update()

        with open(output_file, "w", encoding="utf-8") as f:
            if results:
                f.write("FindID1,FindID2,Similarity (%),Tags\n")
                for a,b,s,t in sorted(results, key=lambda x: x[2], reverse=True):
                    f.write(f"{a},{b},{s:.2f},{t}\n")
            else:
                f.write("No pairs with non-zero similarity to compare.\n")
        print(f"Wrote pairwise similarity CSV to {output_file}")

    except Exception as e:
        print("Error in similarity calc:", e)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"Error: {e}\n")

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. Download & extract
    download_and_extract_zip(DOWNLOAD_URL, ZIP_FILENAME, EXTRACT_FOLDER)

    # 2. Set file paths
    coin_groups_path = os.path.join(EXTRACT_FOLDER, "CoinGroups.csv")
    coin_finds_path  = os.path.join(EXTRACT_FOLDER, "CoinFinds.csv")

    # 3. Define params and intermediate outputs
    similarity_output_csv = "pairwise_similarity_output.csv"
    id_lookup_json       = "id_lookup_output.json"
    sim_cols_groups = ["cg_start_year", "cg_end_year", "DenominationName", "Mint"]
    find_cmp_cols  = ["cf_single_find", "cf_hoard", "cf_vague"]

    # 4. Run similarity
    calculate_similarity_between_pairs(
        coin_groups_path=coin_groups_path,
        coin_finds_path=coin_finds_path,
        similarity_columns_coin_groups=sim_cols_groups,
        coin_finds_compare_columns=find_cmp_cols,
        x_col="cf_custom_x_coordinate",
        y_col="cf_custom_y_coordinate",
        tolerance_x=0.1,
        tolerance_y=0.1,
        min_coins_sus=3,
        output_file=similarity_output_csv,
    )

    # 5. Run ID lookup & export only new
    id_results = find_by_id_from_similarity_csv(
        file_path=coin_finds_path,
        similarity_output_path=similarity_output_csv,
        output_file=id_lookup_json,
        coin_groups_path=coin_groups_path,
    )
    if id_results:
        export_to_firestore_if_new(id_results, collection_name=COLLECTION_NAME)
