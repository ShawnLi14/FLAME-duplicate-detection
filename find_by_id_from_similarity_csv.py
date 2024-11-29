import pandas as pd

def find_by_id_from_similarity_csv(file_path, similarity_output_path, output_file):
    """
    Finds entries in the CSV file corresponding to each pair of IDs from the similarity output,
    writes results to a file, and includes similarity percentages.

    Parameters:
        file_path (str): Path to the CoinFinds.csv file.
        similarity_output_path (str): Path to the pairwise similarity output file.
        output_file (str): Path to the output file where results will be written.
    """
    try:
        # Read the similarity output file with utf-8 encoding
        try:
            similarity_df = pd.read_csv(similarity_output_path, encoding="utf-8")
            if not {"FindID1", "FindID2", "Similarity (%)"}.issubset(similarity_df.columns):
                with open(output_file, 'w', encoding="utf-8") as f:
                    f.write("The similarity output file does not have the required columns.\n")
                print("The similarity output file does not have the required columns.")
                return
        except FileNotFoundError:
            with open(output_file, 'w', encoding="utf-8") as f:
                f.write(f"Similarity output file not found: {similarity_output_path}\n")
            print(f"Similarity output file not found: {similarity_output_path}")
            return

        # Read the CoinFinds.csv file with utf-8 encoding
        df = pd.read_csv(file_path, encoding="utf-8")

        # Check if the "ID" column exists
        if "ID" not in df.columns:
            with open(output_file, 'w', encoding="utf-8") as f:
                f.write("The 'ID' column does not exist in the file.\n")
            print("The 'ID' column does not exist in the file.")
            return

        # Open the output file for writing results
        with open(output_file, 'w', encoding="utf-8") as f:
            f.write("Matching entries for each pair with similarity percentage:\n\n")

            # Process each pair of FindID1 and FindID2
            for _, row in similarity_df.iterrows():
                find_id1 = int(row["FindID1"])
                find_id2 = int(row["FindID2"])
                similarity_percentage = row["Similarity (%)"]

                # Filter rows for the current pair
                matching_rows1 = df[df["ID"] == find_id1]
                matching_rows2 = df[df["ID"] == find_id2]

                # Write results for the pair
                f.write(f"Pair: FindID1={find_id1}, FindID2={find_id2}, Similarity={similarity_percentage:.2f}%\n")
                if matching_rows1.empty:
                    f.write(f"  No matching entry found for FindID1={find_id1}\n")
                else:
                    f.write("  Matching entry for FindID1:\n")
                    f.write(matching_rows1.to_string(index=False) + "\n")

                if matching_rows2.empty:
                    f.write(f"  No matching entry found for FindID2={find_id2}\n")
                else:
                    f.write("  Matching entry for FindID2:\n")
                    f.write(matching_rows2.to_string(index=False) + "\n")

                # Create the manual verification link for the pair
                manual_verification_link = f"http://csla100w.princeton.edu:82/?FindFocusIDs={find_id1},{find_id2}"
                f.write(f"  Manual verification link: {manual_verification_link}\n\n")

        print(f"Results written to {output_file}")

    except FileNotFoundError:
        with open(output_file, 'w', encoding="utf-8") as f:
            f.write(f"File not found: {file_path}\n")
        print(f"File not found: {file_path}")
    except Exception as e:
        with open(output_file, 'w', encoding="utf-8") as f:
            f.write(f"An error occurred: {e}\n")
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    file_path = "CoinFinds.csv"  # Path to CoinFinds.csv
    similarity_output_path = "pairwise_similarity_output.csv"  # Path to similarity output file
    output_file = "id_lookup_output.txt"  # Output file path

    find_by_id_from_similarity_csv(file_path, similarity_output_path, output_file)
