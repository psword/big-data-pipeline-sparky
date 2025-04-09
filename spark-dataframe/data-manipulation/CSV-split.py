import pandas as pd
import os

# Set the path to CSV file
input_path = r"<exact file path>\TMDB_movie_dataset_v11.csv"

# Output folder and base filename
output_dir = os.path.dirname(r"<exact file path>\split_chunks")
base_filename = "TMDB_movie_dataset_chunk"

# Step 1: Count total number of rows (excluding the header)
with open(input_path, 'r', encoding='utf-8') as f:
    total_rows = sum(1 for line in f) - 1  # minus header

# Step 2: Decide how many chunks
num_chunks = 3  # You can set this to 2 or 3 as you prefer
rows_per_chunk = total_rows // num_chunks + 1  # Slightly round up

print(f"Total rows: {total_rows}")
print(f"Rows per chunk: {rows_per_chunk}")

# Step 3: Split and save chunks
chunk_iter = pd.read_csv(input_path, chunksize=rows_per_chunk)

for i, chunk in enumerate(chunk_iter):
    output_path = os.path.join(output_dir, f"{base_filename}_{i}.csv")
    chunk.to_csv(output_path, index=False)
    print(f"✅ Saved: {output_path}")

print("🎉 All chunks saved!")
