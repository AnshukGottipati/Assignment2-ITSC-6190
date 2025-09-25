#!/usr/bin/env bash


# ---------- Config ----------
JAR="/opt/hadoop-3.2.1/share/hadoop/mapreduce/DocumentSimilarity-0.0.1-SNAPSHOT.jar"
CLASS="com.example.controller.DocumentSimilarityDriver"

HDFS_INPUT_DIR="/input/dataset"   # where your 3 files live on HDFS
FILES=(
  "dataset_1_1000_words.txt"
  "dataset_2_3000_words.txt"
  "dataset_3_5000_words.txt"
)

CSV="timings.csv"                 # results will be appended here
# -----------------------------

# Build the jar if it doesn't exist
if [[ ! -f "$JAR" ]]; then
  echo "[build] $JAR not found; running mvn package..."
  mvn -q -DskipTests package
fi

# Ensure inputs exist on HDFS
echo "[check] Listing HDFS input dir: $HDFS_INPUT_DIR"
hdfs dfs -ls "$HDFS_INPUT_DIR" || {
  echo "ERROR: HDFS input dir $HDFS_INPUT_DIR not found. Upload your datasets first."
  exit 1
}

# Init CSV
echo "dataset,wall_seconds,stage2_dir,final_dir,exit_code" > "$CSV"

run_one() {
  local file="$1"
  local in="$HDFS_INPUT_DIR/$file"
  local tag="${file//[^0-9a-zA-Z]/_}"
  local stage2="/output/ds_stage2_${tag}"
  local out="/output/ds_similarity_${tag}"

  echo
  echo "=== Running $file ==="
  echo "[prep] cleaning $stage2 and $out if they exist..."
  hdfs dfs -rm -r -f "$stage2" "$out" >/dev/null 2>&1 || true

  # Time the whole 3-stage pipeline
  SECONDS=0
  hadoop jar "$JAR" "$CLASS" "$in" "$stage2" "$out"
  exit_code=$?
  elapsed=$SECONDS

  echo "[done] exit=$exit_code elapsed=${elapsed}s"
  echo "${file},${elapsed},${stage2},${out},${exit_code}" | tee -a "$CSV"

  if [[ $exit_code -eq 0 ]]; then
    echo "[peek] output listing:"
    hdfs dfs -ls "$out" || true
    echo "[peek] first 5 lines:"
    hdfs dfs -cat "$out/part-"* 2>/dev/null | head -n 5 || true
  fi
}

for f in "${FILES[@]}"; do
  run_one "$f"
done

echo
echo "All done. Results saved to ${CSV}"
