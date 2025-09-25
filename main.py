import random
from pathlib import Path
from collections import Counter

# --- Config ---
random.seed(42)  # for reproducibility

# Vocabulary to synthesize text
VOCAB = [
    "this","is","a","sample","document","with","some","words","another","that","also","has","text",
    "different","simple","example","contains","data","information","about","topics","random","lorem",
    "ipsum","dolor","sit","amet","consectetur","adipiscing","elit","sed","do","eiusmod","tempor",
    "incididunt","ut","labore","et","dolore","magna","aliqua","ut","enim","ad","minim","veniam",
    "quis","nostrud","exercitation","ullamco","laboris","nisi","aliquip","ex","ea","commodo",
    "consequat","duis","aute","irure","in","reprehenderit","voluptate","velit","esse","cillum",
    "eu","fugiat","nulla","pariatur","excepteur","sint","occaecat","cupidatat","non","proident",
    "sunt","in","culpa","qui","officia","deserunt","mollit","anim","id","est","laborum","python",
    "code","index","query","search","engine","machine","learning","vector","token","string","number",
    "graph","tree","node","edge","value","key","result","score","rank","merge","split","shuffle"
]

def generate_dataset(total_words: int, vocab=VOCAB) -> str:
    """
    Returns dataset text with lines of the form:
      DocumentN <space-separated words...>
    Total word count across all documents equals `total_words`.
    """
    num_docs = max(5, min(200, total_words // 50 if total_words >= 50 else 5))
    base = total_words // num_docs
    remainder = total_words % num_docs
    words_per_doc = [base + (1 if i < remainder else 0) for i in range(num_docs)]

    lines = []
    for i, n in enumerate(words_per_doc, start=1):
        doc_words = [random.choice(vocab) for _ in range(n)]
        if doc_words:
            doc_words[0] = doc_words[0].capitalize()
        lines.append(f"Document{i} " + " ".join(doc_words))
    return "\n".join(lines)

def write_dataset_file(name: str, content: str) -> str:
    path = Path(f"{name}.txt")
    path.write_text(content, encoding="utf-8")
    return str(path)

def analyze_dataset(content: str):
    lines = content.strip().splitlines()
    total_words = 0
    vocab_counter = Counter()
    for line in lines:
        try:
            _, text = line.split(" ", 1)
        except ValueError:
            text = ""
        words = text.strip().split()
        total_words += len(words)
        vocab_counter.update([w.lower() for w in words])
    return {
        "documents": len(lines),
        "total_words": total_words,
        "unique_words": len(vocab_counter),
        "top10": vocab_counter.most_common(10)
    }

if __name__ == "__main__":
    targets = [1000, 3000, 5000]
    for i, t in enumerate(targets, start=1):
        content = generate_dataset(t)
        fname = write_dataset_file(f"dataset_{i}_{t}_words", content)
        stats = analyze_dataset(content)
        print(f"{fname}:")
        print(f"  documents={stats['documents']}, total_words={stats['total_words']}, unique_words={stats['unique_words']}")
        print(f"  top10={stats['top10']}")
