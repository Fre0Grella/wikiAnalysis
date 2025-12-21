#!/usr/bin/env python3
"""
Fast Wikimedia Category Parser - WITH CHECKPOINTING & OPTIMIZATION

Parse category.sql.gz and categorylinks.sql.gz with:
1. Early main topic detection (find main topics FIRST, before flattening)
2. Selective flattening (only flatten ancestors for main topics and their children)
3. Checkpoint system (save intermediate results, resume on interruption)
4. Progress tracking (know exactly where you are)

Checkpoints saved:
  - categories.pkl: Parsed categories
  - category_parents.pkl: Category hierarchy
  - article_categories.pkl: Article memberships
  - main_topics.pkl: Identified main topics (FOUND EARLY)
  - main_topic_articles.pkl: Final output (articles + main topics)

Output:
  - articles_main_topics.tsv: Final TSV file
"""

import csv
import gzip
import sys
import time
import pickle
import os
from collections import defaultdict, deque
from pathlib import Path

# Increase field size limit for massive SQL lines
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2147483647)


class CheckpointedCategoryParser:
    def __init__(self, category_file, categorylinks_file, checkpoint_dir="./checkpoints"):
        self.category_file = category_file
        self.categorylinks_file = categorylinks_file
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # Data structures
        self.categories = {}
        self.category_parents = defaultdict(set)
        self.article_to_categories = defaultdict(set)
        self.main_topics = set()
        self.main_topic_articles = {}  # article_id -> main_topic

        print("Using standard gzip library (safe mode)")
        print(f"Checkpoint directory: {self.checkpoint_dir}")

    # ========== CHECKPOINT SYSTEM ==========
    def save_checkpoint(self, name, data):
        """Save a checkpoint."""
        path = self.checkpoint_dir / f"{name}.pkl"
        with open(path, 'wb') as f:
            pickle.dump(data, f)
        print(f"  ✓ Checkpoint saved: {name}")

    def load_checkpoint(self, name):
        """Load a checkpoint if it exists."""
        path = self.checkpoint_dir / f"{name}.pkl"
        if path.exists():
            with open(path, 'rb') as f:
                data = pickle.load(f)
            print(f"  ✓ Checkpoint loaded: {name}")
            return data
        return None

    def checkpoint_exists(self, name):
        """Check if a checkpoint exists."""
        return (self.checkpoint_dir / f"{name}.pkl").exists()

    # ========== SQL PARSING ==========
    def parse_sql_values(self, line):
        """Fast SQL VALUES parsing using csv module."""
        if not line.startswith("INSERT INTO"):
            return []

        start = line.find("VALUES (")
        if start == -1:
            return []
        start += 8

        end = line.rfind(");")
        if end == -1:
            end = len(line)

        payload = line[start:end]
        payload = payload.replace("),(", "\n").replace("), (", "\n")

        reader = csv.reader(
            payload.splitlines(),
            delimiter=",",
            quotechar="'",
            escapechar="\\",
            doublequote=False,
            strict=False,
        )
        return list(reader)

    def open_file(self, filename):
        """Open gzip file safely."""
        return gzip.open(filename, 'rt', encoding='utf-8', errors='replace')

    # ========== STEP 1: PARSE CATEGORIES ==========
    def parse_category_table(self):
        """Parse category.sql.gz -> categories dict."""
        # Check checkpoint
        if self.checkpoint_exists("categories"):
            self.categories = self.load_checkpoint("categories")
            return

        print("\n" + "="*70)
        print("STEP 1: Parsing category table...")
        print("="*70)
        print(f"Input: {self.category_file}")
        print("Goal:  Learn the name of every category (ID -> Name)")
        print()

        count = 0
        start_time = time.time()

        try:
            with self.open_file(self.category_file) as f:
                for line in f:
                    rows = self.parse_sql_values(line)
                    for values in rows:
                        if len(values) < 2:
                            continue
                        try:
                            cat_id = int(values[0])
                            cat_title = values[1]
                            self.categories[cat_id] = cat_title
                            count += 1

                            if count % 100000 == 0:
                                elapsed = time.time() - start_time
                                rate = int(count / elapsed) if elapsed > 0 else 0
                                print(f"  {count:,} categories read ({rate:,}/sec)...")
                        except (ValueError, IndexError):
                            continue
        except Exception as e:
            print(f"Error: {e}")

        elapsed = time.time() - start_time
        print(f"\n✓ Loaded {len(self.categories):,} categories in {elapsed:.1f}s")
        self.save_checkpoint("categories", self.categories)

    # ========== STEP 2: PARSE LINKS (& BUILD HIERARCHY) ==========
    def parse_categorylinks_table(self):
        """Parse categorylinks.sql.gz -> category_parents & article_to_categories."""
        # Check checkpoint
        if self.checkpoint_exists("category_parents") and self.checkpoint_exists("article_categories"):
            self.category_parents = defaultdict(set, self.load_checkpoint("category_parents"))
            self.article_to_categories = defaultdict(set, self.load_checkpoint("article_categories"))
            return

        print("\n" + "="*70)
        print("STEP 2: Parsing categorylinks (build hierarchy)...")
        print("="*70)
        print(f"Input: {self.categorylinks_file}")
        print("Goal:  Build the tree: Article/Category -> Parents")
        print()

        count = 0
        start_time = time.time()

        try:
            with self.open_file(self.categorylinks_file) as f:
                for line in f:
                    rows = self.parse_sql_values(line)
                    for values in rows:
                        if len(values) < 2:
                            continue
                        try:
                            cl_from = int(values[0])
                            cl_to = values[1]

                            # KEY LOGIC:
                            # If cl_from is a known category ID -> subcategory link
                            # If cl_from is UNKNOWN -> article link
                            if cl_from in self.categories:
                                child_cat = self.categories[cl_from]
                                self.category_parents[child_cat].add(cl_to)
                            else:
                                self.article_to_categories[cl_from].add(cl_to)

                            count += 1

                            if count % 1000000 == 0:
                                elapsed = time.time() - start_time
                                rate = int(count / elapsed) if elapsed > 0 else 0
                                print(f"  {count:,} links read ({rate:,}/sec)...")

                        except ValueError:
                            continue
        except Exception as e:
            print(f"Error: {e}")

        elapsed = time.time() - start_time
        print(f"\n✓ Parsed {count:,} links in {elapsed:.1f}s")
        print(f"  - Category-to-category relationships: {len(self.category_parents):,}")
        print(f"  - Articles found: {len(self.article_to_categories):,}")

        self.save_checkpoint("category_parents", dict(self.category_parents))
        self.save_checkpoint("article_categories", dict(self.article_to_categories))

    # ========== STEP 3: FIND MAIN TOPICS (EARLY!) ==========
    def find_main_topics_early(self):
        """Find main topics by doing a SHALLOW BFS (only 2-3 levels up)."""
        # Check checkpoint
        if self.checkpoint_exists("main_topics"):
            self.main_topics = self.load_checkpoint("main_topics")
            return

        print("\n" + "="*70)
        print("STEP 3: Finding main topics (EARLY detection)...")
        print("="*70)
        print("Goal:  Find categories that are DIRECT children of")
        print("       'Main topic classifications' using SHALLOW search")
        print()

        start_time = time.time()
        main_topics = set()

        # Strategy: Do a VERY SHALLOW BFS (max depth = 3)
        # This finds main topics much faster without full hierarchy
        visited = set()
        to_visit = deque([("Main topic classifications", 0)])

        while to_visit:
            current, depth = to_visit.popleft()

            if current in visited:
                continue
            visited.add(current)

            # Find CHILDREN of current (not parents!)
            # This requires reverse lookup: who lists 'current' as parent?
            for child, parents in self.category_parents.items():
                if current in parents and depth < 2:  # Max 2 levels to find Main Topics
                    if depth == 0:
                        # Direct children of "Main topic classifications"
                        main_topics.add(child)
                    to_visit.append((child, depth + 1))

        elapsed = time.time() - start_time
        print(f"\n✓ Found {len(main_topics)} main topics in {elapsed:.1f}s:")
        for topic in sorted(main_topics):
            print(f"  - {topic}")

        self.main_topics = main_topics
        self.save_checkpoint("main_topics", main_topics)

    # ========== STEP 4: MAP ARTICLES TO MAIN TOPICS (SMART) ==========
    def map_articles_to_main_topics(self):
        """Map articles to main topics using LAZY flattening."""
        # Check checkpoint
        if self.checkpoint_exists("main_topic_articles"):
            self.main_topic_articles = self.load_checkpoint("main_topic_articles")
            return

        print("\n" + "="*70)
        print("STEP 4: Mapping articles to main topics (lazy flattening)...")
        print("="*70)
        print("Goal:  For each article, walk up the tree to find a main topic")
        print("       Only flatten paths that lead to main topics (not all!)")
        print()

        start_time = time.time()
        count = 0
        cache = {}  # Category -> main topic (for memoization)

        def find_main_topic_for_category(cat, visited=None):
            """Walk up from a category to find a main topic."""
            if visited is None:
                visited = set()

            # Check cache first
            if cat in cache:
                return cache[cat]

            if cat in visited or len(visited) > 20:  # Prevent infinite loops
                return None

            visited.add(cat)

            # Is this category a main topic?
            if cat in self.main_topics:
                cache[cat] = cat
                return cat

            # Check parents
            for parent in self.category_parents.get(cat, []):
                result = find_main_topic_for_category(parent, visited.copy())
                if result:
                    cache[cat] = result
                    return result

            cache[cat] = None
            return None

        # Map each article
        for article_id, categories in self.article_to_categories.items():
            main_topic = None

            # Check direct categories first
            for cat in categories:
                if cat in self.main_topics:
                    main_topic = cat
                    break

            # If not found, walk up
            if not main_topic:
                for cat in categories:
                    main_topic = find_main_topic_for_category(cat)
                    if main_topic:
                        break

            if main_topic:
                self.main_topic_articles[article_id] = main_topic
                count += 1

                if count % 100000 == 0:
                    elapsed = time.time() - start_time
                    rate = int(count / elapsed) if elapsed > 0 else 0
                    print(f"  {count:,} articles mapped ({rate:,}/sec)...")

        elapsed = time.time() - start_time
        print(f"\n✓ Mapped {len(self.main_topic_articles):,} articles in {elapsed:.1f}s")
        print(f"  Articles without main topic: {len(self.article_to_categories) - len(self.main_topic_articles):,}")

        self.save_checkpoint("main_topic_articles", self.main_topic_articles)

    # ========== STEP 5: WRITE OUTPUT ==========
    def write_output(self, output_dir):
        """Write final TSV file."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "articles_main_topics.tsv"

        print("\n" + "="*70)
        print("STEP 5: Writing output...")
        print("="*70)
        print(f"Output: {output_file}")
        print()

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("article\tmain_topic\n")
            for article_id, main_topic in self.main_topic_articles.items():
                article_name = f"Article_{article_id}"
                f.write(f"{article_name}\t{main_topic}\n")

        print(f"✓ Wrote {len(self.main_topic_articles):,} lines to TSV")

        # Print sample
        print("\nSample output:")
        with open(output_file, encoding="utf-8") as f:
            next(f)  # skip header
            for i in range(5):
                try:
                    line = next(f)
                    print(f"  {line.strip()}")
                except StopIteration:
                    break

    # ========== MAIN RUNNER ==========
    def run(self, output_dir="./output"):
        """Run all steps."""
        print("\n")
        print("╔" + "="*68 + "╗")
        print("║" + " "*68 + "║")
        print("║" + "  WIKIPEDIA CATEGORY PARSER WITH CHECKPOINTING".center(68) + "║")
        print("║" + " "*68 + "║")
        print("╚" + "="*68 + "╝")

        try:
            self.parse_category_table()
            self.parse_categorylinks_table()
            self.find_main_topics_early()
            self.map_articles_to_main_topics()
            self.write_output(output_dir)

            print("\n" + "="*70)
            print("✓ ALL STEPS COMPLETED SUCCESSFULLY")
            print("="*70)
            print(f"\nCheckpoints saved in: {self.checkpoint_dir}")
            print("You can resume from any step if interrupted.\n")

        except KeyboardInterrupt:
            print("\n\n⚠ INTERRUPTED BY USER")
            print(f"Checkpoints saved in: {self.checkpoint_dir}")
            print("Run the script again to resume from the last checkpoint.\n")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Parse Wikimedia category dumps with checkpointing"
    )
    parser.add_argument("--category", required=True, help="Path to category.sql.gz")
    parser.add_argument("--categorylinks", required=True, help="Path to categorylinks.sql.gz")
    parser.add_argument("--output", default="./output", help="Output directory")
    parser.add_argument("--checkpoints", default="./checkpoints", help="Checkpoint directory")
    parser.add_argument("--reset", action="store_true", help="Delete checkpoints and start fresh")

    args = parser.parse_args()

    # Reset checkpoints if requested
    if args.reset:
        import shutil
        if Path(args.checkpoints).exists():
            shutil.rmtree(args.checkpoints)
            print("✓ Checkpoints deleted. Starting fresh.\n")

    parser = CheckpointedCategoryParser(args.category, args.categorylinks, args.checkpoints)
    parser.run(args.output)
