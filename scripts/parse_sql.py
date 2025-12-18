#!/usr/bin/env python3
"""
Parse Wikimedia SQL dumps to create a flattened category tree.

Input:
  - page.sql.gz: Maps page_id -> page_title, page_namespace
  - categorylinks.sql.gz: Maps (cl_from=page_id) -> cl_to=category_name

Output:
  - category_hierarchy.tsv: (category) -> (parent_category)
  - article_categories.tsv: (article) -> (category)
  - category_tree_flat.tsv: (category) -> (all_ancestors, depth)

This flattens the tree so you can group articles by topic at any depth level.
"""

import gzip
import re
import sys
from collections import defaultdict, deque
from pathlib import Path

class SQLDumpParser:
    def __init__(self, page_file, categorylinks_file):
        self.page_file = page_file
        self.categorylinks_file = categorylinks_file

        # Data structures
        self.page_id_to_title = {}  # page_id -> page_title
        self.article_to_categories = defaultdict(set)  # article_title -> {categories}
        self.category_parents = defaultdict(set)  # category -> {parent_categories}
        self.category_children = defaultdict(set)  # category -> {child_categories}

    def parse_sql_insert(self, line):
        """
        Parse a single SQL INSERT statement line.

        Format: INSERT INTO `table` VALUES (val1, val2, val3), (val4, val5, val6), ...;

        Returns: List of tuples, each tuple is one row of values.
        """
        # Remove leading "INSERT INTO ... VALUES "
        if not line.startswith("INSERT"):
            return []

        # Extract the part after "VALUES"
        match = re.search(r'VALUES\s*\((.*)\);?$', line)
        if not match:
            return []

        values_str = match.group(1)

        # Split by "), (" to separate rows
        rows = []
        row_strings = re.split(r'\),\s*\(', values_str)

        for row_str in row_strings:
            # Clean up parentheses
            row_str = row_str.strip("()")

            # Parse individual values
            # SQL values can be: NULL, numbers, or 'strings'
            # We need to handle escaped quotes: \'
            values = []
            current_val = ""
            in_string = False

            i = 0
            while i < len(row_str):
                char = row_str[i]

                if char == "'" and (i == 0 or row_str[i-1] != "\\"):
                    in_string = not in_string
                    current_val += char
                elif char == "," and not in_string:
                    values.append(current_val.strip())
                    current_val = ""
                else:
                    current_val += char

                i += 1

            if current_val:
                values.append(current_val.strip())

            rows.append(values)

        return rows

    def extract_sql_value(self, val_str):
        """
        Extract Python value from SQL representation.

        NULL -> None
        'string' -> string (without quotes)
        12345 -> 12345
        """
        val_str = val_str.strip()

        if val_str.upper() == "NULL":
            return None

        if val_str.startswith("'") and val_str.endswith("'"):
            # Remove quotes and unescape
            return val_str[1:-1].replace("\\'", "'").replace('\\"', '"')

        try:
            return int(val_str)
        except ValueError:
            return val_str

    def parse_page_table(self):
        """
        Parse page.sql.gz to build page_id -> page_title mapping.

        SQL schema: page_id, page_title, page_namespace, page_is_redirect, ...
        We care about:
          - Column 0: page_id
          - Column 1: page_title
          - Column 2: page_namespace (0 = main articles, 14 = categories)
        """
        print("Parsing page table...")

        count = 0
        with gzip.open(self.page_file, 'rt', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if not line.startswith("INSERT"):
                    continue

                rows = self.parse_sql_insert(line)

                for values in rows:
                    if len(values) < 3:
                        continue

                    try:
                        page_id = self.extract_sql_value(values[0])
                        page_title = self.extract_sql_value(values[1])
                        page_namespace = self.extract_sql_value(values[2])

                        if page_id is None or page_title is None:
                            continue

                        self.page_id_to_title[page_id] = page_title
                        count += 1

                        if count % 100000 == 0:
                            print(f"  ... parsed {count:,} pages")
                    except Exception as e:
                        continue

        print(f"  ✓ Parsed {count:,} pages")

    def parse_categorylinks_table(self):
        """
        Parse categorylinks.sql.gz to build article->category and category->category links.

        SQL schema: cl_from, cl_to, cl_sortkey, ...
        We care about:
          - Column 0: cl_from (page_id)
          - Column 1: cl_to (category name, as string)

        Logic:
          - If page_id is in page_id_to_title and title doesn't start with "Category:",
            it's an article -> category link
          - If page_id title starts with "Category:", it's a subcategory link
        """
        print("Parsing categorylinks table...")

        count = 0
        with gzip.open(self.categorylinks_file, 'rt', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if not line.startswith("INSERT"):
                    continue

                rows = self.parse_sql_insert(line)

                for values in rows:
                    if len(values) < 2:
                        continue

                    try:
                        cl_from = self.extract_sql_value(values[0])
                        cl_to = self.extract_sql_value(values[1])

                        if cl_from is None or cl_to is None:
                            continue

                        # Look up the source page
                        source_title = self.page_id_to_title.get(cl_from)
                        if source_title is None:
                            continue

                        # Determine link type
                        if source_title.startswith("Category:"):
                            # Category -> Parent Category link
                            source_cat = source_title.replace("Category:", "")
                            self.category_parents[source_cat].add(cl_to)
                            self.category_children[cl_to].add(source_cat)
                        else:
                            # Article -> Category link
                            self.article_to_categories[source_title].add(cl_to)

                        count += 1

                        if count % 100000 == 0:
                            print(f"  ... parsed {count:,} links")
                    except Exception as e:
                        continue

        print(f"  ✓ Parsed {count:,} category links")

    def flatten_category_tree(self, max_depth=15):
        """
        For each category, find ALL its ancestor categories (up to max_depth).

        Uses BFS to traverse the hierarchy.

        Output: category -> (ancestors, depth)
        where ancestors is a comma-separated list of all parent categories.
        """
        print("Flattening category tree...")

        # Result: category -> (list_of_all_ancestors, max_depth_reached)
        category_ancestors = {}

        for category in self.category_parents.keys():
            ancestors = []
            visited = set()
            queue = deque([(cat, 0) for cat in self.category_parents.get(category, [])])
            max_d = 0

            while queue and max_d < max_depth:
                current_cat, depth = queue.popleft()

                if current_cat in visited:
                    continue
                visited.add(current_cat)

                ancestors.append((current_cat, depth + 1))
                max_d = max(max_d, depth + 1)

                # Add parents of this category
                for parent in self.category_parents.get(current_cat, []):
                    if parent not in visited:
                        queue.append((parent, depth + 1))

            # Sort by depth (closest ancestors first)
            ancestors.sort(key=lambda x: x[1])
            ancestor_names = [a[0] for a in ancestors]

            category_ancestors[category] = {
                "ancestors": ancestor_names,
                "depth": max_d
            }

        print(f"  ✓ Flattened {len(category_ancestors):,} categories")
        return category_ancestors

    def write_output_files(self, category_ancestors, output_dir="./output"):
        """
        Write three TSV files:
          1. category_hierarchy.tsv: (child_category) -> (parent_category)
          2. article_categories.tsv: (article) -> (category)
          3. category_tree_flat.tsv: (category) -> (ancestors, max_depth)
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # 1. Category hierarchy
        print(f"Writing category_hierarchy.tsv...")
        with open(output_dir / "category_hierarchy.tsv", "w") as f:
            f.write("child_category\tparent_category\n")
            count = 0
            for child, parents in self.category_parents.items():
                for parent in parents:
                    f.write(f"{child}\t{parent}\n")
                    count += 1
        print(f"  ✓ Wrote {count:,} category relationships")

        # 2. Article -> categories
        print(f"Writing article_categories.tsv...")
        with open(output_dir / "article_categories.tsv", "w") as f:
            f.write("article\tcategory\n")
            count = 0
            for article, categories in self.article_to_categories.items():
                for category in categories:
                    f.write(f"{article}\t{category}\n")
                    count += 1
        print(f"  ✓ Wrote {count:,} article-category links")

        # 3. Flattened category tree
        print(f"Writing category_tree_flat.tsv...")
        with open(output_dir / "category_tree_flat.tsv", "w") as f:
            f.write("category\tancestors\tmax_depth\n")
            count = 0
            for category, info in category_ancestors.items():
                ancestors_str = ";".join(info["ancestors"]) if info["ancestors"] else ""
                f.write(f"{category}\t{ancestors_str}\t{info['depth']}\n")
                count += 1
        print(f"  ✓ Wrote {count:,} categories with flattened trees")

    def run(self, output_dir="./output"):
        """Main execution pipeline."""
        print("=" * 60)
        print("SQL Dump Parser: Category Tree Flattener")
        print("=" * 60)
        print()

        self.parse_page_table()
        self.parse_categorylinks_table()
        category_ancestors = self.flatten_category_tree()
        self.write_output_files(category_ancestors, output_dir)

        print()
        print("=" * 60)
        print("✓ Done!")
        print("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Parse Wikimedia SQL dumps to flatten category tree"
    )
    parser.add_argument(
        "--page",
        default="./dataset/sql_dumps/enwiki-latest-page.sql.gz",
        help="Path to page.sql.gz"
    )
    parser.add_argument(
        "--categorylinks",
        default="./dataset/sql_dumps/enwiki-latest-categorylinks.sql.gz",
        help="Path to categorylinks.sql.gz"
    )
    parser.add_argument(
        "--output",
        default="./output",
        help="Output directory for TSV files"
    )

    args = parser.parse_args()

    parser = SQLDumpParser(args.page, args.categorylinks)
    parser.run(args.output)
