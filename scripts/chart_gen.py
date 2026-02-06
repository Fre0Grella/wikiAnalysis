import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import Counter
from matplotlib.colors import LinearSegmentedColormap

# Load data
bus_factor = pd.read_csv('output_aws/bus_factor.tsv', sep='\t',
                         names=['Category', 'BusFactor', 'TotalBytes'])
top_contributors = pd.read_csv('output_aws/top_contributors.tsv', sep='\t',
                               names=['Category', 'Username', 'BytesContributed'])

# Remove Main_topic_articles (special case)
bus_factor = bus_factor[bus_factor['Category'] != 'Main_topic_articles']

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("Set2")

# Create figure with 6 subplots
fig = plt.figure(figsize=(20, 12))

# ============================================
# CHART 1: Bus Factor Distribution with GRADIENT
# ============================================
ax1 = plt.subplot(2, 3, 1)
bus_factor_sorted = bus_factor.sort_values('BusFactor', ascending=True)

# Create gradient colormap from red (low) to green (high)
cmap = LinearSegmentedColormap.from_list('bus_factor', ['#d62728', '#ff7f0e', '#2ca02c'])
norm = plt.Normalize(vmin=bus_factor_sorted['BusFactor'].min(),
                     vmax=bus_factor_sorted['BusFactor'].max())
colors_gradient = cmap(norm(bus_factor_sorted['BusFactor']))

bars = ax1.barh(range(len(bus_factor_sorted)), bus_factor_sorted['BusFactor'],
                color=colors_gradient)
ax1.set_yticks(range(len(bus_factor_sorted)))
ax1.set_yticklabels(bus_factor_sorted['Category'], fontsize=8)
ax1.axvline(x=2000, color='black', linestyle='--', linewidth=2,
            label='Stability Threshold (2000)')
ax1.set_xlabel('Bus Factor', fontsize=10, fontweight='bold')
ax1.set_title('Bus Factor by Category\n(Gradient: Red=Vulnerable → Green=Stable)',
              fontsize=12, fontweight='bold')
ax1.legend()
ax1.grid(axis='x', alpha=0.3)

# ============================================
# CHART 2: Content Volume Distribution
# ============================================
ax2 = plt.subplot(2, 3, 2)
sorted_contents = bus_factor.sort_values('TotalBytes', ascending=True)
colors_content = ['#2ca02c' if bf >= 2000 else '#d62728' for bf in sorted_contents['BusFactor']]
bars = ax2.barh(range(len(sorted_contents)), sorted_contents['TotalBytes'] / 3e9,
                color=colors_content)
ax2.set_yticks(range(len(sorted_contents)))
ax2.set_yticklabels(sorted_contents['Category'], fontsize=9)
ax2.set_xlabel('Content Volume (GB)', fontsize=10, fontweight='bold')
ax2.set_title('Categories by Content Volume', fontsize=12, fontweight='bold')
ax2.grid(axis='x', alpha=0.3)

# Legenda per colori (verde/rosso in base al bus factor)
from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor='#2ca02c', edgecolor='black', label='Bus factor ≥ 2000 (stable)'),
    Patch(facecolor='#d62728', edgecolor='black', label='Bus factor < 2000')
]
ax2.legend(handles=legend_elements, loc='lower right', fontsize=8, title='Bus factor status')

# ============================================
# CHART 3: Cross-Category Distribution
# ============================================
ax3 = plt.subplot(2, 3, 3)
contributor_categories = top_contributors.groupby('Username')['Category'].count()
distribution = contributor_categories.value_counts().sort_index()
x_vals = range(1, min(32, distribution.index.max() + 1))
y_vals = [distribution.get(i, 0) for i in x_vals]
colors_dist = ['#1f77b4' if i < 10 else '#ff7f0e' if i < 20 else '#d62728' for i in x_vals]
ax3.bar(x_vals, y_vals, color=colors_dist, edgecolor='black', linewidth=0.5)
ax3.set_xlabel('Number of Categories', fontsize=10, fontweight='bold')
ax3.set_ylabel('Number of Contributors', fontsize=10, fontweight='bold')
ax3.set_title('Cross-Category Contributor Distribution', fontsize=12, fontweight='bold')
ax3.set_yscale('log')
ax3.grid(axis='y', alpha=0.3)

# ============================================
# CHART 4: Contributor Concentration (Pareto Chart)
# FIXED: Deduplicate contributions to avoid counting same edits multiple times
# ============================================
ax4 = plt.subplot(2, 3, 4)

# Deduplicate: Take max BytesContributed per Username to avoid triplication
# when same article appears in multiple categories
total_bytes_per_contributor = top_contributors.groupby('Username')['BytesContributed'].max().sort_values(ascending=False)

cumsum_bytes = total_bytes_per_contributor.cumsum()
cumsum_pct = (cumsum_bytes / total_bytes_per_contributor.sum()) * 100
num_contributors = range(1, len(total_bytes_per_contributor) + 1)

# Create dual-axis plot
ax4_twin = ax4.twinx()
ax4.plot(num_contributors, cumsum_pct, color='#2ca02c', linewidth=3, label='Cumulative %')
ax4_twin.bar(num_contributors, total_bytes_per_contributor.values / 1e6,
             color='#1f77b4', alpha=0.3, width=1, label='Individual Contribution')

# Add threshold lines
ax4.axhline(y=50, color='red', linestyle='--', linewidth=2, alpha=0.7, label='50% threshold')
ax4.axhline(y=80, color='orange', linestyle='--', linewidth=2, alpha=0.7, label='80% threshold')

# Find and annotate key points
top_50_idx = np.argmax(cumsum_pct >= 50) + 1
top_80_idx = np.argmax(cumsum_pct >= 80) + 1

ax4.scatter([top_50_idx], [50], color='red', s=100, zorder=5)
ax4.text(top_50_idx, 55, f'{top_50_idx:,} contributors\n(50% of bytes)',
         fontsize=9, ha='left', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

ax4.scatter([top_80_idx], [80], color='orange', s=100, zorder=5)
ax4.text(top_80_idx, 85, f'{top_80_idx:,} contributors\n(80% of bytes)',
         fontsize=9, ha='left', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

ax4.set_xlabel('Number of Contributors (ranked)', fontsize=10, fontweight='bold')
ax4.set_ylabel('Cumulative % of Bytes', fontsize=10, fontweight='bold', color='#2ca02c')
ax4_twin.set_ylabel('Individual Contribution (MB)', fontsize=10, fontweight='bold', color='#1f77b4')
ax4.set_title('Contributor Concentration (Deduplicated)\n(Few Contributors Account for Majority)',
              fontsize=12, fontweight='bold')
ax4.set_xlim([0, min(5000, len(num_contributors))])
ax4.set_ylim([0, 105])
ax4.tick_params(axis='y', labelcolor='#2ca02c')
ax4_twin.tick_params(axis='y', labelcolor='#1f77b4')
ax4.legend(loc='upper left', fontsize=8)
ax4.grid(alpha=0.3)

# ============================================
# CHART 5: Category Pair Overlaps
# ============================================
ax5 = plt.subplot(2, 3, 5)
two_cat_contributors = contributor_categories[contributor_categories == 2]
category_pairs = []
contributor_cats = top_contributors.groupby('Username')['Category'].apply(list)
for username in two_cat_contributors.index:
    cats = contributor_cats[username]
    if len(cats) == 2:
        category_pairs.append(tuple(sorted(cats)))
pair_counts = Counter(category_pairs).most_common(20)
pair_names = [f"{p[0][:12]}\n{p[1][:12]}" for p, c in pair_counts]
pair_values = [c for p, c in pair_counts]
colors_pairs = plt.cm.YlOrRd(np.linspace(0.3, 0.9, len(pair_values)))
ax5.barh(range(len(pair_names)), pair_values, color=colors_pairs)
ax5.set_yticks(range(len(pair_names)))
ax5.set_yticklabels(pair_names, fontsize=7)
ax5.set_xlabel('Shared Contributors', fontsize=10, fontweight='bold')
ax5.set_title('Top 20 Category Pairs', fontsize=12, fontweight='bold')
ax5.grid(axis='x', alpha=0.3)

# ============================================
# CHART 6: Vulnerability vs Content Scatter WITH LABELS
# ============================================
ax6 = plt.subplot(2, 3, 6)
x = bus_factor['TotalBytes'] / 3e9
y = bus_factor['BusFactor']
colors_scatter = ['#2ca02c' if bf >= 2000 else '#d62728' for bf in y]
ax6.scatter(x, y, c=colors_scatter, s=100, alpha=0.6, edgecolors='black', linewidth=1)
ax6.axhline(y=2000, color='black', linestyle='--', linewidth=2, label='Stability Threshold')

# Detect outliers using IQR method
q1_x, q3_x = x.quantile(0.25), x.quantile(0.75)
iqr_x = q3_x - q1_x
x_outlier_threshold = q3_x + 1.5 * iqr_x

q1_y, q3_y = y.quantile(0.25), y.quantile(0.75)
iqr_y = q3_y - q1_y
y_lower_threshold = q1_y - 1.5 * iqr_y

outliers = bus_factor[
    ((bus_factor['TotalBytes'] / 3e9) > x_outlier_threshold) |
    (bus_factor['BusFactor'] < y_lower_threshold)
    ]

# Label outliers
for _, row in outliers.iterrows():
    x_pos = row['TotalBytes'] / 3e9
    y_pos = row['BusFactor']
    ax6.annotate(row['Category'],
                 xy=(x_pos, y_pos),
                 xytext=(5, 5),
                 textcoords='offset points',
                 fontsize=8,
                 bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                 arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))

ax6.set_xlabel('Content Volume (GB)', fontsize=10, fontweight='bold')
ax6.set_ylabel('Bus Factor', fontsize=10, fontweight='bold')
ax6.set_title('Vulnerability vs Content Volume', fontsize=12, fontweight='bold')
ax6.set_ylim([1950, 2010])
ax6.grid(alpha=0.3)
ax6.legend()

plt.tight_layout()
plt.savefig('docs/wikipedia_analysis_complete.png', dpi=300, bbox_inches='tight')
print("✓ Complete visualization saved!")
print(f"\nStatistics:")
print(f"- Top {top_50_idx:,} contributors account for 50% of all bytes")
print(f"- Top {top_80_idx:,} contributors account for 80% of all bytes")
print(f"- Total contributors: {len(total_bytes_per_contributor):,}")
print(f"- Outliers labeled: {len(outliers)}")
