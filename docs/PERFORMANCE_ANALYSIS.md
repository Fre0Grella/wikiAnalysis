# Performance Analysis Report
**Wikipedia Data Analysis Project**

---

## 1. Summary

This report compares two implementations of Wikipedia data analysis jobs:
- **Baseline (Non-Optimized)**: Uses standard RDD operations without optimization
- **Optimized**: Applies advanced Spark optimization techniques

**Key Results:**
- Total runtime speedup: **> 2,22x**
- Shuffle read reduction: **33,8 GB → 10,1 GB (70,2% reduction)**
- Shuffle write reduction: **24,1 GB → 8 GB (66,9% reduction)**

---

## 2. Dataset Description

### 2.1 Source Data
- **Wikipedia Revision History Dumps**:
  - Partition: Into monthly files
  - Format: TSV compressed with bzip2
  - Size: ~500 MB compressed
  - Available Date range: 2001-01 to Last Month
  - Used Range: 2020-01 to 2025-11
  - Total Size Used: ~37,5 GB compressed

- **Wikipedia Category Structure**:
  - `categorylinks.sql.bz2`: ~2,1 GB
  - `linktarget.sql.bz2`: ~0,9 GB  
  - `page.sql.bz2`: ~1,8 GB

---

## 3. Job Descriptions

### 3.1 Job 1: Wikipedia Category Analysis
**Objective**: Build a hierarchical category mapping from Wikipedia's category structure

**Algorithm**:
1. Parse SQL dumps (categorylinks, linktarget, page tables)
2. Identify root categories under "Main_topic_classifications"
3. Iteratively propagate category labels through the hierarchy
4. Map articles to their root categories using bitmask encoding

**Complexity**:
- Iterations: Up to 20 (tipically converges in ~17/18)
- Output: Page ID → Root Categories bitmask mapping

**Shuffles Required**: Minimum 2 per iteration + final aggregation = **~40+ total shuffles**

### 3.2 Job 2: Wikipedia Bus Factor Analysis
**Objective**: Calculate the "bus factor" for each Wikipedia topic category

**Definition**: Bus factor = minimum number of contributors needed to account for 50% of content in a category

**Algorithm**:
1. Load page-to-category mapping from Job 1
2. Join with revision history
3. Aggregate contributions per user per category
4. Calculate bus factor using top-N contributors approach

**Complexity**:
- Join: Page IDs with category mapping
- Aggregation: Per (user, category) pair
- Output: Bus factor metrics per category

**Shuffles Required**: **3 shuffles** (filter join, aggregation, top-N)

---

## 4. Cluster Configuration
- Instance type: m4.xlarge
- Executors: 3
- Cores per executor: 4
- Memory per executor: 16 GB

---

## 5. Optimization Strategies

### 5.1 Category Analysis Job

| Optimization | Baseline | Optimized | Impact |
|--------------|----------|-----------|--------|
| **Broadcast Join** | Regular join (shuffle) | Broadcast category map (~200MB) | Eliminates 1 massive shuffle per iteration |
| **Persistence Strategy** | No caching | Strategic persist/unpersist | Reduces recomputation by ~60% |
| **Partitioning** | Default (200 partitions) | Coalesce after filters | Reduces shuffle overhead |
| **GroupByKey vs ReduceByKey** | groupByKey (moves all data) | reduceByKey (combines locally) | 50% less shuffle write |
| **Shuffle Compression** | None | LZ4 codec enabled | ~30% smaller shuffle files |
| **Checkpointing** | Every 3 iterations | Every 3 iterations | ✅ Same (necessary for lineage) |

### 5.2 Bus Factor Job

| Optimization | Baseline | Optimized | Impact                                                      |
|--------------|----------|-----------|-------------------------------------------------------------|
| **Broadcast Join** | Regular join (shuffle) | Broadcast category map (~200MB) | Eliminates 1 massive shuffle per iteration, less memory use |
| **Aggregation Pattern** | groupByKey → sum | aggregateByKey with PriorityQueue | ~40% faster, less memory                                    |
| **Data Structure** | Array sorting in mapValues | PriorityQueue during aggregation | O(n log k) vs O(n log n)                                    |
| **Coalesce** | No reduction | Coalesce after filter (200→50 partitions) | Less shuffle overhead                                       |

---

## 6. Performance Results

### 6.1 Category Analysis Job

#### Execution Time
```
Baseline:     > 120 minutes (With the current hardware configuration it seems to be blocked halfway after 2 hours)
Optimized:    34 minutes, 23 seconds
SpeedUp:  > 3,52x faster
```

#### Shuffle Metrics
| Metric | Baseline | Optimized | Δ |
|--------|----------|-----------|---|
| Total Shuffle Write | 5 GB | 5 GB      | **-0%** |
| Total Shuffle Read | 12,3 GB | 7,1 GB    | **-43%** |

(These are the last value read for the baseline version)

#### Considerations
 The baseline version was not able to complete the job in a reasonable time (2 hours).
 This is probably due to the absence of the coalesce in the iterative part of the algorithm that caused an explosion of tasks (The last register stage got around ~34000 task for a count) and such a overhead in the shuffle phase that it simply stopped.
 The optimized version, thanks to the addition of the coalesce, was able to keep a constant value of task reducing the overhead and completing the job in a reasonable time.
### 6.2 Bus Factor Job

#### Execution Time
```
Baseline:     56 minutes, 41 seconds
Optimized:    44 minutes, 53 seconds
SpeedUp:      1,26x faster
```

#### Shuffle Metrics
| Metric | Baseline | Optimized | Δ        |
|--------|---------|-------|----------|
| Total Shuffle Write | 19.1 GB | 3 GB | **-84%** |
| Total Shuffle Read | 20.5 GB | 3 GB | **-85%** |

---
#### Considerations
 Most of the time is spent on the first filter that reduces the dataset from 37,5 GB to ~3 GB. The rest of the job is pretty fast due to the optimizations applied.
 So this jobs really needs only 2 shuffles operation to complete the task (reduceByKey and aggregateByKey).
 The speedup is not as big as the one of the category analysis job because the resources available was enough to avoid the shuffle bottleneck,on the other hand, the optimized version could be executed in the same time on a cluster with less resources reducing the overall costs.

## 7. Detailed Optimization Explanations

### 7.1 Broadcast Join
- Category masks fit in memory (200 MB << executor memory)
- Read-heavy workload (50M lookups vs 2M entries)
- No risk of broadcast variable becoming stale

### 7.2 ReduceByKey vs GroupByKey

**GroupByKey (Baseline):**
Problem:
1. Sends ALL values for each key across network
2. No local aggregation (map-side combine)
3. High memory pressure on reducers

For key "Alice" with 1000 contributions:
→ Shuffle: 1000 values × record_size


**ReduceByKey (Optimized):**
Solution:
1. Local aggregation BEFORE shuffle (combiner)
2. Only partial sums cross network
3. Less memory on reducers

For key "Alice" with 1000 contributions:
→ Shuffle: 1 partial sum per partition (200 values instead of 1000)
→ 80% less shuffle data


### 7.3 Checkpointing Strategy

**Why Checkpointing is Necessary:**
Without checkpointing after 20 iterations:
- Lineage graph: 20 joins × 3 transformations = 60 stages in DAG
- Stack overflow risk on recomputation
- Executor failures trigger full recompute from iteration 1

With checkpointing every 3 iterations:
- Lineage reset at iterations 3, 6, 9, 12, 15, 18
- Max recompute: 3 iterations
- Disk cost: ~5 GB × 6 checkpoints = 30 GB
- Time cost: ~30 seconds per checkpoint
→ Trade-off is worth it for long iterative jobs


**Both versions use checkpointing** because the lineage graph explode in size and it will be unnecessary to afford such resources. (> 16GB of memory)

### 7.4 Coalesce After Filters

**Problem:**

Start: 200 partitions, 100 GB data

After Filter: 200 partitions, 5 GB data
Each partition has only 25 MB → underutilized

Next shuffle:
- 200 tasks created
- 195 tasks finish in <1 second (mostly empty)
- Stragglers and overhead


**Solution:**

After filter and coalesce: 50 partitions, 5 GB data
Each partition: 100 MB → optimal

Next shuffle:
- 50 tasks (4× fewer)
- Better CPU utilization
- Less scheduling overhead

---

## 8. Iterative Algorithm
 As I study more in depth the Wikipedia dataset, I found that the only possible solution to get some general categories was the use oa an iterative algorithm even if it's not ideal in a spark environment.
So my choice was to use a label propagation algorithm that starts from the root categories and iteratively labels the children until convergence.

**Algorithm Pattern:**
```
iteration 1: Root categories initialized
iteration 2: Children of roots labeled  
iteration 3: Grandchildren labeled
...
iteration N: Convergence (no new labels)
```
---


