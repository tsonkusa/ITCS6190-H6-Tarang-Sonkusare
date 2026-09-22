# Hands-on L6: Report

**Name:** Tarang Sonkusare
**Student ID:** 801352372
**Email:** tsonkusa@charlotte.edu

---

## Seed and commands

Seed used for `datagen.py`: `801352372`. The generator wrote 1,000 listening logs and 50 songs.

```bash
python3 datagen.py 801352372
ls shared-folder/input
docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output
find shared-folder/output -name 'part-*.csv' -print
```

I first ran the starter file and observed that `timestamp` and `duration_sec` were strings. I added the explicit schema, then implemented the four tasks one at a time. After each edit, I repeated the `docker cp` and `spark-submit` commands. For UI inspection, I temporarily paused the final run before `spark.stop()` and then removed the pause from `main.py`.

---

## Results

### Task 1: favorite genre per user

First ten rows:

```text
user_id,genre,play_count
user_1,Classical,5
user_10,Jazz,7
user_100,Classical,6
user_11,Pop,9
user_12,Pop,6
user_13,Classical,7
user_14,Pop,6
user_15,Classical,7
user_16,Rock,10
user_17,Jazz,4
```

Among these first ten users in user_id order, Classical appears four times and Pop three times as the favorite genre. user_16 has the largest favorite-genre count in this sample, with 10 Rock plays.

### Task 2: average listening time per song

First ten rows:

```text
song_id,title,avg_duration_sec,play_count
song_39,Title_song_39,206.42,12
song_12,Title_song_12,194.71,21
song_19,Title_song_19,190.92,13
song_35,Title_song_35,187.83,12
song_45,Title_song_45,178.05,21
song_36,Title_song_36,177.5,16
song_46,Title_song_46,177.17,18
song_42,Title_song_42,176.4,15
song_8,Title_song_8,176.3,27
song_40,Title_song_40,174.11,19
```

`song_39` has the longest average listening time, 206.42 seconds across 12 plays. Among these ten songs, `song_8` has the most plays, 27, although it ranks ninth by average listening time.

### Task 3: genre loyalty score, top 10

```text
user_id,genre,play_count,total_plays,loyalty_score
user_95,Pop,8,8,1.0
user_3,Jazz,6,6,1.0
user_87,Hip-Hop,6,6,1.0
user_91,Classical,6,6,1.0
user_73,Jazz,5,5,1.0
user_33,Jazz,11,12,0.917
user_57,Classical,10,11,0.909
user_35,Jazz,9,10,0.9
user_26,Hip-Hop,8,9,0.889
user_71,Classical,8,9,0.889
```

Five users have a loyalty score of 1.0 because every recorded play was in their favorite genre. `user_33` is next, with 11 of 12 plays in Jazz. Users with few plays can reach 1.0 more easily; I would keep the specified score but display `total_plays` alongside it, or require a minimum play count when comparing users.

### Task 4: night owls

First ten rows:

```text
user_id,night_plays
user_89,8
user_20,7
user_82,6
user_60,5
user_94,5
user_11,4
user_12,4
user_18,4
user_25,4
user_29,4
```

`user_89` has the most plays between midnight and 5 AM, with eight. `user_20` follows with seven; five users in this top ten have four such plays.

---

## The plan

Task 1's `explain()` output:

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [user_id#0 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(user_id#0 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=975]
      +- Project [user_id#0, genre#7, play_count#28L]
         +- Filter (rank#38 = 1)
            +- Window [row_number() windowspecdefinition(user_id#0, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#38], [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST]
               +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Final
                  +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(user_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=968]
                        +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Partial
                           +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                              +- HashAggregate(keys=[user_id#0, genre#7], functions=[count(1)])
                                 +- Exchange hashpartitioning(user_id#0, genre#7, 200), ENSURE_REQUIREMENTS, [plan_id=962]
                                    +- HashAggregate(keys=[user_id#0, genre#7], functions=[partial_count(1)])
                                       +- Project [user_id#0, genre#7]
                                          +- BroadcastHashJoin [song_id#1], [song_id#4], Inner, BuildRight, false, false
                                             :- Filter isnotnull(song_id#1)
                                             :  +- FileScan csv [user_id#0,song_id#1] Batched: false, DataFilters: [isnotnull(song_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/listening_logs.csv], PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<user_id:string,song_id:string>
                                             +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=957]
                                                +- Filter isnotnull(song_id#4)
                                                   +- FileScan csv [song_id#4,genre#7] Batched: false, DataFilters: [isnotnull(song_id#4)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/songs_metadata.csv], PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<song_id:string,genre:string>
```

The two `FileScan csv` operators read the listening logs and song metadata. Spark chose an **inner `BroadcastHashJoin`** on `song_id`, broadcasting the smaller songs table on the right (`BuildRight`). `BroadcastExchange` prepares that table for broadcast to the join workers. `HashAggregate` counts plays by `(user_id, genre)`; its `Exchange hashpartitioning(user_id, genre, 200)` brings matching groups together. The next `Exchange hashpartitioning(user_id, 200)` brings each user's genre counts together for `row_number()` and the window limit. Finally, `Exchange rangepartitioning(user_id ..., 200)` supports the final global `user_id` ordering. The SQL / DataFrame diagram for Task 1's `showString` query showed the same two scans, broadcast join, aggregation, window operations, and exchanges.

---

## Transformations and actions

The DataFrame joins, filters, `groupBy`, `agg`, column expressions, window ranking, `orderBy`, and `limit` define transformations. The actions in `main.py` are `logs.count()` and `songs.count()` on the row-count line, plus `df.show(20, truncate=False)` and the CSV write inside `save()` for each of the four tasks. That is two explicit count calls, four `show()` calls, and four writes. `printSchema()` and `favorite.explain()` display schema and plan information without executing the Task 1 query.

The Spark UI Jobs tab showed **40 completed jobs**, numbered 0 through 39. Its SQL / DataFrame tab showed ten completed executions: the two counts, then a `showString` and a CSV write for each task. For example, Task 1's `showString` execution launched jobs 4–7 and its CSV write launched jobs 8–13. The job count is greater than the ten action calls because Spark can launch multiple jobs for one action while preparing broadcast data, shuffles, and output.

---

## Problems and fixes

No error stopped the completed run. The starter file initially showed `timestamp: string` and `duration_sec: string` because `logs_schema` was `None`. Defining `logs_schema` with `TimestampType()` and `IntegerType()` corrected the printed schema. All four tasks subsequently ran and wrote their CSV outputs.