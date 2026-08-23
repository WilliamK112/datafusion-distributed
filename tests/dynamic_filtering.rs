#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::physical_plan::collect;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, DistributedMetricsFormat, assert_snapshot,
        display_plan_ascii, rewrite_distributed_plan_with_dynamic_filters,
        rewrite_distributed_plan_with_metrics,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn collect_left_local_dynamic_filters() -> Result<()> {
        let display = execute_local_hash_join(true, false).await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=2, partitions=6
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(key@0, RainToday@0)], projection=[]
          │     CoalescePartitionsExec
          │       [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=6, input_tasks=2
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ RainToday@19 >= No AND RainToday@19 <= Yes AND RainToday@19 IN (SET) ([<values>]) ], dynamic_rg_pruning=eligible, pruning_predicate=RainToday_null_count@1 != row_count@2 AND RainToday_max@0 >= No AND RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= Yes AND (RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= Yes AND Yes <= RainToday_max@0 OR RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= No AND No <= RainToday_max@0), required_guarantees=[RainToday in (No, Yes)]
          │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ RainToday@19 >= No AND RainToday@19 <= Yes AND RainToday@19 IN (SET) ([<values>]) ], dynamic_rg_pruning=eligible, pruning_predicate=RainToday_null_count@1 != row_count@2 AND RainToday_max@0 >= No AND RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= Yes AND (RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= Yes AND Yes <= RainToday_max@0 OR RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= No AND No <= RainToday_max@0), required_guarantees=[RainToday in (No, Yes)]
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=12
            │ BroadcastExec: input_partitions=3, consumer_tasks=2, output_partitions=6
            │   AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
            │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=2, partitions=6
              │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
              │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
              │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn partitioned_local_dynamic_filters() -> Result<()> {
        let display =
            normalize_runtime_dynamic_filters(execute_local_hash_join(false, false).await?);
        assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, RainToday@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │     [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=3
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn collect_left_union_probe_deduplicates_dynamic_filters() -> Result<()> {
        let display = execute_local_union_probe_hash_join().await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 2] => NetworkCoalesceExec: output_partitions=14, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=14
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(key@0, MinTemp@0)], projection=[]
          │     CoalescePartitionsExec
          │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=6, input_tasks=1
          │     DistributedUnionExec: t0:[c0, c2, c4] t1:[c1, c3]
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp], file_type=parquet, predicate=DynamicFilter [ MinTemp@0 >= -5.3 AND MinTemp@0 <= 20.9 AND true ], dynamic_rg_pruning=eligible, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 >= -5.3 AND MinTemp_null_count@1 != row_count@2 AND MinTemp_min@3 <= 20.9, required_guarantees=[]
          │       ProjectionExec: expr=[CAST(-1000 AS Float64) as MinTemp]
          │         PlaceholderRowExec
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp], file_type=parquet, predicate=DynamicFilter [ MinTemp@0 >= -5.3 AND MinTemp@0 <= 20.9 AND true ], dynamic_rg_pruning=eligible, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 >= -5.3 AND MinTemp_null_count@1 != row_count@2 AND MinTemp_min@3 <= 20.9, required_guarantees=[]
          │       ProjectionExec: expr=[CAST(-1001 AS Float64) as MinTemp]
          │         PlaceholderRowExec
          │       ProjectionExec: expr=[CAST(-1002 AS Float64) as MinTemp]
          │         PlaceholderRowExec
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=1, partitions=6
            │ BroadcastExec: input_partitions=3, consumer_tasks=2, output_partitions=6
            │   AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
            │     RepartitionExec: partitioning=Hash([key@0], 3), input_partitions=3
            │       AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │         DistributedLeafExec:
            │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp@0 as key], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn partitioned_dynamic_planning_executes_with_remote_consumers() -> Result<()> {
        let display =
            normalize_runtime_dynamic_filters(execute_local_hash_join(false, true).await?);
        assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 3] => NetworkCoalesceExec: output_partitions=3, input_tasks=1
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=1, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, RainToday@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │     [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=3
            │ RepartitionExec: partitioning=Hash([key@0], 3), input_partitions=3
            │   SamplerExec: partitions=3
            │     AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │       DistributedLeafExec:
            │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            │         t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=3
            │ RepartitionExec: partitioning=Hash([RainToday@0], 3), input_partitions=3
            │   SamplerExec: partitions=3
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn partitioned_remote_union_dynamic_filters() -> Result<()> {
        let display =
            normalize_runtime_dynamic_filters(execute_partitioned_remote_union_hash_join().await?);
        assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, key@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │     [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedUnionExec: t0:[c0] t1:[c1]
            │       DistributedLeafExec:
            │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            │       DistributedLeafExec:
            │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   DistributedUnionExec: t0:[c0] t1:[c1]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    /// Three union branches over two tasks force one specialized task to own two consumers of the
    /// same dynamic filter with different column remappings.
    #[tokio::test]
    async fn partitioned_remote_union_remaps_each_consumer() -> Result<()> {
        let display = execute_partitioned_remote_union_multi_consumer_hash_join().await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, key@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=1
          │     [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=1, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=6
            │   DistributedUnionExec: t0:[c0, c2] t1:[c1]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet, predicate=DynamicFilter [ CASE hash_repartition % 3 WHEN 1 THEN RainToday@19 >= No AND RainToday@19 <= No AND RainToday@19 IN (SET) ([<values>]) WHEN 2 THEN RainToday@19 >= Yes AND RainToday@19 <= Yes AND RainToday@19 IN (SET) ([Yes]) ELSE false END OR false ], dynamic_rg_pruning=eligible
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet, predicate=DynamicFilter [ CASE hash_repartition % 3 WHEN 1 THEN WindGustDir@5 >= No AND WindGustDir@5 <= No AND WindGustDir@5 IN (SET) ([<values>]) WHEN 2 THEN WindGustDir@5 >= Yes AND WindGustDir@5 <= Yes AND WindGustDir@5 IN (SET) ([Yes]) ELSE false END OR false ], dynamic_rg_pruning=eligible
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindDir9am@7 as key], file_type=parquet, predicate=DynamicFilter [ CASE hash_repartition % 3 WHEN 1 THEN WindDir9am@7 >= No AND WindDir9am@7 <= No AND WindDir9am@7 IN (SET) ([<values>]) WHEN 2 THEN WindDir9am@7 >= Yes AND WindDir9am@7 <= Yes AND WindDir9am@7 IN (SET) ([Yes]) ELSE false END OR false ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn transitive_remote_dynamic_filters() -> Result<()> {
        let display = normalize_runtime_dynamic_filters(
            execute_transitive_remote_dynamic_filter_hash_join().await?,
        );
        assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 5] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 5 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, RainToday@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │     [Stage 4] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 4 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=3
            │   HashJoinExec: mode=Partitioned, join_type=Right, on=[(key@0, WindGustDir@0)], projection=[RainToday@2]
            │     [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            │     [Stage 3] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 2 ── tasks=2, partitions=6
              │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet
              │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet
              └──────────────────────────────────────────────────
              ┌───── Stage 3 ── tasks=2, partitions=6
              │ RepartitionExec: partitioning=Hash([WindGustDir@0], 6), input_partitions=3
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir, RainToday], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
              │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir, RainToday], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    /// Consumers intentionally do not wait for a remote filter. Depending on scheduling, a task's
    /// final display report can contain its local filter, the coordinator merge, or no update. Keep
    /// the whole-plan snapshot deterministic while unit tests validate the applied predicate.
    fn normalize_runtime_dynamic_filters(display: String) -> String {
        const START: &str = "predicate=DynamicFilter [";
        const END: &str = "dynamic_rg_pruning=eligible";

        display
            .lines()
            .map(|line| {
                let (Some(start), Some(end)) = (line.find(START), line.find(END)) else {
                    return line.to_owned();
                };
                format!(
                    "{}predicate=DynamicFilter [ <runtime> ], {}",
                    &line[..start],
                    &line[end..end + END.len()]
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    async fn execute_local_hash_join(
        broadcast_joins: bool,
        dynamic_task_count: bool,
    ) -> Result<String> {
        execute_local_query(
            broadcast_joins,
            dynamic_task_count,
            false,
            r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) build
                JOIN weather probe ON build.key = probe."RainToday"
            "#,
        )
        .await
    }

    async fn execute_local_union_probe_hash_join() -> Result<String> {
        execute_local_query(
            true,
            false,
            true,
            r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "MinTemp" AS key
                    FROM weather
                ) build
                JOIN (
                    SELECT "MinTemp" FROM weather
                    UNION ALL
                    SELECT CAST(-1000.0 AS DOUBLE) AS "MinTemp"
                    UNION ALL
                    SELECT "MinTemp" FROM weather
                    UNION ALL
                    SELECT CAST(-1001.0 AS DOUBLE) AS "MinTemp"
                    UNION ALL
                    SELECT CAST(-1002.0 AS DOUBLE) AS "MinTemp"
                ) probe ON build.key = probe."MinTemp"
            "#,
        )
        .await
    }

    async fn execute_partitioned_remote_union_hash_join() -> Result<String> {
        execute_local_query(
            false,
            false,
            true,
            r#"
                WITH keys AS (
                    SELECT "RainToday" AS key FROM weather
                    UNION ALL
                    SELECT "WindGustDir" AS key FROM weather
                )
                SELECT COUNT(*)
                FROM (SELECT DISTINCT key FROM keys) build
                JOIN keys probe ON build.key = probe.key
            "#,
        )
        .await
    }

    async fn execute_partitioned_remote_union_multi_consumer_hash_join() -> Result<String> {
        execute_local_query(
            false,
            false,
            true,
            r#"
                WITH probe AS (
                    SELECT "RainToday" AS key FROM weather
                    UNION ALL
                    SELECT "WindGustDir" AS key FROM weather
                    UNION ALL
                    SELECT "WindDir9am" AS key FROM weather
                )
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) build
                JOIN probe ON build.key = probe.key
            "#,
        )
        .await
    }

    async fn execute_transitive_remote_dynamic_filter_hash_join() -> Result<String> {
        execute_local_query(
            false,
            false,
            false,
            r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) build
                JOIN (
                    SELECT probe."RainToday"
                    FROM (
                        SELECT "WindGustDir" AS key
                        FROM weather
                    ) nested_build
                    RIGHT JOIN weather probe ON nested_build.key = probe."WindGustDir"
                ) probe ON build.key = probe."RainToday"
            "#,
        )
        .await
    }

    async fn execute_local_query(
        broadcast_joins: bool,
        dynamic_task_count: bool,
        one_task_per_leaf: bool,
        sql: &str,
    ) -> Result<String> {
        let (ctx, _guard, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let mut ctx = ctx.with_distributed_broadcast_joins(broadcast_joins)?;
        ctx.set_distributed_dynamic_task_count(dynamic_task_count)?;
        if one_task_per_leaf {
            ctx = ctx.with_distributed_desired_task_count_handler(1usize);
        }
        if !broadcast_joins {
            let state = ctx.state_ref();
            let mut state = state.write();
            let optimizer = &mut state.config_mut().options_mut().optimizer;
            optimizer.hash_join_single_partition_threshold = 0;
            optimizer.hash_join_single_partition_threshold_rows = 0;
        }
        register_parquet_tables(&ctx).await?;

        let plan = ctx.sql(sql).await?.create_physical_plan().await?;

        let results_with_dynamic_filters = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_eq!(
            results_with_dynamic_filters
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );

        set_dynamic_filter_pushdown(&ctx, false);
        let plan_without_dynamic_filters = ctx.sql(sql).await?.create_physical_plan().await?;
        let results_without_dynamic_filters =
            collect(plan_without_dynamic_filters, ctx.task_ctx()).await?;
        assert_eq!(
            pretty_format_batches(&results_with_dynamic_filters)?.to_string(),
            pretty_format_batches(&results_without_dynamic_filters)?.to_string(),
        );

        let original_display = display_plan_ascii(plan.as_ref(), false);
        let plan_with_dynamic_filters =
            rewrite_distributed_plan_with_dynamic_filters(Arc::clone(&plan)).await?;
        assert_eq!(display_plan_ascii(plan.as_ref(), false), original_display);

        let plan_with_metrics = rewrite_distributed_plan_with_metrics(
            plan_with_dynamic_filters,
            DistributedMetricsFormat::Aggregated,
        )
        .await?;
        Ok(display_plan_ascii(plan_with_metrics.as_ref(), false))
    }

    fn set_dynamic_filter_pushdown(ctx: &datafusion::prelude::SessionContext, enabled: bool) {
        let state = ctx.state_ref();
        state
            .write()
            .config_mut()
            .options_mut()
            .set(
                "datafusion.optimizer.enable_dynamic_filter_pushdown",
                &enabled.to_string(),
            )
            .expect("static dynamic-filter setting should be valid");
    }
}
