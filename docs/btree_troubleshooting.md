# B+Treeトラブルシューティングガイド

## よくある問題と解決方法

### 1. デッドロック関連

#### 症状
- 複数のスレッドがハングアップし、進行しない
- `LatchTimeout`エラーが頻発
- アプリケーションが応答しなくなる

#### 原因
1. **ラッチ取得順序の不一致**
   ```rust
   // 悪い例：異なる順序でラッチを取得
   Thread1: latch(PageId(10)) -> latch(PageId(20))
   Thread2: latch(PageId(20)) -> latch(PageId(10))
   ```

2. **長時間のラッチ保持**
   ```rust
   // 悪い例：I/O操作中にラッチを保持
   let latch = acquire_exclusive(page_id)?;
   perform_heavy_computation();  // ラッチを保持したまま
   ```

#### 解決方法
```rust
// 1. 常に同じ順序でラッチを取得
let pages = vec![page1, page2].sort();
for page in pages {
    acquire_latch(page)?;
}

// 2. ラッチの保持時間を最小化
let data = {
    let latch = acquire_shared(page_id)?;
    read_data()  // ラッチはスコープを抜けると自動解放
};
process_data(data);  // ラッチ解放後に処理

// 3. デッドロック検出の有効化
let latch_manager = LatchManager::new()
    .with_deadlock_detection(true)
    .with_timeout(Duration::from_millis(100));
```

### 2. ページ分割の失敗

#### 症状
- `PageSplitError`の発生
- インデックスの不整合
- 挿入操作の失敗

#### 原因と診断
```rust
// 診断ツールを使用
let diagnostics = btree.diagnose_page(page_id)?;
println!("Page fill factor: {:.2}%", diagnostics.fill_factor * 100.0);
println!("Can split: {}", diagnostics.can_split);
println!("Parent has space: {}", diagnostics.parent_has_space);
```

#### 解決方法
1. **ルート分割の特別処理**
   ```rust
   if is_root(page_id) && needs_split(page_id) {
       // 新しいルートを作成
       let new_root = create_new_root()?;
       // 既存のルートを子として設定
       split_root(old_root, new_root)?;
   }
   ```

2. **再帰的な分割の処理**
   ```rust
   fn insert_with_split(&mut self, page_id: PageId, key: &BTreeKey) -> Result<()> {
       if !has_space(page_id) {
           let (new_page, split_key) = split_page(page_id)?;
           // 親に再帰的に挿入
           self.insert_with_split(parent_id, &split_key)?;
       }
       insert_into_page(page_id, key)
   }
   ```

### 3. パフォーマンスの問題

#### 症状
- 検索・挿入が遅い
- CPU使用率が高い
- メモリ使用量が増大

#### 診断方法
```rust
// パフォーマンス統計の取得
let stats = btree.get_performance_stats()?;
println!("Average search time: {:?}", stats.avg_search_time);
println!("Average insert time: {:?}", stats.avg_insert_time);
println!("Page cache hit rate: {:.2}%", stats.cache_hit_rate * 100.0);
println!("Tree height: {}", stats.tree_height);

// ホットページの特定
let hot_pages = btree.identify_hot_pages(top_n: 10)?;
for (page_id, access_count) in hot_pages {
    println!("Page {:?}: {} accesses", page_id, access_count);
}
```

#### 最適化方法

1. **ページサイズの調整**
   ```rust
   // より大きなページサイズで再構築
   const LARGE_PAGE_SIZE: usize = 16384;  // 16KB
   let btree = BTree::create_with_page_size(buffer_pool, root, schema, LARGE_PAGE_SIZE)?;
   ```

2. **バッファプールのチューニング**
   ```rust
   // ホットページ用の専用バッファプール
   let hot_buffer_pool = BufferPoolManager::new(
       page_manager,
       Box::new(LruReplacer::new(1000)),  // 1000ページ
       1000
   );
   ```

3. **インデックスの再構築**
   ```rust
   // 断片化したインデックスの再構築
   let entries = btree.dump_all_entries()?;
   btree.truncate()?;
   btree.bulk_load(entries)?;
   ```

### 4. データ不整合

#### 症状
- 検索で期待した結果が返らない
- 重複キーエラーが発生するが実際には重複していない
- 範囲検索で一部のデータが欠落

#### 診断ツール

```rust
// 1. インデックス整合性チェック
let validation_report = btree.validate_full()?;
if !validation_report.is_valid {
    println!("Validation errors:");
    for error in validation_report.errors {
        println!("  - {}", error);
    }
}

// 2. ページレベルの検証
fn validate_page(page_id: PageId) -> Result<PageValidation> {
    let page = fetch_page(page_id)?;
    
    // キーの順序をチェック
    let keys = extract_keys(&page);
    for i in 1..keys.len() {
        if keys[i-1] >= keys[i] {
            return Err(anyhow!("Keys not in order at position {}", i));
        }
    }
    
    // 親子関係の検証
    if let Some(parent_id) = page.parent_page_id {
        validate_parent_child_relationship(parent_id, page_id)?;
    }
    
    Ok(PageValidation { valid: true })
}

// 3. 詳細なダンプ
btree.dump_tree_structure("btree_dump.txt")?;
```

#### 修復方法

1. **部分的な修復**
   ```rust
   // 特定のページの修復
   btree.repair_page(corrupted_page_id)?;
   
   // 兄弟ポインタの修復
   btree.repair_sibling_pointers()?;
   ```

2. **完全な再構築**
   ```rust
   // 全データをスキャンして再構築
   let mut all_entries = Vec::new();
   
   // リーフページを直接スキャン（インデックス構造を信用しない）
   for page_id in 0..max_page_id {
       if let Ok(page) = fetch_page_direct(page_id) {
           if is_leaf_page(&page) {
               all_entries.extend(extract_entries(&page)?);
           }
       }
   }
   
   // 新しいインデックスに再構築
   let new_btree = BTree::create(buffer_pool, new_root, schema)?;
   new_btree.bulk_load(&mut all_entries)?;
   ```

### 5. 並行性の問題

#### 症状
- Phantom Read（範囲検索で異なる結果）
- Lost Update（更新が失われる）
- ラッチ競合によるスループット低下

#### 解決方法

1. **スナップショット分離**
   ```rust
   // MVCC（Multi-Version Concurrency Control）との統合
   let snapshot = btree.create_snapshot()?;
   let results = snapshot.range_scan(start, end)?;
   ```

2. **楽観的並行制御**
   ```rust
   loop {
       let version = btree.get_version();
       let results = btree.search_optimistic(&key)?;
       
       if btree.validate_version(version) {
           return Ok(results);
       }
       // バージョンが変わっていたら再試行
   }
   ```

## デバッグツールとコマンド

### 1. 環境変数によるデバッグ

```bash
# 詳細なログを有効化
export RUST_LOG=vibedb::access::btree=debug

# ラッチのトレースを有効化
export VIBEDB_TRACE_LATCHES=1

# ページアクセスの統計を有効化
export VIBEDB_PAGE_STATS=1
```

### 2. 実行時診断コマンド

```rust
// インタラクティブなデバッガ
let mut debugger = BTreeDebugger::new(&btree);

// ページの内容を表示
debugger.show_page(PageId(10))?;

// ツリー構造の可視化
debugger.visualize_tree()?;

// 特定のキーの検索パスをトレース
debugger.trace_search(&key)?;

// Output:
// Searching for key: 42
// [1] Root (PageId(0)): keys=[100, 200], taking child[0]
// [2] Internal (PageId(5)): keys=[50, 75], taking child[0]  
// [3] Leaf (PageId(10)): keys=[10, 20, 30, 42, 48], found at position 3
```

### 3. パフォーマンスプロファイリング

```rust
// プロファイラの起動
let profiler = BTreeProfiler::new();
btree.attach_profiler(profiler);

// 操作の実行
perform_operations();

// レポートの生成
let report = profiler.generate_report();
println!("{}", report);

// Output:
// B+Tree Performance Report
// ========================
// Total operations: 10000
// 
// Operation breakdown:
// - Search: 5000 (avg: 120μs, p99: 500μs)
// - Insert: 3000 (avg: 450μs, p99: 2ms)
// - Delete: 2000 (avg: 380μs, p99: 1.5ms)
// 
// Page access statistics:
// - Total page accesses: 45000
// - Cache hit rate: 87.3%
// - Average pages per operation: 4.5
// 
// Latch statistics:
// - Total latch acquisitions: 35000
// - Average wait time: 15μs
// - Contention rate: 2.1%
```

## よくあるエラーメッセージと対処法

### `LatchTimeout: Failed to acquire latch on PageId(123) after 100ms`
- **原因**: デッドロックまたは長時間のラッチ保持
- **対処**: タイムアウト値の調整、デッドロック検出の有効化

### `PageSplitError: No space in parent for split key`
- **原因**: 親ページも満杯で再帰的な分割が必要
- **対処**: 適切な再帰的分割の実装を確認

### `KeyNotFound: Key does not exist in page`
- **原因**: インデックスの不整合または並行性の問題
- **対処**: インデックスの検証と修復

### `CorruptedPage: Invalid page header at PageId(456)`
- **原因**: ディスクI/Oエラーまたはメモリ破損
- **対処**: チェックサムの検証、ページの再読み込み

## 予防的メンテナンス

### 定期的な検証
```rust
// 日次でインデックスの整合性をチェック
schedule_daily_task(|| {
    let report = btree.validate_full()?;
    if !report.is_valid {
        alert_admin("B+Tree validation failed", &report);
    }
});
```

### 統計情報の監視
```rust
// メトリクスの収集と監視
let metrics = btree.collect_metrics()?;
prometheus::gauge!("btree_height", metrics.height as f64);
prometheus::gauge!("btree_fill_factor", metrics.avg_fill_factor);
prometheus::counter!("btree_page_splits_total", metrics.page_splits);
```

### 自動最適化
```rust
// フラグメンテーションの自動検出と対処
if btree.get_fragmentation_ratio()? > 0.3 {
    schedule_maintenance_window(|| {
        btree.defragment()?;
    });
}
```