# B+Tree並行制御

## 概要

B+Treeの並行制御は、複数のトランザクションが同時にインデックスにアクセスする際の一貫性と効率性を保証します。本実装では、学術的に実証されたラッチカップリング（Crab Latching）プロトコルを採用しています。

## ラッチ vs ロック

### ラッチ（Latch）
- **目的**: データ構造の物理的な一貫性を保護
- **粒度**: ページ単位
- **保持期間**: 非常に短期間（操作中のみ）
- **デッドロック**: Wait-Forグラフで検出

### ロック（Lock）
- **目的**: 論理的なトランザクション分離
- **粒度**: テーブル、行単位
- **保持期間**: トランザクション終了まで
- **デッドロック**: 2PLプロトコルで回避

## ラッチカップリング（Crab Latching）プロトコル

### 基本原理

「カニ（Crab）」のように、常に少なくとも一つの支点（ラッチ）を保持しながら木を下降します：

1. 親ノードのラッチを保持したまま子ノードのラッチを取得
2. 子ノードが「安全」であることを確認
3. 安全であれば親のラッチを解放、そうでなければ保持継続

### 安全性の定義

ノードが「安全」とは、現在の操作が親ノードに影響を与えないことを意味します：

- **挿入における安全性**: `key_count < max_keys`（分割が発生しない）
- **削除における安全性**: `key_count > min_keys`（マージが発生しない）
- **検索における安全性**: 常に安全（構造変更なし）

## 操作別詳細プロトコル

### 1. 検索操作（楽観的読み取り）

```rust
fn search(&self, key: &BTreeKey) -> Result<Vec<TupleId>> {
    // ルートから開始
    let root_latch = self.latch_manager.acquire_shared(self.root_page_id)?;
    let mut current_page_id = self.root_page_id;
    let mut current_latch = root_latch;
    
    loop {
        let page = self.buffer_pool.fetch_page(current_page_id)?;
        
        if is_leaf_page(&page) {
            // リーフノードで検索
            let results = search_in_leaf(&page, key);
            return Ok(results);
        }
        
        // 内部ノードで次のページを決定
        let next_page_id = find_child(&page, key);
        
        // 子ページのラッチを取得してから親を解放
        let next_latch = self.latch_manager.acquire_shared(next_page_id)?;
        drop(current_latch);  // 親のラッチを解放
        
        current_page_id = next_page_id;
        current_latch = next_latch;
    }
}
```

### 2. 挿入操作（悲観的書き込み）

```rust
fn insert(&mut self, key: &BTreeKey, tuple_id: TupleId) -> Result<()> {
    // ルートから排他ラッチで開始
    let mut latch_stack = vec![];
    let root_latch = self.latch_manager.acquire_exclusive(self.root_page_id)?;
    latch_stack.push((self.root_page_id, root_latch));
    
    let mut current_page_id = self.root_page_id;
    
    // リーフまで下降
    loop {
        let page = self.buffer_pool.fetch_page_write(current_page_id)?;
        
        if is_leaf_page(&page) {
            break;
        }
        
        let next_page_id = find_child(&page, key);
        let next_latch = self.latch_manager.acquire_exclusive(next_page_id)?;
        
        // 子ページが安全かチェック
        let child_page = self.buffer_pool.fetch_page(next_page_id)?;
        if is_safe_for_insert(&child_page) {
            // 安全なら祖先のラッチを全て解放
            latch_stack.clear();
        }
        
        latch_stack.push((next_page_id, next_latch));
        current_page_id = next_page_id;
    }
    
    // リーフノードに挿入
    self.insert_into_leaf(current_page_id, key, tuple_id)?;
    
    // 必要に応じて分割を上方に伝播
    if needs_split(current_page_id) {
        self.split_and_propagate(current_page_id, &mut latch_stack)?;
    }
    
    Ok(())
}
```

### 3. 削除操作（悲観的書き込み）

```rust
fn delete(&mut self, key: &BTreeKey, tuple_id: TupleId) -> Result<()> {
    // 挿入と同様の悲観的プロトコル
    let mut latch_stack = vec![];
    let root_latch = self.latch_manager.acquire_exclusive(self.root_page_id)?;
    latch_stack.push((self.root_page_id, root_latch));
    
    // リーフまで下降（安全性チェック付き）
    let leaf_page_id = self.find_leaf_with_latches(key, &mut latch_stack)?;
    
    // リーフから削除
    self.delete_from_leaf(leaf_page_id, key, tuple_id)?;
    
    // 必要に応じてマージ/再分配を上方に伝播
    if needs_merge(leaf_page_id) {
        self.merge_or_redistribute(leaf_page_id, &mut latch_stack)?;
    }
    
    Ok(())
}
```

## 兄弟ポインタの並行制御

### 範囲スキャン時の課題

範囲スキャン中に並行して分割やマージが発生すると、以下の問題が生じる可能性があります：

1. **Lost Update**: スキャン中のページが分割され、一部のエントリを見逃す
2. **Phantom Read**: 新しいエントリが挿入され、同じ範囲で異なる結果
3. **Broken Chain**: 兄弟ポインタの更新中に不整合

### 解決策：ラッチカップリング on 兄弟

```rust
pub struct BTreeIterator {
    current_page_id: Option<PageId>,
    current_latch: Option<LatchGuard>,
    current_slot: usize,
    end_key: Option<BTreeKey>,
}

impl BTreeIterator {
    fn move_to_next_page(&mut self) -> Result<bool> {
        let current_page = self.get_current_page()?;
        let next_page_id = current_page.get_next_page_id();
        
        if let Some(next_id) = next_page_id {
            // 次のページのラッチを取得してから現在のを解放
            let next_latch = self.latch_manager.acquire_shared(next_id)?;
            self.current_latch = Some(next_latch);
            self.current_page_id = Some(next_id);
            self.current_slot = 0;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
```

### 分割時の兄弟ポインタ更新

```rust
fn split_leaf_page(&mut self, page_id: PageId) -> Result<(PageId, BTreeKey)> {
    // 1. 現在のページと右隣のページの両方をラッチ
    let current_latch = self.latch_manager.acquire_exclusive(page_id)?;
    let old_next = get_next_page_id(page_id);
    let next_latch = old_next.map(|id| 
        self.latch_manager.acquire_exclusive(id)
    ).transpose()?;
    
    // 2. 新しいページを作成
    let (new_page_id, new_page) = self.create_new_page()?;
    
    // 3. データを分割
    let middle_key = self.split_data(page_id, new_page_id)?;
    
    // 4. ポインタを更新（アトミックに）
    // current -> new_page -> old_next
    set_next_page_id(page_id, Some(new_page_id));
    set_prev_page_id(new_page_id, Some(page_id));
    set_next_page_id(new_page_id, old_next);
    if let Some(next_id) = old_next {
        set_prev_page_id(next_id, Some(new_page_id));
    }
    
    // 5. ラッチを解放
    drop(next_latch);
    drop(current_latch);
    
    Ok((new_page_id, middle_key))
}
```

## デッドロック検出と回避

### Wait-Forグラフ

```rust
pub struct WaitForGraph {
    // トランザクションID -> 待機中のページID
    waiting_for: DashMap<TransactionId, PageId>,
    // ページID -> 保持しているトランザクションID
    held_by: DashMap<PageId, Vec<TransactionId>>,
}

impl WaitForGraph {
    pub fn detect_cycle(&self, txn_id: TransactionId) -> bool {
        let mut visited = HashSet::new();
        let mut stack = vec![txn_id];
        
        while let Some(current) = stack.pop() {
            if !visited.insert(current) {
                return true;  // サイクル検出
            }
            
            if let Some(waiting_page) = self.waiting_for.get(&current) {
                if let Some(holders) = self.held_by.get(&waiting_page) {
                    stack.extend(holders.value());
                }
            }
        }
        
        false
    }
}
```

### デッドロック回避戦略

1. **ラッチ順序の統一**
   - 常にPageIdの昇順でラッチを取得
   - レベル内では左から右へ
   - 上位レベルから下位レベルへ

2. **タイムアウト**
   ```rust
   const LATCH_TIMEOUT: Duration = Duration::from_millis(50);
   
   match latch_manager.try_acquire_exclusive_timeout(page_id, LATCH_TIMEOUT) {
       Ok(latch) => proceed(),
       Err(TimeoutError) => abort_and_retry(),
   }
   ```

3. **No-Wait戦略（楽観的）**
   - ラッチが取得できない場合は即座に諦める
   - 上位層で再試行

## パフォーマンス最適化

### 1. 楽観的下降（Optimistic Descent）

読み取り専用の操作では、より楽観的なアプローチが可能：

```rust
fn optimistic_search(&self, key: &BTreeKey) -> Result<Option<TupleId>> {
    loop {
        // バージョン番号を記録
        let root_version = self.get_root_version();
        
        // ラッチなしで下降
        let leaf_page_id = self.find_leaf_optimistic(key)?;
        
        // リーフでのみラッチを取得
        let leaf_latch = self.latch_manager.acquire_shared(leaf_page_id)?;
        
        // バージョンが変わっていなければ成功
        if self.get_root_version() == root_version {
            return search_in_leaf(leaf_page_id, key);
        }
        
        // 変わっていれば再試行
        drop(leaf_latch);
    }
}
```

### 2. ラッチのアップグレード/ダウングレード

```rust
impl LatchGuard {
    // 共有ラッチから排他ラッチへ
    pub fn upgrade(&mut self) -> Result<()> {
        // アトミックにアップグレード試行
        // 失敗したら一旦解放して再取得
    }
    
    // 排他ラッチから共有ラッチへ
    pub fn downgrade(&mut self) {
        // 常に成功
    }
}
```

### 3. Blink-Tree手法

将来の拡張として、Blink-Tree（高リンクB+Tree）の実装も検討：

- 各ノードに「高リンク」（右兄弟への追加ポインタ）を追加
- 分割中でも検索が可能
- より高い並行性を実現

## 統計情報とモニタリング

```rust
pub struct LatchStatistics {
    pub total_acquisitions: AtomicU64,
    pub total_waits: AtomicU64,
    pub total_wait_time: AtomicU64,
    pub deadlock_count: AtomicU64,
    pub timeout_count: AtomicU64,
    
    // ページ別の統計
    pub page_stats: DashMap<PageId, PageLatchStats>,
}

pub struct PageLatchStats {
    pub shared_acquisitions: u64,
    pub exclusive_acquisitions: u64,
    pub avg_hold_time: Duration,
    pub contention_count: u64,
}
```

これらの統計情報は、ホットスポットの特定やパフォーマンスチューニングに活用できます。