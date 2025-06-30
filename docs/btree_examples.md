# B+Treeインデックス使用例

## 基本的な使用方法

### インデックスの作成

```rust
use vibedb::access::{BTree, BTreeKey};
use vibedb::access::{DataType, Value};
use vibedb::storage::BufferPoolManager;

// 単一カラムインデックスの作成
let schema = vec![DataType::Int32];
let btree = BTree::create(buffer_pool.clone(), root_page_id, schema)?;

// 複合インデックスの作成（例：user_id, timestamp）
let schema = vec![DataType::Int32, DataType::Int64];
let btree = BTree::create(buffer_pool.clone(), root_page_id, schema)?;

// 文字列を含むインデックス
let schema = vec![DataType::Varchar, DataType::Int32];
let btree = BTree::create(buffer_pool.clone(), root_page_id, schema)?;
```

### データの挿入

```rust
// 単一キーの挿入
let key = BTreeKey::from_values(&[Value::Int32(42)], &schema)?;
let tuple_id = TupleId::new(PageId(10), 5);
btree.insert(&key, tuple_id)?;

// 複合キーの挿入
let key = BTreeKey::from_values(&[
    Value::Int32(100),          // user_id
    Value::Int64(1234567890),   // timestamp
], &schema)?;
btree.insert(&key, TupleId::new(PageId(20), 10))?;

// NULL値を含むキーの挿入
let key = BTreeKey::from_values(&[
    Value::String("Alice".to_string()),
    Value::Null,  // NULLは最小値として扱われる
], &schema)?;
btree.insert(&key, TupleId::new(PageId(30), 15))?;
```

### 検索操作

```rust
// 完全一致検索
let search_key = BTreeKey::from_values(&[Value::Int32(42)], &schema)?;
let tuple_ids = btree.search(&search_key)?;

for tuple_id in tuple_ids {
    println!("Found tuple at: {:?}", tuple_id);
    // 実際のタプルデータの取得
    let tuple = table_heap.get(tuple_id)?;
}

// 複合キーでの検索
let search_key = BTreeKey::from_values(&[
    Value::Int32(100),
    Value::Int64(1234567890),
], &schema)?;
let results = btree.search(&search_key)?;
```

### 範囲検索

```rust
// 10 <= key <= 50 の範囲検索
let start_key = BTreeKey::from_values(&[Value::Int32(10)], &schema)?;
let end_key = BTreeKey::from_values(&[Value::Int32(50)], &schema)?;

let mut iter = btree.range_scan(Some(&start_key), Some(&end_key))?;
while let Some((key, tuple_id)) = iter.next()? {
    let values = key.to_values()?;
    println!("Key: {:?}, TupleId: {:?}", values, tuple_id);
}

// 開区間での範囲検索（key > 10）
let start_key = BTreeKey::from_values(&[Value::Int32(10)], &schema)?;
let mut iter = btree.range_scan_exclusive(Some(&start_key), None)?;

// プレフィックス検索（複合キーの場合）
// user_id = 100 の全てのエントリを取得
let prefix_key = BTreeKey::from_values(&[Value::Int32(100)], &[DataType::Int32])?;
let mut iter = btree.prefix_scan(&prefix_key)?;
```

### 削除操作

```rust
// 特定のキーとタプルの削除
let key = BTreeKey::from_values(&[Value::Int32(42)], &schema)?;
let tuple_id = TupleId::new(PageId(10), 5);
btree.delete(&key, tuple_id)?;

// キーに関連する全てのタプルを削除
let key = BTreeKey::from_values(&[Value::Int32(42)], &schema)?;
let tuple_ids = btree.search(&key)?;
for tuple_id in tuple_ids {
    btree.delete(&key, tuple_id)?;
}
```

## 高度な使用例

### バルクローディング

大量のデータを効率的に挿入する場合：

```rust
// データを事前にソート
let mut entries: Vec<(BTreeKey, TupleId)> = vec![];
for i in 0..1000000 {
    let key = BTreeKey::from_values(&[Value::Int32(i)], &schema)?;
    let tuple_id = TupleId::new(PageId(i / 100), (i % 100) as u16);
    entries.push((key, tuple_id));
}

// バルクロード（ボトムアップ構築）
btree.bulk_load(&mut entries)?;
```

### トランザクション内での使用

```rust
// トランザクション開始
let txn = TransactionManager::begin()?;

// インデックス操作
let key = BTreeKey::from_values(&[Value::Int32(100)], &schema)?;
btree.insert_txn(&txn, &key, tuple_id)?;

// 他の操作...

// コミット
txn.commit()?;
```

### 統計情報の取得

```rust
// インデックスの統計情報
let stats = btree.get_statistics()?;
println!("Total keys: {}", stats.total_keys);
println!("Total pages: {}", stats.total_pages);
println!("Height: {}", stats.height);
println!("Average fill factor: {:.2}%", stats.avg_fill_factor * 100.0);

// ページレベルの詳細情報
let page_info = btree.analyze_page(page_id)?;
println!("Page type: {:?}", page_info.page_type);
println!("Key count: {}", page_info.key_count);
println!("Free space: {} bytes", page_info.free_space);
```

## 実践的なユースケース

### 1. ユーザーテーブルのインデックス

```rust
// ユーザーテーブル: (id, name, email, created_at)
// emailカラムに一意インデックスを作成

let email_index_schema = vec![DataType::Varchar];
let email_index = BTree::create(buffer_pool.clone(), email_idx_root, email_index_schema)?;

// ユーザー登録時
fn register_user(name: &str, email: &str) -> Result<()> {
    // メールアドレスの重複チェック
    let email_key = BTreeKey::from_values(&[Value::String(email.to_string())], &schema)?;
    if !email_index.search(&email_key)?.is_empty() {
        return Err(anyhow!("Email already exists"));
    }
    
    // ユーザーデータを挿入
    let user_id = generate_user_id();
    let tuple_id = users_table.insert_user(user_id, name, email)?;
    
    // インデックスを更新
    email_index.insert(&email_key, tuple_id)?;
    
    Ok(())
}
```

### 2. 時系列データのインデックス

```rust
// ログテーブル: (id, timestamp, level, message)
// (timestamp, level)の複合インデックス

let log_index_schema = vec![DataType::Int64, DataType::Int32];
let log_index = BTree::create(buffer_pool.clone(), log_idx_root, log_index_schema)?;

// 特定期間のエラーログを検索
fn find_error_logs(start_time: i64, end_time: i64) -> Result<Vec<LogEntry>> {
    let start_key = BTreeKey::from_values(&[
        Value::Int64(start_time),
        Value::Int32(LogLevel::Error as i32),
    ], &schema)?;
    
    let end_key = BTreeKey::from_values(&[
        Value::Int64(end_time),
        Value::Int32(LogLevel::Error as i32),
    ], &schema)?;
    
    let mut logs = Vec::new();
    let mut iter = log_index.range_scan(Some(&start_key), Some(&end_key))?;
    
    while let Some((key, tuple_id)) = iter.next()? {
        let log_entry = logs_table.get(tuple_id)?;
        logs.push(log_entry);
    }
    
    Ok(logs)
}
```

### 3. 全文検索インデックス（簡易版）

```rust
// 単語 -> ドキュメントIDのマッピング
let word_index_schema = vec![DataType::Varchar];
let word_index = BTree::create(buffer_pool.clone(), word_idx_root, word_index_schema)?;

// ドキュメントのインデックス作成
fn index_document(doc_id: u32, content: &str) -> Result<()> {
    let words = tokenize(content);
    let tuple_id = TupleId::new(PageId(doc_id / 100), (doc_id % 100) as u16);
    
    for word in words {
        let key = BTreeKey::from_values(&[Value::String(word.to_lowercase())], &schema)?;
        word_index.insert(&key, tuple_id)?;
    }
    
    Ok(())
}

// 単語検索
fn search_word(word: &str) -> Result<Vec<u32>> {
    let key = BTreeKey::from_values(&[Value::String(word.to_lowercase())], &schema)?;
    let tuple_ids = word_index.search(&key)?;
    
    let mut doc_ids = Vec::new();
    for tuple_id in tuple_ids {
        let doc_id = tuple_id.page_id.0 * 100 + tuple_id.slot_id as u32;
        doc_ids.push(doc_id);
    }
    
    Ok(doc_ids)
}
```

## パフォーマンスのベストプラクティス

### 1. 適切なインデックスの選択

```rust
// 良い例：選択性の高いカラムをインデックス化
let unique_id_index = BTree::create(buffer_pool, root, vec![DataType::Int64])?;

// 悪い例：選択性の低いカラム（性別など）
// これは全表スキャンとあまり変わらない性能になる可能性
let gender_index = BTree::create(buffer_pool, root, vec![DataType::Boolean])?;
```

### 2. 複合インデックスの順序

```rust
// クエリ: WHERE user_id = ? AND timestamp > ?
// 良い順序：頻繁に等価条件で使われるカラムを先に
let schema = vec![DataType::Int32, DataType::Int64];  // (user_id, timestamp)

// 悪い順序：範囲条件のカラムを先にすると効率が落ちる
let schema = vec![DataType::Int64, DataType::Int32];  // (timestamp, user_id)
```

### 3. バッチ操作の活用

```rust
// 複数の挿入をまとめて実行
let mut batch = Vec::new();
for i in 0..1000 {
    let key = BTreeKey::from_values(&[Value::Int32(i)], &schema)?;
    batch.push((key, generate_tuple_id(i)));
}

// バッチ挿入（ラッチの取得回数を削減）
btree.insert_batch(&batch)?;
```

## エラーハンドリング

```rust
use vibedb::access::btree::{BTreeError, ErrorKind};

match btree.insert(&key, tuple_id) {
    Ok(()) => println!("Insert successful"),
    Err(BTreeError { kind: ErrorKind::DuplicateKey, .. }) => {
        println!("Key already exists");
    }
    Err(BTreeError { kind: ErrorKind::PageFull, .. }) => {
        // ページ分割が必要だが、何らかの理由で失敗
        println!("Failed to split page");
    }
    Err(e) => {
        eprintln!("Unexpected error: {}", e);
    }
}
```

## デバッグとテスト

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_btree_operations() -> Result<()> {
        let btree = create_test_btree()?;
        
        // 挿入テスト
        for i in 0..100 {
            let key = BTreeKey::from_values(&[Value::Int32(i)], &schema)?;
            btree.insert(&key, TupleId::new(PageId(0), i as u16))?;
        }
        
        // 検索テスト
        let key = BTreeKey::from_values(&[Value::Int32(50)], &schema)?;
        let results = btree.search(&key)?;
        assert_eq!(results.len(), 1);
        
        // 範囲検索テスト
        let start = BTreeKey::from_values(&[Value::Int32(10)], &schema)?;
        let end = BTreeKey::from_values(&[Value::Int32(20)], &schema)?;
        let mut count = 0;
        let mut iter = btree.range_scan(Some(&start), Some(&end))?;
        while let Some(_) = iter.next()? {
            count += 1;
        }
        assert_eq!(count, 11);  // 10から20まで（両端含む）
        
        Ok(())
    }
}
```