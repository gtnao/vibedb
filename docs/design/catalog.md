# Catalog Layer Design

## 概要

Catalog層は、データベース内のテーブルやカラムなどのメタデータを管理します。システムカタログ自体もテーブルとして実装され、自己記述的な構造を持ちます。

## 設計原則

1. **自己記述性**
   - システムカタログ自体もテーブルとして扱う
   - カタログテーブルの情報もカタログ内に格納

2. **シンプルさ**
   - 最小限の情報から開始
   - 段階的に機能を追加

3. **永続性**
   - メタデータは通常のテーブルと同様に永続化
   - 再起動後も情報を保持

## アーキテクチャ

```
┌─────────────────────────────────────┐
│           Application               │
├─────────────────────────────────────┤
│            Database                 │
│  ┌─────────────┬─────────────────┐  │
│  │   Catalog   │   User Tables   │  │
│  └─────────────┴─────────────────┘  │
├─────────────────────────────────────┤
│           Access Layer              │
│         (TableHeap)                 │
├─────────────────────────────────────┤
│          Storage Layer              │
│      (BufferPoolManager)            │
└─────────────────────────────────────┘
```

## システムカタログテーブル

### pg_tables（テーブル情報）

最初の実装では、以下の情報のみを管理：

| カラム名 | 型 | 説明 |
|---------|---|------|
| table_id | u32 | テーブルの一意識別子 |
| table_name | text | テーブル名 |
| first_page_id | u32 | 最初のページのID |

### 将来の拡張

#### pg_attribute（カラム情報）
```
| table_id | column_id | column_name | type_id | nullable |
```

#### pg_type（型情報）
```
| type_id | type_name | type_len |
```

## ブートストラップ処理

### 問題
- カタログ情報を読むにはカタログテーブルが必要
- カタログテーブルの情報はカタログに格納されている

### 解決策

```rust
// 固定定義
pub const CATALOG_TABLE_ID: TableId = 1;
pub const CATALOG_FIRST_PAGE: PageId = PageId(0);
pub const CATALOG_TABLE_NAME: &str = "pg_tables";

// ブートストラップ時の処理
1. PageId(0)を固定的にカタログテーブルのページとして扱う
2. カタログテーブル自身のエントリを最初に書き込む
3. その後、通常のテーブルとして扱える
```

## 実装詳細

### TableInfo構造体

```rust
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_id: TableId,
    pub table_name: String,
    pub first_page_id: PageId,
}
```

### Catalog構造体

```rust
pub struct Catalog {
    buffer_pool: BufferPoolManager,
    catalog_heap: TableHeap,  // カタログ自体もTableHeap
    // キャッシュ（オプション）
    table_cache: RwLock<HashMap<String, TableInfo>>,
}
```

### 主要メソッド

```rust
impl Catalog {
    /// 新規データベースの初期化
    pub fn initialize(buffer_pool: BufferPoolManager) -> Result<Self> {
        // 1. カタログテーブル用のページを作成
        // 2. カタログテーブル自身のエントリを挿入
        // 3. Catalogインスタンスを返す
    }
    
    /// 既存データベースのオープン
    pub fn open(buffer_pool: BufferPoolManager) -> Result<Self> {
        // 1. PageId(0)からカタログテーブルを読み込み
        // 2. Catalogインスタンスを構築
    }
    
    /// テーブル情報の取得
    pub fn get_table(&self, name: &str) -> Result<TableInfo> {
        // 1. キャッシュをチェック（あれば）
        // 2. カタログテーブルをスキャン
        // 3. 該当するエントリを返す
    }
    
    /// 新規テーブルの作成
    pub fn create_table(&mut self, name: &str) -> Result<TableInfo> {
        // 1. 新しいTableIdを生成
        // 2. 最初のページを作成
        // 3. カタログテーブルにエントリを追加
        // 4. TableInfoを返す
    }
    
    /// 全テーブルのリスト
    pub fn list_tables(&self) -> Result<Vec<TableInfo>> {
        // カタログテーブルの全エントリを返す
    }
}
```

## データフォーマット

### カタログエントリのシリアライズ

```rust
// 簡易実装
fn serialize_table_info(info: &TableInfo) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&info.table_id.to_le_bytes());
    data.extend_from_slice(&(info.table_name.len() as u32).to_le_bytes());
    data.extend_from_slice(info.table_name.as_bytes());
    data.extend_from_slice(&info.first_page_id.0.to_le_bytes());
    data
}

fn deserialize_table_info(data: &[u8]) -> Result<TableInfo> {
    // 逆の処理
}
```

## ページ管理

### テーブルのページ連結

```
カタログ: "users" → first_page: PageId(10)

Page 10:
  - next_page_id: Some(PageId(15))
  - データ...

Page 15:
  - next_page_id: Some(PageId(23))
  - データ...

Page 23:
  - next_page_id: None
  - データ...
```

### 新規ページの割り当て

1. **簡易実装**: 常にファイル末尾に新規ページを追加
2. **将来**: Free Page Listから取得

## テスト計画

### 単体テスト
- TableInfoのシリアライズ/デシリアライズ
- カタログエントリの追加・検索
- ブートストラップ処理

### 統合テスト
- 複数テーブルの作成
- データベースの再起動
- カタログの永続性確認

## 将来の拡張

### スキーマ情報
- カラム定義の追加
- 型情報の管理
- 制約情報

### 統計情報
- テーブルサイズ
- 行数
- 最終更新時刻

### アクセス制御
- テーブルの所有者
- アクセス権限