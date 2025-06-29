# vibedb Development Roadmap

## 概要

vibedbの開発ロードマップです。各フェーズは前のフェーズの成果物に依存し、段階的に機能を追加していきます。

## Phase 1: Access層の改善

### 目的
現在のAccess層の問題点を修正し、複数テーブル対応の基盤を整える。

### 実装内容

#### 1.1 HeapPageの拡張
- Special領域（4バイト）を活用してnext_page_idを実装
- `get_next_page_id()` / `set_next_page_id()` メソッドの追加
- ページ連結によるテーブル構造の実現

#### 1.2 スペース計算の責務分離
- HeapPageに `required_space_for(data_len: usize) -> usize` メソッド追加
- マジックナンバー（+4）の除去
- HeapPage側で必要なスロットサイズを管理

#### 1.3 ゼロコピーget実装
- 不要なページデータのコピーを除去
- PageReadGuardを直接利用
- メモリ効率の向上

### 成果物
- 改善されたHeapPage実装
- より効率的なTableHeap実装
- 更新されたテストスイート

## Phase 2: Catalog層の実装

### 目的
システムカタログを実装し、複数テーブルを管理できるようにする。

### 実装内容

#### 2.1 基本構造
```rust
// catalog.rs
pub struct TableInfo {
    pub table_id: TableId,
    pub table_name: String,
    pub first_page_id: PageId,
}

pub struct Catalog {
    buffer_pool: BufferPoolManager,
    catalog_heap: TableHeap,
}
```

#### 2.2 ブートストラップ処理
- システムカタログテーブル自体の定義
- 固定PageId（0）からの初期化
- カタログテーブルの自己参照エントリ

#### 2.3 基本操作
- `get_table(name: &str) -> Result<TableInfo>`
- `create_table(name: &str) -> Result<TableInfo>`
- `list_tables() -> Result<Vec<TableInfo>>`

### 成果物
- catalog.rs の実装
- システムカタログの初期化処理
- カタログ操作のテスト

## Phase 3: 統合とDatabase抽象化

### 目的
Storage、Access、Catalog層を統合し、使いやすいインターフェースを提供。

### 実装内容

#### 3.1 Database構造体
```rust
pub struct Database {
    buffer_pool: BufferPoolManager,
    catalog: Catalog,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self>;
    pub fn create_table(&mut self, name: &str) -> Result<()>;
    pub fn open_table(&self, name: &str) -> Result<TableHeap>;
}
```

#### 3.2 初期化処理
- データベースファイルの作成
- システムカタログのブートストラップ
- 既存データベースのオープン

### 成果物
- 統合されたDatabase API
- エンドツーエンドのテスト
- 使用例のドキュメント

## Phase 4: 将来の拡張（参考）

### カラム情報の追加
- pg_attribute相当のテーブル
- スキーマ情報の管理

### 型システム
- データ型の定義
- タプルの型付き解釈

### クエリ実行
- SeqScanIterator
- 簡単なフィルタリング

## 実装順序の理由

1. **Access層の改善が先**
   - Catalog層がTableHeapを使うため、先に改善が必要
   - ページ連結はCatalog実装の前提条件

2. **Catalogが次**
   - 複数テーブル管理の基盤
   - システムカタログ自体もテーブルとして扱う

3. **統合が最後**
   - 各層が完成してから統合
   - クリーンなAPIの提供

## テスト戦略

### 単体テスト
- 各コンポーネントの独立したテスト
- エッジケースの網羅

### 統合テスト
- 複数テーブルの作成・操作
- カタログの永続性
- 再起動後の動作確認

### パフォーマンステスト
- 大量データの挿入
- ページ連結のオーバーヘッド測定