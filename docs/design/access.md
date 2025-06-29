# Access Layer Design

## 概要

Access層は、Storage層の上位に位置し、物理的なページ構造を論理的なテーブル・タプル操作に変換する責務を持ちます。この層により、上位層（Executor等）は複数ページにまたがるデータを透過的に扱えるようになります。

## 設計原則

1. **メモリ効率の最優先**
   - Tupleは生のバイト列として保持
   - スキーマ情報の重複を避ける

2. **段階的な実装**
   - 最小限の機能から開始
   - 将来の拡張を妨げない設計

3. **責任の明確な分離**
   - Storage層：ページ単位の物理的I/O
   - Access層：論理的なテーブル・タプル操作

## アーキテクチャ

```
┌─────────────────────────────────────┐
│          Executor Layer             │ ← 将来実装
├─────────────────────────────────────┤
│          Access Layer               │ ← 今回の実装
│  ┌─────────────┬─────────────────┐  │
│  │  TableHeap  │     Tuple       │  │
│  │             │   TupleId       │  │
│  └─────────────┴─────────────────┘  │
├─────────────────────────────────────┤
│         Storage Layer               │ ← 実装済み
│  ┌─────────────┬─────────────────┐  │
│  │BufferPool   │    HeapPage     │  │
│  │ Manager     │                 │  │
│  └─────────────┴─────────────────┘  │
└─────────────────────────────────────┘
```

## 主要コンポーネント

### TupleId

タプルの一意識別子。ページIDとスロットIDの組み合わせで構成。

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TupleId {
    pub page_id: PageId,
    pub slot_id: u16,
}
```

**設計上の考慮点**：
- 8バイトで表現可能（PageId: 4バイト + slot_id: 2バイト）
- 順序性を持つ（ページ順→スロット順）
- コピーが軽量

### Tuple

データベースの行を表す論理的な構造。現在の実装では生のバイト列を保持。

```rust
pub struct Tuple {
    pub tuple_id: TupleId,
    pub data: Vec<u8>,  // HeapPageから取得した生データ
}
```

**なぜAccess層に配置するか**：
1. Storage層は物理的なページ構造のみを扱う
2. Tupleは論理的な行の概念
3. 将来的にMVCC情報やスキーマ解釈を追加予定

**なぜ生のバイト列として扱うか**：
1. スキーマがまだ存在しない
2. 各Tupleがスキーマを持つとメモリ効率が悪い
3. PostgreSQLも同様のアプローチ（HeapTupleData）

### TableHeap

複数のHeapPageからなるテーブル全体を管理。ページ境界を意識せずにタプル操作を提供。

```rust
pub struct TableHeap {
    buffer_pool: BufferPoolManager,
    table_id: TableId,  // 初期実装では u32
}
```

**主要メソッド**：
- `insert(&mut self, data: &[u8]) -> Result<TupleId>`
  - 空きスペースのあるページを探して挿入
  - 必要に応じて新しいページを割り当て
  
- `get(&self, tuple_id: TupleId) -> Result<Option<Tuple>>`
  - TupleIdから該当ページを取得
  - 削除済みの場合はNone
  
- `update(&mut self, tuple_id: TupleId, data: &[u8]) -> Result<()>`
  - In-place更新（サイズが変わる場合は削除+挿入）
  
- `delete(&mut self, tuple_id: TupleId) -> Result<()>`
  - 該当スロットを削除マーク

## 実装詳細

### ページ管理

1. **ページ連結構造**
   - 各ページのSpecial領域にnext_page_idを格納
   - テーブルのページはリンクリストを形成
   - 非連続なページIDでもテーブルを構成可能

2. **空きページ探索**
   - **現在の実装**: first_page_idからnext_page_idを辿って線形探索
   - **改善予定**: 各ページの空き容量を記録し、効率的に探索
   - **将来**: FSM（Free Space Map）で管理

3. **ページ内レイアウト**
   - Storage層のHeapPageをそのまま使用
   - PostgreSQLスタイルのレイアウト
   - Special領域でnext_page_idを管理

### エラーハンドリング

- 無効なTupleId：`Error::TupleNotFound`
- 空きスペース不足：`Error::NoSpace`
- I/Oエラー：Storage層から伝播

## テスト設計

### 単体テスト

1. **TupleIdテスト**
   - 作成と比較
   - 順序性の確認
   - Debug表示

2. **Tupleテスト**
   - 作成とデータアクセス
   - 空のデータの扱い

3. **TableHeapテスト**
   - 空のテーブル作成
   - 単一タプルの挿入・取得
   - 複数タプルの操作
   - ページ境界をまたぐ操作
   - 削除と再利用
   - 更新（同サイズ/異サイズ）

### 統合テスト

- 大量データの挿入
- ランダムアクセスパターン
- 同時アクセス（将来）

## 計画中の改善

### スペース計算の責務分離
- HeapPageに`required_space_for(data_len: usize) -> usize`メソッドを追加
- マジックナンバー（+4）を除去し、HeapPage側でスロットサイズを管理
- より明確な責務分離とカプセル化

### ゼロコピー実装
- 現在：getメソッドで不要なページ全体のコピーが発生
- 改善：PageReadGuardを直接利用し、必要な部分のみをスライス参照
- メモリ使用量の削減とパフォーマンス向上

### ページ連結の活用
- 現在：単純に`page_id + 1`で次のページを探索
- 改善：Special領域のnext_page_idを使用
- 削除されたページの再利用が可能に

## 将来の拡張

### Phase 1（現在）
- ✅ 基本的なCRUD操作
- ✅ 単純な線形ページ探索
- ❌ スキーマ情報
- ❌ トランザクション

### Phase 2（次期）
- Schema/Catalog連携
- 型付きデータアクセス
- NULL値の扱い

### Phase 3（将来）
- MVCC（Multi-Version Concurrency Control）
- インデックス連携
- 統計情報収集
- FSM（Free Space Map）
- Vacuum処理

## 他層との関係

### Storage層との関係
- BufferPoolManagerを通じてページアクセス
- HeapPageの構造に依存
- ページ単位の操作を論理的操作に変換

### Executor層との関係（将来）
- SeqScanIteratorの提供
- フィルタ条件の適用
- プロジェクション（列選択）

### Catalog層との関係（将来）
- テーブルメタデータの取得
- スキーマ情報の参照
- 制約チェック