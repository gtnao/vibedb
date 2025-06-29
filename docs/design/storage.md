# Storage Layer Design

## 概要

vibedbのストレージ層は、ディスクI/Oを抽象化し、ページ単位でデータを管理します。

## ページ管理

### PageId
- ページを一意に識別するID
- 現在は単純なu32で実装
- 将来的にはファイルIDとページ番号の組み合わせに拡張可能

### PageManager
ページ単位でのディスクI/Oを管理するコンポーネント。

#### 主要機能
- **create/open**: データファイルの作成・オープン
- **read_page**: 指定ページの読み込み
- **write_page**: 指定ページへの書き込み（fsyncで永続化保証）
- **num_pages**: 現在のページ数取得

#### 設計原則
1. **固定ページサイズ**: 8KB（PAGE_SIZE定数）
2. **永続化保証**: write_page時にfsyncを実行
3. **シンプルなインターフェース**: バッファを渡すスタイルでゼロコピー

## ファイル構造

```
[Page 0][Page 1][Page 2]...[Page N]
```

- 各ページは8KBの固定サイズ
- ページIDからファイルオフセットは `page_id * PAGE_SIZE` で計算

## エラーハンドリング

anyhow::Resultを使用した統一的なエラーハンドリング：
- I/Oエラー
- バッファサイズ不正
- 存在しないページへのアクセス

## ページタイプ

### HeapPage
タプルデータを格納するページタイプ。PostgreSQLスタイルのレイアウトを採用：
- ヘッダー（20バイト）: page_id, lower, upper, special
- スロット配列: ヘッダー直後から下向きに成長
- タプルデータ: ページ末尾から上向きに成長
- 空き領域: `upper - lower`で管理

詳細は[Buffer Pool Design](buffer_pool.md#heappage構造)を参照。

## 将来の拡張性

1. **BufferPoolManager**: メモリ上でのページキャッシュ ✅ 実装済み
2. **WAL (Write-Ahead Logging)**: トランザクションログ
3. **複数ファイル対応**: テーブルスペースの概念
4. **並行アクセス**: 読み込みの並列化
5. **インデックスページ**: B+木などのインデックス構造
6. **FSMページ**: Free Space Map for efficient space management