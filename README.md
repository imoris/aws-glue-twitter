## 事前準備
- S3にgzip圧縮されたTweetデータを置く
- gzipのデータに対してglueのCrawlerでテーブルを自動生成する
- スクリプトのpathやDB名などを使うものに変更する

## Job実行
- Data sourceは2.で作成したテーブルを使用、Data targetは新規作成(Connection: S3 / Format: Parquet を選択)
- AdvnceでJob BookmarkをEnableにしておくと、次回の変換時に既に処理を行ったデータへの処理をスキップできます
