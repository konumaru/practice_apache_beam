# Practice Apache Beam

Pythonをつかって Apache Beam に慣れるための Repository


## Environment

- python version, 3.8.3
- Package Manegemant Tool, poetry


## Practices
### Step 1, ローカル環境でテキストデータを処理
Execute Command
```
$ python step01_local_processing.py \
    --input ../data/step01/input.txt \
    --output ../data/step01/output.txt
```

下記のようなtextファイルを読み込み、各行の文字列の長さを出力する

```input.txt
good morning.
good afternoon.
good evening.
```


### Step 2, Google Cloud Dataflowでテキストデータを処理


### Step 3, ローカル環境でストリームデータを処理


### Step 4, Google Cloud Dataflowでストリームデータを処理



## Reference
- [Apache Beam (Dataflow) 実践入門【Python】](https://qiita.com/esakik/items/3c5c18d4a645db7a8634#%E3%81%AF%E3%81%98%E3%82%81%E3%81%AB)
- [ケーラブルデータサイエンス データエンジニアのための実践Google Cloud Platform](https://www.amazon.co.jp/dp/4798158836)
