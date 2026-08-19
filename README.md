# bp35c2-collector

BP35C2 USB Wi-SUNドングルを使って、Bルート経由でスマート電力量メータから電力メトリクスを取得し、
**stdout (JSONL) / InfluxDB v2 / Prometheus** に多重出力するデーモン。

## 取れるメトリクス

| 項目 | EPC | 間隔 | メトリクス名 |
|---|---|---|---|
| 瞬時電力 (W, 符号付き) | 0xE7 | 10s | `smartmeter_power_instant_w` |
| 瞬時電流 R相/T相 (A) | 0xE8 | 10s | `smartmeter_current_instant_a{phase="r\|t"}` |
| 瞬時電圧 R相/T相 (V) | 0xE9 | 10s | `smartmeter_voltage_instant_v{phase="r\|t"}` |
| 積算電力量 正/逆 (kWh) | 0xE0/E3 | 60s | `smartmeter_energy_forward_kwh`, `..._reverse_kwh` |
| 定時積算 30分値 | 0xEA/EB | 30m | `smartmeter_energy_forward_scheduled_kwh` 他 |
| 異常発生状態 | 0x88 | 60s | `smartmeter_fault` |

単相2線メータは T相電流が `0x7FFE` sentinel で返るので自動判定して片相のみ出力。
`0xE9` 非対応メータはSNAレスポンスで黙ってスキップ (`0x9F` の Get プロパティマップで起動時に検出)。

## 必要なもの

- Linux (Raspberry Pi 含む) + Go 1.22+
- BP35C2 USB ドングル (`/dev/ttyUSB0` 等で認識)
- 電力会社発行の **Bルート認証ID (32文字) + パスワード (12文字)**
- (任意) InfluxDB v2、Prometheus

## ビルド

```sh
go build -o bp35c2-collector ./cmd/bp35c2-collector
```

## 設定

[examples/config.yaml](examples/config.yaml) をコピーして編集。認証情報は環境変数で渡す (YAMLに平文で書かない):

```yaml
broute:
  id: "${BROUTE_ID}"
  password: "${BROUTE_PASSWORD}"
sinks:
  stdout:     { enabled: true }
  prometheus: { enabled: true, listen: ":9101" }
  influxdb:
    enabled: true
    url: http://influxdb:8086
    token: "${INFLUX_TOKEN}"
    org: home
    bucket: smartmeter
```

## 手元で動かす

```sh
export BROUTE_ID=0123456789ABCDEF0123456789ABCDEF
export BROUTE_PASSWORD=YourPass1234
sudo ./bp35c2-collector --config ./examples/config.yaml
# ↑ /dev/ttyUSB0 の読み書きに root or dialout グループが必要
```

初回はアクティブスキャン (~8秒) → PANA認証 (通常数十秒、最長 706秒) → 接続。
`state_dir/channel` にチャネルが保存されるので、2回目以降はスキャンをスキップして即接続します。

## stdout 出力例 (`enabled: true` 時)

```json
{"ts":"2026-08-19T10:00:00.010Z","metric":"smartmeter_power_instant_w","value":523,"unit":"W"}
{"ts":"2026-08-19T10:00:00.020Z","metric":"smartmeter_current_instant_a","value":5.2,"unit":"A","tags":{"phase":"r"}}
{"ts":"2026-08-19T10:00:00.020Z","metric":"smartmeter_current_instant_a","value":4.8,"unit":"A","tags":{"phase":"t"}}
```

## Prometheus

```sh
curl http://localhost:9101/metrics
```

- スマメ値: `smartmeter_*`
- 自己観測: `bp35c2_reconnect_total`, `bp35c2_session_state`, `bp35c2_last_response_seconds`, `bp35c2_sink_write_errors_total{sink}`, `bp35c2_get_errors_total`
- ヘルスチェック: `GET /healthz` → `ok`

## InfluxDB → Grafana

measurement `smartmeter`、tags は `phase`/`direction`、fields は上表のメトリクス名がそのまま入ります。
Grafanaで `from(bucket:"smartmeter") |> range(...) |> filter(fn:(r)=>r._measurement=="smartmeter")` から可視化。

### 容量削減のためのダウンサンプル

Influxに貯まる point 数を減らしたいがPrometheus/stdoutは細かいまま維持したい、というときは
InfluxDB sink だけに downsample を効かせられます:

```yaml
sinks:
  influxdb:
    downsample:
      window: 1m          # 1分ごとに1点にまとめる。0で無効
      aggregation: mean   # mean | max | min | last | sum
```

- 集約は epoch 揃えの固定窓 (プロセス再起動でも境界がズレない)
- 積算 kWh (`smartmeter_energy_*_kwh`) は counter なので常に `last` (mean などにしても自動で override)
- stdout / Prometheus には影響しない — Prometheus は最新値を pull されるので集約は不要

## systemd 常駐

```sh
# バイナリを配置
sudo install -m 0755 bp35c2-collector /usr/local/bin/

# 設定と秘密情報
sudo mkdir -p /etc/bp35c2-collector
sudo install -m 0644 examples/config.yaml /etc/bp35c2-collector/config.yaml
sudo install -m 0600 examples/env.example /etc/bp35c2-collector/env
sudo $EDITOR /etc/bp35c2-collector/env   # 実際のIDとパスワード、Influxトークンを入れる

# ユーザとunit
sudo useradd -r -s /usr/sbin/nologin -G dialout bp35c2
sudo install -m 0644 systemd/bp35c2-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bp35c2-collector
journalctl -u bp35c2-collector -f
```

`Restart=always` + `WatchdogSec=60s` + アプリからの `sd_notify` で、無応答時も自動再起動します。

## トラブルシュート

- **ずっと `PANA authentication timed out`**: 認証ID/パスの typo、または電力会社側の登録漏れ。パスワードは英数のみ12文字。
- **`no meter responded to active scan`**: スマメが遠い/近隣に他のBルート機器の妨害。`broute.scan_time_exp` を 8 まで上げる (全ch ~36秒)。
- **`MAC link lost` を繰り返す**: 電波弱い可能性。Prometheus で `bp35c2_reconnect_total` を監視、ドングルの位置を変える。
- **接続はするが値が全部0**: メータがまだCapabilityを送りきってない。10-20秒待つと出始めるはず。
- **USBを抜き差ししたら止まる**: driverが検知して自動再オープンするはず。`journalctl` で `broute connect failed` が続くなら permission (dialout group) を確認。

## テスト

```sh
go test ./...
```

実機なしで frame/echonet/driver/broute の状態機械までカバー。
