# Geekbench Data Scraper

这是一个用于从 Geekbench 浏览器网站抓取 Geekbench 5/6/AI 基准测试结果数据并存储到本地 SQLite 数据库的 Python 脚本。它支持断点续传、缺失 ID 抓取、指定 ID 抓取、抓取所有 NULL 数据行以及原始数据文件的整理和压缩。

**注意：** 此脚本需要 Geekbench 浏览器帐户才能登录并抓取数据。

## 功能

* **登录和会话管理：** 使用提供的凭据登录 Geekbench 浏览器，并保存/加载会话 cookie 以进行身份验证。
* **数据库集成：** 将抓取的基准测试数据存储在本地 SQLite 数据库中。
* **断点续传：** 能够从数据库中存在的最高 ID 继续抓取新的基准测试结果。
* **抓取特定 ID (Phase X)：** 允许用户通过命令行参数指定一个或多个要抓取的特定基准测试 ID。
* **持续抓取 (Phase 2)：** 从数据库中当前最高 ID 的下一个 ID 开始抓取新的基准测试结果，并持续抓取直到浏览器上当前可用的最大 ID。
* **同步抓取 (Phase 3)：** 在持续运行模式下，抓取完历史数据至当前最大 ID 后，脚本会进入同步阶段。它会周期性检查 Geekbench Browser 上新添加的基准测试结果并进行抓取，以保持数据库最新。
* **原始数据压缩：** 将已完成 ID 范围的原始数据子文件夹压缩成 `.zip` 文件，并删除原文件夹以节省空间。

## 要求

* Python 3.x
* `requests` 库
* `beautifulsoup4` 库
* 标准库 (`sqlite3`, `multiprocessing`, `os`, `time`, `json`, `getpass`, `sys`, `zipfile`, `argparse`, `shutil`, `threading`)

可以使用 pip 安装依赖：

```bash
pip install requests beautifulsoup4
```
## 使用方法

运行脚本需要通过命令行参数指定其操作模式。

```bash
python gb5.py [选项]
```

### 命令行选项

* `-N`
    * 运行 Phase N：尝试抓取数据库中所有指定数据列均为 NULL 的行的数据。
* `-c`
    * 运行清理：将原始数据子文件夹压缩成 `.zip` 文件。
* `-s <ids>`
    * 运行 Phase X：抓取特定的基准测试 ID。`<ids>` 是一个或多个用逗号分隔的 ID（例如 `-s 100,101,105`）。
* `-C`
    * 运行 Phase 2：继续抓取新的基准测试数据。如果未指定 `-N` 和 `-s`，则这是默认行为。
* `-o`
    * 运行整理：将分散在 `raw_data` 主目录下的原始文件整理到按 ID 范围划分的子文件夹中。

### 运行模式示例

* **默认模式 (连续抓取新数据)：** 如果不指定 `-N` 和 `-s`，脚本将运行 Phase 1 (验证缺失 ID) 和 Phase 2 (连续抓取新数据)。
    ```bash
    python gb5.py
    ```
* **只抓取缺失 ID 和所有 NULL 数据行 (不连续抓取)：**
    ```bash
    python gb5.py -N
    ```
    这会运行 Phase 1 (验证缺失 ID) 和 Phase N (抓取所有 NULL 数据行)，然后退出。
* **只抓取特定 ID：**
    ```bash
    python gb5.py -s 12345,67890
    ```
    这会运行 Phase 1 (验证缺失 ID) 和 Phase X (抓取指定的 ID)，然后退出。
* **抓取缺失 ID、所有 NULL 数据行，然后连续抓取：**
    ```bash
    python gb5.py -N -C
    ```
    这会运行 Phase 1, Phase N, 然后进入 Phase 2。
* **整理和压缩原始数据文件：**
    ```bash
    python gb5.py -o -c
    ```
    这会先整理原始文件，然后压缩已完成的 ID 范围文件夹。这些操作可以在抓取运行时或独立运行时执行。

### 认证

脚本首次运行时，会提示输入 Geekbench 浏览器帐户的用户名（电子邮件）和密码。成功登录后，cookie 将被保存在名为 `geekbench_cookies.json` 的文件中，以便后续运行使用。

## 文件结构

脚本运行时会创建以下文件和文件夹：

* `geekbench_x_data.db`：存储抓取的基准测试数据的 SQLite 数据库文件。
* `geekbench_cookies.json`：存储登录会话 cookie 的文件。
* `raw_data_x/`：存放原始文件的根目录。
    * `raw_data_x/<start_id>-<end_id>/`：整理后的原始数据子文件夹，例如 `raw_data_5/1-5000/`。
    * `raw_data_x/<start_id>-<end_id>.zip`：压缩后的原始数据文件，例如 `raw_data_5/1-5000.zip`。

## 注意事项

* 数据库版本控制目前是简单的：如果代码中的 `DATABASE_VERSION` 与数据库中的版本不匹配，将 **删除并重建** `data` 表，导致现有数据丢失。请谨慎修改 `DATABASE_VERSION`。
* 抓取速度取决于网络连接、Geekbench 网站的响应速度以及设置的多进程数量。
* 过高的并发进程数可能会导致 Geekbench 网站的阻止或认证错误。
* 原始数据的整理和压缩是可选的，但对于管理大量原始文件非常有用。

## 许可证

本项目遵循 GNU Affero General Public License v3.0 (AGPL-3.0) 许可。

您可以查看 [LICENSE](LICENSE) 文件以获取完整的许可信息。
