# Geekbench Data Scraper

这是一个用于从 Geekbench 浏览器网站抓取 Geekbench 5 CPU 基准测试结果数据并存储到本地 SQLite 数据库的 Python 脚本。它支持断点续传、缺失 ID 抓取、指定 ID 抓取、抓取所有 NULL 数据行以及原始数据文件的组织和压缩。

**注意：** 此脚本需要 Geekbench 浏览器帐户才能登录并抓取数据。

## 功能

* **登录和会话管理：** 使用提供的凭据登录 Geekbench 浏览器，并保存/加载会话 cookie 以进行身份验证。
* **数据库集成：** 将抓取的基准测试数据存储在本地 SQLite 数据库中。
* **数据库初始化和版本控制：** 自动初始化数据库表，并包含一个基本的版本控制机制（注意：版本不匹配时会重建数据表）。
* **断点续传：** 能够从数据库中存在的最高 ID 继续抓取新的基准测试结果。
* **缺失 ID 验证和抓取 (Phase 1)：** 检查数据库中是否存在 ID 空隙（小于当前最高 ID 的缺失 ID），并尝试抓取这些缺失的数据。
* **抓取所有 NULL 数据行 (Phase N)：** 识别数据库中所有指定数据列均为 NULL 的行，并尝试重新抓取这些数据。
* **抓取特定 ID (Phase X)：** 允许用户通过命令行参数指定一个或多个要抓取的特定基准测试 ID。
* **连续抓取新数据 (Phase 2)：** 从数据库中最高 ID 的下一个 ID 开始，持续抓取新的基准测试结果。
* **原始数据文件保存：** 将每个基准测试结果的原始 `.gb5` 文件保存到本地。
* **原始数据组织：** 将分散在主目录下的原始 `.gb5` 文件按 ID 范围组织到子文件夹中（例如 `raw_data_5/1-5000`）。
* **原始数据压缩：** 将已完成 ID 范围的原始数据子文件夹压缩成 `.zip` 文件，并删除原文件夹以节省空间。
* **多进程抓取：** 利用多进程并行抓取数据，提高效率。
* **错误处理和重试：** 处理网络请求错误、认证错误，并尝试在认证失败后重新登录。
* **进度指示：** 在组织和压缩文件时显示简单的进度指示。

## 要求

* Python 3.x
* `requests` 库
* `beautifulsoup4` 库
* 标准库 (`sqlite3`, `multiprocessing`, `os`, `platform`, `time`, `json`, `getpass`, `http.cookies`, `sys`, `zipfile`, `argparse`, `shutil`, `threading`)

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
    * 运行组织：将分散在 `raw_data_5` 主目录下的 `.gb5` 文件组织到按 ID 范围划分的子文件夹中。

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
* **组织和压缩原始数据文件：**
    ```bash
    python gb5.py -o -c
    ```
    这会先组织原始文件，然后压缩已完成的 ID 范围文件夹。这些操作可以在抓取运行时或独立运行时执行。

### 认证

脚本首次运行时或保存的 cookie 过期时，会提示输入 Geekbench 浏览器帐户的用户名（电子邮件）和密码。成功登录后，cookie 将被保存在名为 `geekbench_cookies.json` 的文件中，以便后续运行使用。

## 文件结构

脚本运行时会创建以下文件和文件夹：

* `geekbench_5_data.db`：存储抓取的基准测试数据的 SQLite 数据库文件。
* `geekbench_cookies.json`：存储登录会话 cookie 的文件。
* `raw_data_5/`：存放原始 `.gb5` 文件的根目录。
    * `raw_data_5/<start_id>-<end_id>/`：组织后的原始数据子文件夹，例如 `raw_data_5/1-5000/`。
    * `raw_data_5/<start_id>-<end_id>.zip`：压缩后的原始数据文件，例如 `raw_data_5/1-5000.zip`。

## 数据库模式

`geekbench_5_data.db` 中的主要数据表为 `data`。其列名与 `DATA_COLUMNS` 列表中的定义一致，包括：

* `id` (INTEGER PRIMARY KEY)
* `date`
* `version`
* `Platform`
* `Compiler`
* `Operating_System`
* `Model`
* `Processor`
* `Threads`
* `Cores`
* `Processors`
* `Processor_Frequency`
* `L1_Instruction_Cache`
* `L1_Data_Cache`
* `L2_Cache`
* `L3_Cache`
* `L4_Cache`
* `RAM`
* `Type`
* `Processor_Minimum_Multiplier`
* `Processor_Maximum_Multiplier`
* `Power_Plan`
* `Number_of_Channels`
* `multicore_score`
* `score`
* 以及各种单核 (ST) 和多核 (MT) 工作负载分数 (`AES_XTS_ST_Score`, `Text_Compression_ST_Score`等)

## 注意事项

* 数据库版本控制目前是简单的：如果代码中的 `DATABASE_VERSION` 与数据库中的版本不匹配，将 **删除并重建** `data` 表，导致现有数据丢失。请谨慎修改 `DATABASE_VERSION`。
* 抓取速度取决于网络连接、Geekbench 网站的响应速度以及设置的多进程数量。
* 过高的并发进程数可能会导致 Geekbench 网站的阻止或认证错误。
* 原始数据的组织和压缩是可选的，但对于管理大量 `.gb5` 文件非常有用。

## 许可证

本项目遵循 GNU Affero General Public License v3.0 (AGPL-3.0) 许可。

您可以查看 [LICENSE](LICENSE) 文件以获取完整的许可信息。
