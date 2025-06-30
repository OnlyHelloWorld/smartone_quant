import os
import csv
from xtquant import xtdata

# 配置日志
from utils.quant_logger import init_logger

logger = init_logger()


def export_sectors_by_prefix(prefixes: list, output_dir: str):
    """
    导出指定前缀的板块及成分股到不同的CSV文件中，避免重复股票，并打印详细日志。

    Args:
        prefixes (list): 板块前缀列表，如 ['300SW1', '500SW1']
        output_dir (str): 输出文件目录
    """
    logger.info(f"开始执行板块成分股导出任务，共 {len(prefixes)} 个前缀：{prefixes}")

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"输出目录已创建或存在：{output_dir}")

    # 下载板块数据
    logger.info("正在下载板块数据...")
    sector_list = xtdata.get_sector_list()
    logger.info(f"共获取 {len(sector_list)} 个板块")

    total_stocks_written = 0

    for prefix in prefixes:
        logger.info(f"\n开始处理前缀：{prefix}")
        matched_sectors = [sector for sector in sector_list if sector.startswith(prefix)]
        logger.info(f"匹配到 {len(matched_sectors)} 个板块：{matched_sectors}")

        output_file = os.path.join(output_dir, f"{prefix}_sectors_stocks.csv")
        seen_stocks = set()
        record_count = 0
        duplicate_count = 0

        with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for sector_name in matched_sectors:
                stock_list = xtdata.get_stock_list_in_sector(sector_name)
                logger.info(f"  [板块] {sector_name} 包含 {len(stock_list)} 支股票")

                for stock_code in stock_list:
                    if stock_code in seen_stocks:
                        logger.warning(
                            f"  [重复] 股票 {stock_code} 已在前缀 {prefix} 的其他板块中出现，跳过写入（当前板块：{sector_name}）")
                        duplicate_count += 1
                        continue
                    seen_stocks.add(stock_code)
                    writer.writerow([stock_code, sector_name])
                    record_count += 1

        total_stocks_written += record_count
        logger.info(f"[SUCCESS] 写入完成：{output_file}，写入 {record_count} 条记录，跳过 {duplicate_count} 条重复记录")

    logger.info(f"\n[SUMMARY] 所有任务完成，累计写入成分股记录：{total_stocks_written} 条")
    logger.info("程序正常结束。\n")


if __name__ == "__main__":
    target_prefixes = ['300SW1', '500SW1', '1000SW1', 'SW1']
    export_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../stock_data/sector_stocks'))
    export_sectors_by_prefix(target_prefixes, export_dir)