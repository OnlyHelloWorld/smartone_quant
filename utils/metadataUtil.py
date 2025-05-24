import pandas as pd
import yaml
import os

class Metadata:
    """
    读取 symbols.csv、config.yaml，提供元数据查询。
    """
    def __init__(self, metadata_path: str):
        self.symbols = pd.read_csv(os.path.join(metadata_path, 'symbols.csv'))
        with open(os.path.join(metadata_path, 'config.yaml'), 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

    def get_symbol_info(self, symbol: str) -> dict:
        row = self.symbols[self.symbols['symbol'] == symbol]
        return row.to_dict(orient='records')[0] if not row.empty else {}

    def get_config(self) -> dict:
        return self.config
