import csv
import argparse
from dataclasses import dataclass
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class Transaction:
    id: str
    user_id: str
    product_id: str
    price_usd: float

@dataclass_json
@dataclass
class TransactionList:
    transactions: list[Transaction]


def main(data_path, report_path):
    with open(data_path, 'r') as f:
        t_list: TransactionList = TransactionList.schema().loads(f.read())
        print(len(t_list.transactions))
        pass #


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='transactions.json', help='Please set datasets path.')
    parser.add_argument('--report_path', type=str, default='report.csv', help='Please set report path.')
    args = parser.parse_args()
    data_path = args.data_path
    report_path = args.report_path
    main(data_path, report_path)

