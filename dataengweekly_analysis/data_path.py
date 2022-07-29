import os


def get_path(filename: str) -> str:
    return os.path.dirname(os.path.abspath(__file__)) + '/data/' + filename + ".json"


def get_csv_path(filename: str) -> str:
    return os.path.dirname(os.path.abspath(__file__)) + '/data/' + filename + ".csv"
