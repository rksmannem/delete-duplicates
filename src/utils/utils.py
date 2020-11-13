import csv
import os


def write_to_csv(filename, dict_data=None):
    if dict_data is None:
        dict_data = []

    file_exists = os.path.isfile(filename)

    columns = ['vin', 'subscriberGuid']
    try:
        with open(filename, 'a', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=columns)
            if not file_exists:
                writer.writeheader()

            for data in dict_data:
                writer.writerow(data)
    except IOError:
        print("I/O error")
        raise
