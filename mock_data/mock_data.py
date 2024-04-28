import argparse
import json
import random

def generate_data(output_file: str, num_rows: int=30) -> None:
    """
    Generate JSON data with random names, ages, and offices.

    :param num_rows: Number of rows to generate.
    :type num_rows: int
    :param output_file: Output file name.
    :type output_file: str
    """
    data = []
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    names = ["Alberto", "Jesus", "David", "Juan"]

    for _ in range(num_rows):
        person = {}
        person["name"] = random.choice(names) # Random name
        person["age"] = random.choice([random.randint(18, 50), None])  # Random age or None
        person["office"] = random.choice([random.choice(cities), ""])  # Random city or None
        data.append(person)

    with open(output_file, 'w') as f:
        json.dump(data, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate JSON data with random names, ages, and offices.')
    parser.add_argument('output_file', type=str, help='Output file name')
    args = parser.parse_args()
    generate_data(args.output_file)
