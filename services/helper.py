import sys
import time
import random
import string


# Function to generate multiple unique IDs
def generate_multiple_unique_ids(count):
    return [generate_unique_id() for _ in range(count)]
    

def generate_unique_id():
    # Use a timestamp as the base for uniqueness
    timestamp = int(time.time() * 1000)  # Milliseconds since epoch
    # Convert timestamp to a base-36 string for shorter length
    timestamp_base36 = base36_encode(timestamp)
    
    # Add a random string to ensure uniqueness
    random_part = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
    
    # Combine timestamp and random part
    return f"{timestamp_base36}{random_part}".upper()

def base36_encode(number):
    """Encodes a number in base-36."""
    if not isinstance(number, int):
        raise TypeError("number must be an integer")
    if number < 0:
        raise ValueError("number must be non-negative")

    digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    result = ""
    while number > 0:
        number, remainder = divmod(number, 36)
        result = digits[remainder] + result
    return result or "0"


def get_list_from_dict(dict, key):
    return [
        col[key]
        for col in dict
    ]


import sys


# Function to generate multiple unique IDs
def generate_multiple_unique_ids(count):
    return [generate_unique_id() for _ in range(count)]


def generate_unique_id():
    # Use a timestamp as the base for uniqueness
    timestamp = int(time.time() * 1000)  # Milliseconds since epoch
    # Convert timestamp to a base-36 string for shorter length
    timestamp_base36 = base36_encode(timestamp)

    # Add a random string to ensure uniqueness
    random_part = ''.join(random.choices(string.ascii_letters + string.digits, k=6))

    # Combine timestamp and random part
    return f"{timestamp_base36}{random_part}".upper()


def base36_encode(number):
    """Encodes a number in base-36."""
    if not isinstance(number, int):
        raise TypeError("number must be an integer")
    if number < 0:
        raise ValueError("number must be non-negative")

    digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    result = ""
    while number > 0:
        number, remainder = divmod(number, 36)
        result = digits[remainder] + result
    return result or "0"


def get_list_from_dict(dict, key):
    return [
        col[key]
        for col in dict
    ]


# Entry point
if __name__ == "__main__":
    if len(sys.argv) > 1:
        function_name = sys.argv[1]
        if function_name == "generate_unique_id":
            print(generate_unique_id())
        elif function_name == "generate_multiple_unique_ids" and len(sys.argv) > 2:
            count = int(sys.argv[2])
            print(generate_multiple_unique_ids(count))
        elif function_name == "base36_encode" and len(sys.argv) > 2:
            number = int(sys.argv[2])
            print(base36_encode(number))
        else:
            print("Unknown function or insufficient arguments.")
    else:
        print("Please specify a function to run.")
