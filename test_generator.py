from generator import main as generator
from _generated import query as _generated

from sql import query as sql


def test_generator():
    # Generate the file
    print("Testing...")

    generator()

    # Compare the output of your generated code to the output of the actual SQL query
    # Note: This only works for standard queries, not ESQL queries.
    assert _generated() == sql(), print("Test Failed")

    print("Test Passed")

if __name__ == "__main__":
    test_generator()