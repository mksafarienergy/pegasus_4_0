import argparse
import inspect
import timeit

def time_functions(file_name):
    # Import the module
    module = __import__(file_name)

    # Get a list of all functions in the module
    functions = inspect.getmembers(module, inspect.isfunction)

    # Iterate over the functions
    for name, function in functions:
        # Get the source code of the function
        source = inspect.getsource(function)

        # Use the timeit function to time the function
        t = timeit.timeit(source, number=10000)
        print(f'{name} took {t:.6f} seconds to run')

if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser(description='Time functions in a Python file')
    parser.add_argument('file_name', type=str, help='The name of the Python file')
    args = parser.parse_args()

    # Call the time_functions function
    time_functions(args.file_name)
