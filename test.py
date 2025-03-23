from Models import *
from importlib.resources import files


def main_testing():
    # Create a new instance of the EasySQL class
    test = EasySQL()
    print(test.get_table_names_and_column_names('datafiles/finance_tracker.db'))
   
   
if __name__ == "__main__":
    main_testing()
    print("Test ran successfully!") 