import json
from datetime import datetime, date, timedelta
import calendar
import re
from difflib import get_close_matches
from dateutil import parser

class MyDateTime:
    def __init__(self):
        """Initialize month mappings for full names, abbreviations, and numeric values."""
        self.months = {m: i for i, m in enumerate(calendar.month_name) if m}
        self.months_abbr = {m: i for i, m in enumerate(calendar.month_abbr) if m}
        self.months_num = {str(i): i for i in range(1, 13)}  # Support month numbers as strings

    def check_leap_year(self, year):
        """Checks if a given year is a leap year."""
        return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

    def correct_month_name(self, month_str):
        """Attempts to correct a misspelled month using fuzzy matching."""
        month_str = month_str.capitalize()
        all_months = list(self.months.keys()) + list(self.months_abbr.keys())
        closest_match = get_close_matches(month_str, all_months, n=1, cutoff=0.7)
        return closest_match[0] if closest_match else None

    def convert_string(self, date_str):
        """Converts input date string to a normalized form."""
        if '0000' in date_str:
            date_str = date_str.replace('0000', '2000')
        return date_str.strip().title()

    def determine_day_and_year(self, date_str) -> dict:
        """Determines the day, year, month, weekday, and validity of a given date string."""
        date_str = self.convert_string(date_str)  # Normalize input

        # Standardize separators
        date_str = re.sub(r"[-/,]", " ", date_str)
        parts = date_str.split()

        # Define the result structure
        result = {
            "input_date": date_str,
            "day": None,
            "month": None,
            "year": None,
            "day_name": None,
            "leap_year": None,
            "valid_last_day": None,
            "error": None,
            "is_valid": False
        }

        # Identify the month (full name, abbreviation, or numeric)
        month, month_index = None, -1
        for i, part in enumerate(parts):
            if corrected_month := self.correct_month_name(part):  # Attempt correction
                month, month_index = self.months.get(corrected_month) or self.months_abbr.get(corrected_month), i
                break
            elif part.isdigit() and part in self.months_num:
                month, month_index = int(part), i
                break

        # If no valid month found, return an error
        if month is None:
            result["error"] = "Invalid month found. Could not correct."
            return result

        try:
            numeric_values = [int(p) for i, p in enumerate(parts) if i != month_index]
            if len(numeric_values) != 2:
                raise ValueError("Invalid numeric values in date.")
            first_num, second_num = numeric_values
        except ValueError:
            result["error"] = "Invalid numeric values in date."
            return result

        # Determine day and year
        if first_num > 31:
            year, day = first_num, second_num
        elif second_num > 31:
            year, day = second_num, first_num
        else:
            year, day = first_num, second_num

        # Convert two-digit years (assuming it's 2000+ for now)
        year = 2000 + year if year < 100 else year
        result["leap_year"] = self.check_leap_year(year)

        # Get valid last day of the month
        try:
            valid_last_day = calendar.monthrange(year, month)[1]
            result["valid_last_day"] = valid_last_day
        except:
            result["error"] = "Invalid month or year."
            return result

        # Validate days
        if day > valid_last_day:
            result["error"] = f"Invalid day ({day}). Replaced with last valid day {valid_last_day}."
            day = valid_last_day  # Replace with last valid day
        else:
            result["is_valid"] = True  # Mark as valid if the day was correct

        # Get day name
        try:
            date_obj = datetime(year, month, day)
            result["day_name"] = date_obj.strftime("%A")
        except ValueError:
            result["error"] = "Invalid date"
            return result

        # Assign final valid values
        result.update({"day": day, "month": month, "year": year})

        return result

    def check_european_format(self, date_str) -> dict:
        """Checks and converts a date string in European format (dd-month-yyyy)."""
        date_str = self.convert_string(date_str)  # Normalize input

        # Standardize separators
        date_str = re.sub(r"[-/,]", " ", date_str)
        parts = date_str.split()

        # Define the result structure
        result = {
            "input_date": date_str,
            "day": None,
            "month": None,
            "year": None,
            "day_name": None,
            "leap_year": None,
            "valid_last_day": None,
            "error": None,
            "is_valid": False
        }

        # Identify the day, month, and year
        if len(parts) == 3:
            day, month_str, year = parts
            day = int(day)
            year = int(year)
            month = self.correct_month_name(month_str)

            if month is None:
                result["error"] = "Invalid month found. Could not correct."
                return result

            result["leap_year"] = self.check_leap_year(year)

            # Get valid last day of the month
            try:
                valid_last_day = calendar.monthrange(year, month)[1]
                result["valid_last_day"] = valid_last_day
            except:
                result["error"] = "Invalid month or year."
                return result

            # Validate days
            if day > valid_last_day:
                result["error"] = f"Invalid day ({day}). Replaced with last valid day {valid_last_day}."
                day = valid_last_day  # Replace with last valid day
            else:
                result["is_valid"] = True  # Mark as valid if the day was correct

            # Get day name
            try:
                date_obj = datetime(year, month, day)
                result["day_name"] = date_obj.strftime("%A")
            except ValueError:
                result["error"] = "Invalid date"
                return result

            # Assign final valid values
            result.update({"day": day, "month": month, "year": year})

        else:
            result["error"] = "Invalid date format. Expected format: dd-month-yyyy."

        return result

    def save_to_json(self, data, filename="converted_date.json"):
        """Saves data to a JSON file."""
        with open(filename, "a") as file:
            json.dump(data, file, indent=4)
            file.write("\n")

    def format_date(self, day, month, year):
        """Returns date in the format DD-Month-YYYY."""
        try:
            month_name = calendar.month_name[month]
            return f"{day:02d}-{month_name}-{year}"
        except:
            return f"01-January-2000"

    def get_converted_and_corrected_date(self, date_str) -> dict:
        """
        Main function to be called from another script.
        Returns a tuple of converted date and corrected date.
        """
        result = self.determine_day_and_year(date_str)
        self.save_to_json(result)
        print(f'Saved the result to JSON file')

        if result["is_valid"]:
            converted_date = self.format_date(result["day"], result["month"], result["year"])
            corrected_date = converted_date  # Since it's already valid
        else:
            corrected_date = self.format_date(result["valid_last_day"], result["month"], result["year"])
            converted_date = f"Invalid ({result['input_date']})"

        return converted_date, corrected_date

    def get_converted_and_corrected_date_european(self, date_str) -> dict:
        """
        Main function to be called from another script for European format.
        Returns a tuple of converted date and corrected date.
        """
        result = self.check_european_format(date_str)
        self.save_to_json(result)
        print(f'Saved the result to JSON file')

        if result["is_valid"]:
            converted_date = self.format_date(result["day"], result["month"], result["year"])
            corrected_date = converted_date  # Since it's already valid
        else:
            corrected_date = self.format_date(result["valid_last_day"], result["month"], result["year"])
            converted_date = f"Invalid ({result['input_date']})"

        return converted_date, corrected_date

    def detect_day(self, day_str, month, year):
        """Detects the day in a date string."""
        print(f'Day String: {day_str}, Month: {month}, Year: {year}')
        if year.isdigit() and len(year) == 2:
            n_year = int(f'20{year}')
        elif year.isdigit() and int(year) == 0:
            n_year = int(f'20{year}')
        elif year.isdigit() and int(year) < 1900:
            n_year = 1900
        elif year.isdigit() and int(year) > 1900:
            n_year = int(year)
        else:
            n_year = int(year)
           
        n_day = day_str if type(day_str) == int else int(day_str)    
        n_month = month if type(month) == int else int(month)
        leap_year = self.check_leap_year(n_year)
        if leap_year and n_month == 2:
            return 29, n_month, n_year
        
        
        if n_day > 28 and n_month == 2:
            return 28, n_month, n_year
        elif n_day > 30 and n_month in [4, 6, 9, 11]:    
            return 30, n_month, n_year
        elif n_day > 31 and n_month in [1, 3, 5, 7, 8, 10, 12]:
            return 31, n_month, n_year
        else:
            return (n_day if 1 <= n_day <= 31 else None), n_month, n_year

    def parse_date(self, date_str:str)-> datetime:
        # Possible formats: MM/DD/YYYY, DD/MM/YYYY, YYYY-MM-DD, etc.
        formats = ["%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y", "%d %m %Y", "%m %d %Y","%Y-%m-%d", "%Y-%d-%m", "%Y %m %d", "%Y %d %m"]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
            
        # As a fallback, use dateutil.parser (tries multiple formats)
        try:
            return parser.parse(date_str).date()
        except ValueError:
            return None

    def check_date(self, date_str:str) -> dict:
        """Checks if a date is valid and returns the corrected date."""
           
        date_str = re.sub(r"[-/,|;:.'\']", " ", date_str)
        parts = date_str.split()
        print(f'Date String: {date_str}')                          
        try:
            return self.parse_date(date_str).strftime("%d-%B-%Y")
        except:
           #date_str = self.convert_string(date_str)
           print(f'Converted Date String: {date_str}')
           date_str= self.determine_day_and_year(date_str)
            
        #date_str = re.sub(r"[-/,]", " ", date_str)
        parts = date_str.split()
        print(f'Parts: {parts}')
        myiter = iter(parts)
        first = self.determine_length(date_str)
        if first == 'year':
            year = next(myiter)
            month = next(myiter)
            test1, var1 = self.detect_month(month)
            n_month = var1
            day = next(myiter)
            n_day, n_month, n_year = self.detect_day(day, n_month, year)
        elif first == 'day':
            day = next(myiter)
            month = next(myiter)
            test2, var2 = self.detect_month(month)
            n_month = var2
            year = next(myiter)
            n_day, n_month, n_year = self.detect_day(day, n_month, year)
        elif first == 'month':
            month = next(myiter)
            test3, var3 = self.detect_month(month)
            n_month = var3
            day = next(myiter)
            year = next(myiter)
            n_day, n_month, n_year = self.detect_day(day, n_month, year)

        return datetime(n_year, n_month, n_day).strftime("%d-%B-%Y")

    def determine_length(self, date_str):
        mylist = date_str.split()
        for item in mylist:
            if item.isdigit() and mylist[1].isdigit() and mylist[2].isdigit():
                if int(item) > 12:
                    return 'day'
                else:
                    return 'month'
            if item.isdigit() and len(item) == 4:
                return 'year'
            elif item.isdigit() and len(item) == 2:
                return 'day'
            else:
                return 'month'

    def detect_month(self, date_str):
        if date_str.isdigit():
            return True, self.months_num.get(date_str)
        valid_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
                        "January", "February", "March", "April", "May", "June", "July", "August", "September",
                        "October", "November", "December", "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE",
                        "JULY", "AUGUST", "SEPTEMBER",'OCTOBER', 'NOVEMBER', 'DECEMBER']
        month_pattern = r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:uary|ch|ril|ember|ust|e)?\b"
        match = re.search(month_pattern, date_str, re.IGNORECASE)
        closest_match = get_close_matches(date_str, valid_months, n=1, cutoff=0.6)
        if match:
            if len(match.group()) > 3:
                return True, self.months.get(match.group())
            return True, self.months_abbr.get(match.group())

        else:
            if closest_match:
                if len(closest_match[0]) > 3:
                    return True, self.months.get(closest_match[0])
                return True, self.months_abbr.get(closest_match[0])  # Return corrected month name


            return False, None  # No valid month found



if __name__ == "__main__":
    # Example dates for testing (including misspelled months)
    dates = [
        '9 8 7 ',"1'\'12/1999",'j j 8','32 | 12 - 2029','20291224','2029-12-24','2029 12 24','2029 24 12','2029 12 32','2029 13, 12'
    ]

    my_date_handler = MyDateTime()

    for date in dates:
        try:
            result = my_date_handler.check_date(date)
            print(f'Input Date: {date} | Converted: {result}')
        except Exception as e:
            print(f'Input Date: {date} | Error: {str(e)}')