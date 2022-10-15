import datetime
import random
import requests

# create random date 
def random_date(start_yr,end_yr):
    start_date = datetime.date(start_yr, 1, 1)
    end_date = datetime.date(end_yr, 12, 31)

    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    rand_date = start_date + datetime.timedelta(days=random_number_of_days)
    return rand_date


def get_github_file(start_year,end_year,files:int):
    for i in range(files):
        date1=random_date(start_year,end_year)
        hour= random.randint(0,23)
        file_name = f"{date1}-{hour}.json.gz"
        url = f"https://data.gharchive.org/{file_name}"
        res = requests.get(url)
        with open(file_name,'wb') as f:
            f.write(res.content)
        print(f"{i}->successfully saved {file_name}")