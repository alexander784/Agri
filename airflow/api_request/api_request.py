import requests
import pandas as pd
from pathlib import Path

API_KEY = "f836d7c5364319d651a58a876863605e"
CITY = "Nairobi"
CITIES = ["Nairobi", "Mombasa", "Kisumu", "Eldoret", "Nakuru"]



def make_api_request(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_weather():
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={CITY}&units=metric&appid={API_KEY}"
    data = make_api_request(url)
    df = pd.json_normalize(data["list"])
    df["city"] = CITY

def get_customers():
    url = "https://fakerapi.it/api/v1/persons?_quantity=200"
    data = make_api_request(url)
    df = pd.DataFrame(data["data"])
    df["city"] = pd.Series(CITIES).sample(len(df), replace=True, random_state=42).values

def get_products():
    url = "https://fakestoreapi.com/products"
    data = make_api_request(url)
    df = pd.DataFrame(data)

def get_orders():
    url = ("https://fakerapi.it/api/v1/custom?_quantity=1000"
           "&order_id=number&customer_id=number&product_id=number"
           "&quantity=number&date=date&city=word")
    data = make_api_request(url)
    df = pd.DataFrame(data["data"])
    df["city"] = pd.Series(CITIES).sample(len(df), replace=True, random_state=42).values


def extract_all():
    get_weather()
    get_customers()
    get_products()
    get_orders()

if __name__ == "__main__":
    extract_all()
    print("Data extracted and saved as CSVs.")
