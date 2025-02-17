import os

# Configuration constants
DEFAULT_WAIT = 20
BASE_PATH = r"C:\Users\andmn\OneDrive - Komatsu Ltd\REPARACION"
CHROME_DRIVER_PATH = r"C:\Users\andmn\PycharmProjects\kbooks\chromedriver.exe"
SHAREPOINT_URL = "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022"
RESO_URL = "https://resoplus.komatsu.cl/"
RESO_CREDENTIALS = {
    "email": os.getenv("RESO_EMAIL", "cecil.vega@global.komatsu"),
    "password": os.getenv("RESO_PASSWORD", "WoW098965978"),
}
