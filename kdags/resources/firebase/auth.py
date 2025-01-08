import os
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd


def init_firebase():
    credentials_dict = {
        "type": "service_account",
        "project_id": "ayudante-mantenimiento",  # hardcode from your json
        "private_key": os.environ.get("FIREBASE_KEY").replace("\\n", "\n"),
        "client_email": "firebase-adminsdk-pg54a@ayudante-mantenimiento.iam.gserviceaccount.com",  # hardcode from your json
        "client_id": "105618258764884721189",  # hardcode from your json
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",  # this is usually standard
        "token_uri": "https://oauth2.googleapis.com/token",  # this is usually standard
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",  # usually standard
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-pg54a%40ayudante-mantenimiento.iam.gserviceaccount.com",  # hardcode from your json
    }

    cred = credentials.Certificate(credentials_dict)
    firebase_admin.initialize_app(cred)
