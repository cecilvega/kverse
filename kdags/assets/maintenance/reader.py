import pandas as pd
from firebase_admin import firestore


def get_attendances():
    db = firestore.client()
    docs = db.collection("attendances").stream()

    attendances = {}
    for doc in docs:
        data = doc.to_dict()
        columns = ["date", "ingreso", "createdAt"]
        # data[columns] = data[columns].astype("datetime64[ns]")
        for col in columns:
            data[col] = pd.to_datetime(data[col]).tz_localize(None)
        # data.loc[:, columns] = data[columns].apply(lambda x: pd.to_datetime(x).dt.tz_localize(None))

        attendances[doc.id] = {"id": doc.id, **data}

    df = pd.DataFrame.from_dict(attendances, orient="index")
    return df
