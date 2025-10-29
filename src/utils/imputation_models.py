import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

class ImputationModels:
    def __init__(self):
        self.label_encoders = {}

    def encode_categorical(self, df, categorical_cols):
        df_encoded = df.copy()
        for col in categorical_cols:
            le = LabelEncoder()
            df_encoded[col] = df_encoded[col].astype(str).fillna("N/A")
            df_encoded[col] = le.fit_transform(df_encoded[col])
            self.label_encoders[col] = le
        return df_encoded

    def decode_categorical(self, df, categorical_cols):
        df_decoded = df.copy()
        for col in categorical_cols:
            if col in self.label_encoders:
                le = self.label_encoders[col]
                df_decoded[col] = le.inverse_transform(df_decoded[col].astype(int))
        return df_decoded

    def fit_and_impute(self, df):
        df_filled = df.copy()

        # Drop rows with missing ID
        if 'id' in df_filled.columns:
            df_filled = df_filled[df_filled['id'].notna()]

        # Fill missing names with N/A
        if 'name' in df_filled.columns:
            df_filled['name'] = df_filled['name'].fillna("N/A")

        categorical_cols = ['department']
        numeric_cols = ['age', 'salary', 'number_of_years_worked']

        # Encode categorical columns
        df_encoded = self.encode_categorical(df_filled, categorical_cols)

        # Impute numeric fields
        for col in numeric_cols:
            if df_encoded[col].isna().sum() > 0:
                not_null = df_encoded[df_encoded[col].notna()]
                null = df_encoded[df_encoded[col].isna()]

                if not_null.empty or null.empty:
                    continue

                # Exclude string columns (name) from features
                X_train = not_null.drop(columns=[col, 'name'])
                y_train = not_null[col]
                X_missing = null.drop(columns=[col, 'name'])

                model = RandomForestRegressor(n_estimators=200, random_state=42)
                model.fit(X_train, y_train)
                preds = model.predict(X_missing)

                # Round appropriately
                if col in ['age', 'number_of_years_worked']:
                    preds = np.round(preds).astype(int)
                elif col == 'salary':
                    preds = np.round(preds, -2).astype(int)

                df_encoded.loc[df_encoded[col].isna(), col] = preds

        # Impute categorical columns
        for col in categorical_cols:
            mask = (df_filled[col].isna()) | (df_filled[col] == "N/A") | (df_filled[col] == "UNKNOWN")
            if mask.sum() > 0:
                not_null = df_encoded[~mask]
                null = df_encoded[mask]

                if not_null.empty or null.empty:
                    continue

                # Exclude non-numeric string columns
                X_train = not_null.drop(columns=[col, 'name'])
                y_train = not_null[col]
                X_missing = null.drop(columns=[col, 'name'])

                model = RandomForestClassifier(n_estimators=200, random_state=42)
                model.fit(X_train, y_train)
                preds = model.predict(X_missing)

                df_encoded.loc[mask, col] = preds

        # Decode categorical columns
        df_final = self.decode_categorical(df_encoded, categorical_cols)

        # Final rounding
        df_final['age'] = df_final['age'].astype(int)
        df_final['number_of_years_worked'] = df_final['number_of_years_worked'].astype(int)
        df_final['salary'] = df_final['salary'].astype(int)

        return df_final
