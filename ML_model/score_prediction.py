import os

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MultiLabelBinarizer, OneHotEncoder, StandardScaler
from sklearn.tree import DecisionTreeRegressor
from sqlalchemy import create_engine

load_dotenv(dotenv_path="/home/datdp/DE_Prj/Dagster-dlt-dbt-pipeline/.env")


class Config:
    """
    Central configuration
    """

    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DATABASE = os.getenv("POSTGRES_DB")

    DB_URI = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{POSTGRES_DATABASE}"

    TABLE_NAME = "cleaned_data"
    SCHEMA = "silver"

    TARGET = "score"

    NUM_FEATURES = [
        "members",
        "favorites",
        "popularity",
        "episodes",
        "engagement",
        "year",
    ]

    CAT_FEATURES = ["type", "source", "season", "status", "rating"]

    TEST_SIZE = 0.2
    RANDOM_STATE = 42

    MODEL_PATH = "models/best_model.pkl"


class PostgresDataLoader:
    """
    Load data
    """

    def __init__(self, db_uri: str, table_name: str, schema: str):
        self.engine = create_engine(db_uri)
        self.table_name = table_name
        self.schema = schema

    def load(self) -> pd.DataFrame:
        query = f"SELECT * FROM {self.schema}.{self.table_name}"
        return pd.read_sql(query, self.engine)


class DataCleaner:
    """
    Clean raw dataframe
    """

    @staticmethod
    def clean(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df = df.replace("", np.nan)

        return df


class AnimeFeatureEngineer(BaseEstimator, TransformerMixin):
    """
    Feature engineering + genres encoding
    """

    def __init__(self):
        self.mlb = MultiLabelBinarizer()

    def fit(self, X, y=None):
        if "genres" not in X.columns:
            self.mlb.fit([["unknown"]])
            return self

        genres = (
            X["genres"]
            .fillna("unknown")
            .apply(lambda x: [g.strip() for g in str(x).split(",") if g])
        )

        self.mlb.fit(genres)
        return self

    def transform(self, X):
        X = X.copy()

        X["engagement"] = X["favorites"] / (X["members"] + 1)

        X["aired_from"] = pd.to_datetime(X["aired_from"], errors="coerce")
        X["year"] = X["aired_from"].dt.year

        if "genres" in X.columns:
            genres = (
                X["genres"]
                .fillna("unknown")
                .apply(lambda x: [g.strip() for g in str(x).split(",") if g])
            )

            genres_encoded = self.mlb.transform(genres)

            genres_df = pd.DataFrame(
                genres_encoded,
                columns=self.mlb.classes_,
                index=X.index,
            )

            X = pd.concat([X.drop(columns=["genres"]), genres_df], axis=1)

        X = X.drop(columns=["aired_from"], errors="ignore")

        return X


class PreprocessorBuilder:
    """
    Build preprocessing
    """

    def __init__(self, num_features, cat_features):
        self.num_features = num_features
        self.cat_features = cat_features

    def build(self):
        num_pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
            ]
        )

        cat_pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="constant", fill_value="unknown")),
                ("encoder", OneHotEncoder(handle_unknown="ignore")),
            ]
        )

        return ColumnTransformer(
            [
                ("num", num_pipeline, self.num_features),
                ("cat", cat_pipeline, self.cat_features),
            ],
            remainder="drop",
        )


class ModelFactory:
    """
    Provide models
    """

    @staticmethod
    def get_models():
        return {
            "linear_regression": LinearRegression(),
            "decision_tree": DecisionTreeRegressor(random_state=42),
            "random_forest": RandomForestRegressor(random_state=42),
            "gradient_boosting": GradientBoostingRegressor(random_state=42),
        }


class MultiModelTrainer:
    """
    Train models
    """

    def __init__(self, preprocessor):
        self.preprocessor = preprocessor

    def train_all(self, X_train, y_train):
        models = ModelFactory.get_models()
        trained_models = {}

        for name, model in models.items():
            pipeline = Pipeline(
                [
                    ("feature_engineering", AnimeFeatureEngineer()),
                    ("preprocessor", self.preprocessor),
                    ("model", model),
                ]
            )

            pipeline.fit(X_train, y_train)
            trained_models[name] = pipeline

        return trained_models


class ModelComparator:
    """
    Compare models
    """

    @staticmethod
    def compare(models, X_test, y_test):
        results = []

        for name, model in models.items():
            preds = model.predict(X_test)

            results.append(
                {
                    "model": name,
                    "MAE": mean_absolute_error(y_test, preds),
                    "RMSE": root_mean_squared_error(y_test, preds),
                    "R2": r2_score(y_test, preds),
                }
            )

        return pd.DataFrame(results).sort_values(by="RMSE")


class Evaluator:
    """
    Evaluate model
    """

    @staticmethod
    def evaluate(model, X_test, y_test):
        preds = model.predict(X_test)

        print(f"MAE: {mean_absolute_error(y_test, preds):.4f}")
        print(f"RMSE: {root_mean_squared_error(y_test, preds):.4f}")
        print(f"R2: {r2_score(y_test, preds):.4f}")


class ModelPersistence:
    """
    Save/load model
    """

    @staticmethod
    def save(model, path: str):
        directory = os.path.dirname(path)

        if directory:
            os.makedirs(directory, exist_ok=True)

        joblib.dump(model, path)


class AnimeMLWorkflow:
    """
    Full workflow
    """

    def __init__(self, config: Config):
        self.config = config

    def run(self):
        loader = PostgresDataLoader(
            self.config.DB_URI, self.config.TABLE_NAME, self.config.SCHEMA
        )

        df = loader.load()
        df = DataCleaner.clean(df)

        df = df.dropna(subset=[self.config.TARGET])

        DROP_COLS = ["anime_id", "title"]
        X = df.drop(columns=[self.config.TARGET] + DROP_COLS, errors="ignore")
        y = df[self.config.TARGET]

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=self.config.TEST_SIZE,
            random_state=self.config.RANDOM_STATE,
        )

        preprocessor = PreprocessorBuilder(
            self.config.NUM_FEATURES, self.config.CAT_FEATURES
        ).build()

        trainer = MultiModelTrainer(preprocessor)
        models = trainer.train_all(X_train, y_train)

        results = ModelComparator.compare(models, X_test, y_test)

        print("\nModel Comparison:")
        print(results)

        best_model_name = results.iloc[0]["model"]
        best_model = models[best_model_name]

        print(f"\nBest model: {best_model_name}")

        Evaluator.evaluate(best_model, X_test, y_test)

        ModelPersistence.save(best_model, self.config.MODEL_PATH)


if __name__ == "__main__":
    config = Config()
    workflow = AnimeMLWorkflow(config)
    workflow.run()
