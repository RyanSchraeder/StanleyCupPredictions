# Data Prep
import os
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import seaborn as sns

from src.utils.logging import logger  # for logging


# ignore warnings
def warn(*args, **kwargs):
    pass


import warnings

warnings.warn = warn

from sklearn import metrics
from sklearn.decomposition import PCA  # principle component analysis
from sklearn.ensemble import StackingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix

## Pre-Processing
from sklearn.model_selection import (  # train/test split & k-fold cross validation
    KFold,
    cross_val_score,
    train_test_split,
)

# Models
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler  # scaler
from sklearn.svm import SVC, LinearSVC
from sklearn.tree import DecisionTreeClassifier

## Scoring


DATA_PATH = "../data/"

x = classification_df.loc[:, classification_df.columns != "outcome"]
y = classification_df["outcome"]

# Creating the Train and Test Split

# Use a train and test split for initial training & testing
x_train, x_test, y_train, y_test = train_test_split(
    x, y, test_size=0.30, random_state=42
)

# Building, Training, and Testing the Model Stack
## Building each model with some fun inheritance!


class model_development:
    def __init__(self, model, predictors, target):
        self.model = model
        self.predictors = predictors
        self.target = target

    @classmethod
    def ensemble(names: List[str], models: List) -> Dict[str, Any]:
        """Creates a dictionary with each model name and the model associated"""
        models = {names[i]: models[i] for i in range(len(names))}
        return models

    @classmethod
    def evaluate_ensemble(models, x_train, y_train, x_test) -> Dict[str, Any]:
        """
        1) Conducts PCA for feature selection and k-fold cross-validation on each model
        2) Evaluates each model with accuracy, precision, and recall scores and returns all average scores
        """

        pca = PCA(n_components=10)
        pca.fit_transform(x_train, y_train)

        cv = KFold(n_splits=5, random_state=0, shuffle=True)

        scores = {
            names: {
                "Accuracy": cross_val_score(
                    models, x_train, y_train, scoring="accuracy", cv=cv
                ).mean(),
                "Precision": cross_val_score(
                    models, x_train, y_train, scoring="precision", cv=cv
                ).mean(),
                "Recall": cross_val_score(
                    models, x_train, y_train, scoring="recall", cv=cv
                ).mean(),
            }
            for (names, models) in models.items()
        }

        return scores

    @classmethod
    def predict(model, x_test):
        predictions = model.predict(x_test)
        return predictions

    @classmethod
    def stacking_model(
        estimators: List[Any],
        final_estimator,
        x_train,
        y_train,
        x_test,
        y_test,
        n_folds,
    ):

        global cv
        cv = KFold(n_splits=n_folds, random_state=0, shuffle=True)

        stack = StackingClassifier(estimators, final_estimator)
        stack = stack.fit(x_train, y_train)

        return stack


if __name__ == "__main__":
    run()
