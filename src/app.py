import os

import boto3
import chalice
from botocore.exceptions import ClientError

app = Chalice(app_name='hockey-predictions')

@app.route('/')
def index():
    return {'hello': 'world'}

@app.route('/import-data')
def import_data(filename: str, bucket, object_name=None):
    """ LOAD DATA """
    path = os.path.join(DATA_PATH, filename)
    if not '.csv' in filename:
        logger.info(
            f'Invalid file type. Must be a CSV'
        )
    else:

    def read_data(filename)
    classification_df = data # model-ready data
    teams_df = pd.read_csv("../data/categorical_teams_set.csv") # output
    playoffs = pd.read_csv("../data/playoffs.csv")


## Pass in the models we wish to stack
@
def import_models(models: List[Any], names: List[str]) -> List[Any]:
    models = model_development.ensemble(
        names, models
    )
    models_list = list(models.items())

    # Training and Implementing the Stacking Model
    # Train and implement stacking model
    logger.info(
        f'Models for ensemble imported. Building model stacking classifier.'
    )
    def stacked_model()
         = model_development.stacking_model(
            models_list,
            (make_pipeline(StandardScaler(), LinearSVC(random_state=42))),
            x_train,
            y_train,
            x_test,
            y_test,
            n_folds = 10
    )

def evaluation()
    summary['stacked_linear_svm'] = [
                cross_val_score(stacked_model, x_train, y_train, scoring='accuracy', cv=cv).mean(),
                cross_val_score(stacked_model, x_train, y_train, scoring='precision', cv=cv).mean(),
                cross_val_score(stacked_model, x_train, y_train, scoring='recall', cv=cv).mean()
    ]
    dfi.export(summary, '../images/model_performance.png')
    summary
