'''
conda activate mlflow
python 01_create_experiment.py
mlflow ui                                                                                                         ✔  mlflow   17:38:53  
[2025-02-22 17:45:13 +0800] [47659] [INFO] Starting gunicorn 23.0.0
[2025-02-22 17:45:13 +0800] [47659] [INFO] Listening at: http://127.0.0.1:5000 (47659)
[2025-02-22 17:45:13 +0800] [47659] [INFO] Using worker: sync
[2025-02-22 17:45:13 +0800] [47660] [INFO] Booting worker with pid: 47660
[2025-02-22 17:45:13 +0800] [47661] [INFO] Booting worker with pid: 47661
[2025-02-22 17:45:14 +0800] [47662] [INFO] Booting worker with pid: 47662
[2025-02-22 17:45:14 +0800] [47663] [INFO] Booting worker with pid: 47663
'''

import mlflow

if __name__ == "__main__":
    # create a new mlflow experiment
    experiment_id = mlflow.create_experiment(
        name="testing_mlflow1",
        artifact_location="testing_mlflow1_artifacts",
        tags={"env": "dev", "version": "1.0.0"},
    )

    print(experiment_id)