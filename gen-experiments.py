#!/usr/bin/python3
import os
import subprocess
import shutil
import json
from git import Repo
from pathlib import Path
import pymongo

# Path to the configuration of the cluster
CLUSTER_CONFIG_PATH = './cluster-config.json'

with open(CLUSTER_CONFIG_PATH, 'r') as f:
    cluster_config = json.load(f)

# Constant definitions
EXPERIMENTS_PATH = cluster_config['experiments_path']
EXPERIMENTS_REPO_PATH = cluster_config['repo_path']
SINGULARITY_PATH = cluster_config['singularity_path']
DB_USER = cluster_config['db_user']
DB_PASS = cluster_config['db_password']
DB_ENDPOINT = cluster_config['db_endpoint']
DB_NAME = cluster_config['db_name']
DB_COLLECTION = cluster_config['db_collection']

def get_database_entry(debug=False):
    """Connects against the database and retrieves the last added entry"""
    if debug:
        if os.path.exists('./Kaysera'):
            shutil.rmtree('./Kaysera')
    
    client = pymongo.MongoClient(f"mongodb+srv://{DB_USER}:{DB_PASS}@{DB_ENDPOINT}/?retryWrites=true&w=majority")
    db = client[DB_NAME]
    coll = db[DB_COLLECTION]

    # A MongoDB is explored using cursors, and $natural is the insertion order
    # So -1 reverses the order to find the last inserted element
    cursor = coll.find().sort('$natural', -1)
    entry = next(cursor, None)
    # TODO: DELETE HERE THE ENTRY
    return entry


def process_database_entry(entry):
    # First, we create the folder structure where the experiments will be recorded
    commit_folder = os.path.join(EXPERIMENTS_PATH, entry['username'], entry['commit'])
    Path(commit_folder).mkdir(parents=True, exist_ok=True)

    # Second, we update the experiments repo (where the Singularity files will be)
    # so that we have the last available version
    repo = Repo(EXPERIMENTS_REPO_PATH)
    o = repo.remotes.origin
    o.pull()

    # Lastly we checkout the commit of the experiment and return which files have changed
    files_changed = repo.commit(entry['commit']).stats.files
    return commit_folder, files_changed


def validate_files(commit_folder, files_changed):
    
    # This is the list of expected files and subject to change
    expected_files = {'Singularity.def', 'parameters.csv', 'config.json'}
    for file_changed in files_changed:
        filename = file_changed.split('/')[-1]
        if filename in expected_files:
            # This removes the file from the list to later check if 
            # all the expected files are actually here
            expected_files.remove(filename)

            # Then we move the file from the repo to the path where
            # the experiments will be executed
            shutil.copy(os.path.join(EXPERIMENTS_REPO_PATH, file_changed), commit_folder)
    
    if expected_files:
        raise ValueError('Not all files needed are here')


def generate_experiment_command(entry, commit_folder):
    # We generate the path of the parameters and configuration of the 
    # project to be run
    parameters_path = os.path.join(commit_folder, 'parameters.csv')
    config_path = os.path.join(commit_folder, 'config.json')

    with open(config_path, 'r') as f:
        config = json.load(f)

    # Definition of necessary variables
    image_name = f"{commit_folder}/{entry['commit']}.sif"
    def_file = f'{commit_folder}/Singularity.def'
    build_job_name = f"build_{entry['commit']}"
    run_job_name = f"run_{entry['commit']}"
    env_vars = f"EXP_PATH={commit_folder},REPO_NAME={config['repo-name']},COMMIT={config['commit']}"

    # Singularity command to build the image
    build_image = [SINGULARITY_PATH, 'build', '--fakeroot', '--force', image_name, def_file]
    # qsub command to send the build command to the job queue
    build_image = ['qsub', '-N', build_job_name, '-v', env_vars, '-l', 'select=1:ncpus=1:mem=4gb', '--'] + build_image
    
    # Singularity command to run the image
    run_image = [SINGULARITY_PATH, 'run', '--env', env_vars, image_name]
    with open(parameters_path, 'r') as f:
        n_jobs = len(f.readlines()) - 1
    # qsub command to send the run command to the job queue as an array job
    # as many times as lines there are in the parameters.csv file
    # NOTE: qsub command not added because the job_id from the qsub build command is NEEDED
    run_image = ['-N', run_job_name, '-J', '1-'+str(n_jobs), '--'] + run_image
    return build_image, run_image


if __name__ == '__main__':
    # First, get the last entry in the database
    entry = get_database_entry(debug=True)
    # Second, process that entry
    commit_folder, files_changed = process_database_entry(entry)
    # Third, validate the files are right
    # TODO: Deeper validation of the Singularity.def file
    validate_files(commit_folder, files_changed)
    # Fourth, get the commands to send the experiments to the build queue
    build_image, run_image = generate_experiment_command(entry, commit_folder)
    os.chdir(commit_folder)  # We change the directory to the experiments path
    # Fifth, run the build command and get the job_id. subprocess.check_output returns a 
    # byte-array so it must be first decoded
    build_job_id = subprocess.check_output(build_image)
    build_job_id = build_job_id.decode('utf-8')[:-1]
    # Sixth, build the qsub command to wait for the build to finish and run it
    run_image = ['qsub', '-W', f'depend=afterok:{build_job_id}'] + run_image
    subprocess.call(run_image)
