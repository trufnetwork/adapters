import os
import pandas as pd
from github import Github
from prefect import task
import io

@task
def read_repo_csv_file(repo: str, path: str, branch: str = "main") -> pd.DataFrame:
    g = Github(os.getenv("GITHUB_TOKEN")) 
    repository_api = g.get_repo(repo)
    file_content = repository_api.get_contents(path, ref=branch)
    df = pd.read_csv(io.StringIO(file_content.decoded_content.decode()))
    return df