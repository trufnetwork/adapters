import os
import pandas as pd
from github import Github, GithubException
from prefect import task
import io

@task
def task_read_repo_csv_file(repo: str, path: str, branch: str = "main") -> pd.DataFrame:
    return read_repo_csv_file(repo, path, branch)

def read_repo_csv_file(repo: str, path: str, branch: str = "main") -> pd.DataFrame:
    gh_token = os.getenv("GITHUB_TOKEN")

    g = Github(gh_token) 
    try:
        repository_api = g.get_repo(repo)
    except GithubException as e:
        if e.status == 404:
            if not gh_token:
                raise Exception(
                    "Repository not found (404). It might be private or a GitHub token is missing."
                )
            else:
                raise Exception(
                    "Repository not found (404). Please ensure your GitHub token has read access to the repository."
                )
        else:
            raise e
    file_content = repository_api.get_contents(path, ref=branch)
    df = pd.read_csv(io.StringIO(file_content.decoded_content.decode()))
    return df


